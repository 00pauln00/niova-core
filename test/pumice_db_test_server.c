/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdio.h>
#include <unistd.h>

#include "registry.h"
#include "raft_net.h"
#include "raft_test.h"
#include "pumice_db.h"

#define OPTS "u:r:h"

REGISTRY_ENTRY_FILE_GENERATE;

static const char *pmdbts_column_family_name = "PMDBTS_CF";
static rocksdb_column_family_handle_t *pmdbts_cfh;

const char *raft_uuid_str;
const char *my_uuid_str;

static rocksdb_column_family_handle_t *
pmdbst_get_cfh(void)
{
    NIOVA_ASSERT(pmdbts_cfh);

    return pmdbts_cfh;
}

static int
pmdbst_init_rocksdb(void)
{
    if (pmdbts_cfh)
        return 0;

    rocksdb_options_t *opts = rocksdb_options_create();
    if (!opts)
        return -ENOMEM;

    char *err = NULL;
    rocksdb_options_set_create_if_missing(opts, 1);

    pmdbts_cfh = rocksdb_create_column_family(PmdbGetRocksDB(), opts,
                                              pmdbts_column_family_name, &err);

    rocksdb_options_destroy(opts);

    if (!err)
    {
        pmdbts_cfh = NULL;
        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_create_column_family(): %s",
                       err);
    }

    return err ? -EINVAL : 0;;
}

static int
pmdbts_lookup(const uuid_t app_uuid, struct raft_test_values *rtv)
{
    if (!rtv || uuid_is_null(app_uuid))
        return -EINVAL;

    DECLARE_AND_INIT_UUID_STR(key, app_uuid);
    char *err = NULL;

    rocksdb_readoptions_t *ropts = rocksdb_readoptions_create();
    if (!ropts)
        return -ENOMEM;

    size_t value_len = 0;

    char *value =
        rocksdb_get_cf(PmdbGetRocksDB(), ropts, pmdb_get_rocksdb_readopts(),
                       pmdbst_get_cfh(), key, UUID_STR_LEN - 1, &value_len,
                       &err);

    rocksdb_readoptions_destroy(ropts);

    int rc = 0;

    if (!value) // Xxx need better error interpretation
    {
        rc = -ENOENT;
    }
    else if (err)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_get_cf(`%s`): %s", key, err);
        rc = -EINVAL;
    }
    else if (value_len != sizeof(struct raft_test_values))
    {
        rc = -EBADMSG;
    }
    else
    {
        *rtv = *(struct raft_test_values *)value;
    }

    if (value)
        free(value);

    return rc;
}

static int
pmdbts_apply_lookup_and_check(const uuid_t app_uuid, const char *input_buf,
                              size_t input_bufsz,
                              struct raft_test_values *ret_rtv)
{
    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)input_buf;

    if (!rdtb || (ssize_t)input_bufsz != raft_test_data_block_total_size(rtdb))
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "null input buf or invalid size (%zu) "
                       "raft_test_data_block_total_size(): %zd",
                       input_bufsz, raft_test_data_block_total_size(rtdb));

        return -EINVAL;
    }
    else if (uuid_compare(app_uuid, rtdb->rtdb_client_uuid))
    {
        DECLARE_AND_INIT_UUID_STR(app_uuid_str, app_uuid);

        DBG_RAFT_TEST_DATA_BLOCK(LL_ERROR, rtdb, "mismatched UUID=%s",
                                 app_uuid_str);
        return -EBADMSG;
    }
    else if (rdtb->rtdb_num_values == 0)
    {
        DBG_RAFT_TEST_DATA_BLOCK(LL_ERROR, rtdb, "num_values == 0");
        return -ENOMSG;
    }

    // Lookup the app uuid in the DB.
    struct raft_test_values current_rtv;
    int rc = pmdbts_lookup(app_uuid, &current_rtv);

    if (rc == -ENOENT)
    {
        memset(&current_rtv, 0, sizeof(struct raft_test_values));
        DBG_RAFT_TEST_DATA_BLOCK(LL_NOTIFY, rtdb, "entry does not yet exist");
    }
    else if (rc)
    {
        DBG_RAFT_TEST_DATA_BLOCK(LL_ERROR, rtdb, "pmdbts_lookup(): %s",
                                 strerror(-rc));
        return rc;
    }

    DBG_RAFT_TEST_DATA_BLOCK(LL_DEBUG, rtdb,
                             "pmdbts_lookup(): current seqno=%ld, val=%ld",
                             current_rtv.rtv_seqno,
                             current_rtv.rtv_reply_xor_all_values)

    // Check the sequence is correct and the contents are valid.
    for (uint16_t i = 0; i < rtdb->rtdb_num_values; i++)
    {
        const struct raft_test_values *prov_rtv = &rtbd->rtdb_values[i];

        if ((current_rtv.rtv_seqno + i + 1) != prov_rtv->rtv_seqno)
        {
            DBG_RAFT_TEST_DATA_BLOCK(
                LL_ERROR, rtdb, "invalid sequence @idx-%hu(%ld) (current=%ld)",
                i, prov_rtv->rtv_seqno, current_rtv.rtv_seqno);

            return -EILSEQ;
        }
    }

    // Success
    *ret_rtv = current_rtv;

    return 0;
}

static void
pmdbts_sum_incoming_rtv(const struct raft_test_data_block *rtdb_src,
                        struct raft_test_values *dest_rtv)
{
    for (uint16_t i = 0; i < rtdb_src->rtdb_num_values; i++)
    {
        const struct raft_test_values *src_rtv = &rtbd_src->rtdb_values[i];

        // These sequences should have already been checked!
        NIOVA_ASSERT((dest_rtv->rtv_seqno + 1) == src_rtv->rtv_seqno);

        dest_rtv->rtv_reply_xor_all_values ^= src_rtv->rtv_request_value;
        dest_rtv->rtv_seqno++;
    }
}

static pumicedb_apply_ctx_t
pmdbts_apply(const uuid_t app_uuid, const char *input_buf, size_t input_bufsz,
             void *pmdb_handle)
{
    NIOVA_ASSERT(!pmdbst_init_rocksdb());

    const struct raft_test_data_block *rtdb =
        (const struct raft_test_data_block *)input_buf;

    struct raft_test_values stored_rtv;
    int rc = pmdbts_apply_lookup_and_check(app_uuid, input_buf, input_bufsz,
                                           &stored_rtv);
    if (rc)
        return;

    pmdbts_sum_incoming_rtv(rtdb, &stored_rtv);

    DBG_RAFT_TEST_DATA_BLOCK(LL_DEBUG, rtdb, "new seqno=%ld, val=%ld",
                             stored_rtv.rtv_seqno,
                             stored_rtv.rtv_reply_xor_all_values);

    DECLARE_AND_INIT_UUID_STR(app_uuid_str, app_uuid);

    // Stage the KV back through pumiceDB.
    PmdbWriteKV(app_uuid, pmdb_handle, app_uuid_str, (const char *)&stored_rtv,
                sizeof(struct raft_test_values), NULL,
                (void *)pmdbst_get_cfh());

}

static pumicedb_read_ctx_ssize_t
pmdbts_read(const uuid_t app_uuid, const char *request_buf,
            size_t request_bufsz, char *reply_buf, size_t reply_bufsz)
{
    if (!request_buf || !request_bufsz || !reply_buf || !reply_bufsz)
        return (ssize_t)-EINVAL;

    const struct raft_test_data_block *req_rtdb =
        (const struct raft_test_data_block *)request_buf;

    const ssize_t rrc = raft_test_data_block_total_size(req_rtdb);
    if (rrc != (ssize_t)request_bufsz)
    {
        DBG_RAFT_TEST_DATA_BLOCK(
            LL_NOTIFY, req_rtdb,
            "raft_test_data_block_total_size()=%zd != request_bufsz=%zu",
            rrc, request_bufsz);

        return rrc < 0 ? rrc : -EBADMSG;
    }
    else if (reply_bufsz !=
             (sizeof(struct raft_test_data_block) +
              sizeof(struct raft_test_values)))
    {
        DBG_RAFT_TEST_DATA_BLOCK(LL_NOTIFY, req_rtdb,
                                 "reply_bufsz=%zd is too small", reply_bufsz);
        return -ENOSPC;
    }

    struct raft_test_data_block *reply_rtdb =
        (struct raft_test_data_block *)reply_buf;

    uuid_copy(reply_rtdb->rtdb_client_uuid, req_rtdb->rtdb_client_uuid);
    reply_rtdb->rtdb_op = RAFT_TEST_DATA_OP_READ;

    int rc = pmdbts_lookup(reply_rtdb->rtdb_client_uuid,
                           &reply_rtdb->rtdb_values[0]);

    if (rc == -ENOENT)
        reply_rtdb->rtdb_num_values = 0;

    else if (!rc)
        reply_rtdb->rtdb_num_values = 1;

    else
        return (ssize_t)rc;

    return raft_test_data_block_total_size(reply_rtdb);
}

static void
pmdbts_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -r <UUID> -u <UUID>\n", argv[0]);

    exit(error);
}

static void
pmdbts_getopt(int argc, char **argv)
{
    if (!argc || !argv)
        return;

    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'r':
            raft_uuid_str = optarg;
            break;
	case 'u':
            my_uuid_str = optarg;
	    break;
	case 'h':
            pmdbts_print_help(0, argv);
	    break;
        default:
            pmdbts_print_help(EINVAL, argv);
            break;
	}
    }

    if (!raft_uuid_str || !my_uuid_str)
	pmdbts_print_help(EINVAL, argv);
}

int
main(int argc, char **argv)
{
    pmdbts_getopt(argc, argv);

    struct PmdbAPI api = {
        .pmdb_apply = pmdbts_apply,
        .pmdb_read = pmdbts_read,
    };

    return PmdbExec(raft_uuid_str, my_uuid_str, &api);
}

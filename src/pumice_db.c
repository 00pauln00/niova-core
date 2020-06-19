/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdlib.h>
#include <uuid/uuid.h>

#include <rocksdb/c.h>

#include "registry.h"
#include "log.h"
#include "raft.h"
#include "raft_net.h"
#include "pumice_db.h"
#include "ref_tree_proto.h"
#include "alloc.h"
#include "pumice_db.h"

#define PMDB_COLUMN_FAMILY_NAME "pumiceDB_private"

static const struct PmdbAPI *pmdbApi;
static rocksdb_column_family_handle_t *pmdbRocksdbCFH;
static rocksdb_readoptions_t *pmdbRocksdbReadOpts;

struct pmdb_rpc_msg
{
    uuid_t   pmrbrm_uuid; // must match rcrm_sender_id
    int64_t  pmdbrm_write_seqno; // request::next, reply::committed
    uint8_t  pmdbrm_op;
    uint8_t  pmdbrm_write_pending;  // reply context only
    uint8_t  pmdbrm__pad[2];
    uint32_t pmdbrm_data_size; // size of application payload
    char     pmdbrm_data[];
};

struct pmdb_value_obj_v0
{
    uint64_t pmdb_oextra_commit_term;
    uuid_t   pmdb_oextra_commit_uuid; // UUID of leader at the time
};

/**
 * pmdb_object - object which is stored in the value contents of a RocksDB KV
 */
struct pmdb_object
{
    uint32_t pmdb_obj_crc;
    uint32_t pmdb_obj_version;
    int64_t  pmdb_obj_commit_seqno;
    int64_t  pmdb_obj_pending_term;
    union
    {
        struct pmdb_obj_extras_v0 v0;
    };
};


#define PMDB_OBJ_DEBUG(log_level, pmdb_obj_str, pmdb_obj, fmt, ...)     \
{                                                                       \
    LOG_MSG(log_level, "%s: v=%d cmt-seqno=%ld pndg-term=%ld",          \
            (pmdb_obj_str), (pmdb_obj) ? (pmdb_obj)->pmdb_obj_version : -1U, \
            (pmdb_obj) ? (pmdb_obj)->pmdb_obj_commit_seqno : -1ULL,     \
            (pmdb_obj) ? (pmdb_obj)->pmdb_obj_pending_term : -1ULL);    \
}

static void
pmdb_object_init(struct pmdb_object *pmdb_obj, uint32_t version,
                 int64_t current_raft_term)
{
    NIOVA_ASSERT(pmdb_obj);

    memset(0, sizeof(*pmdb_obj));

    pmdb_obj->pmdb_obj_version = version;
    pmdb_obj->pmdb_obj_commit_seqno = ID_ANY_64bit;
    pmdb_obj->pmdb_obj_pending_term = current_raft_term;
}

static rocksdb_t *
pmdb_get_rocksdb_instance(void)
{
    rocksdb_t *db = raft_server_get_rocksdb_instance(raft_net_get_instance());
    NIOVA_ASSERT(db);

    return db;
}

static rocksdb_readoptions_t *
pmdb_get_rocksdb_readopts(void)
{
    NIOVA_ASSERT(pmdbRocksdbReadOpts)
    return pmdbRocksdbReadOpts;
}

static int
pmdb_init_rocksdb(void)
{
    if (pmdbRocksdbCFH)
        return 0;

    pmdbRocksdbReadOpts = rocksdb_readoptions_create();
    if (!pmdbRocksdbReadOpts)
        return -ENOMEM;

    rocksdb_options_t *opts = rocksdb_options_create();
    if (!wopts)
    {
        rocksdb_readoptions_destroy(pmdbRocksdbReadOpts);
        pmdbRocksdbReadOpts = NULL;
        return -ENOMEM;
    }

    char *err = NULL;
    rocksdb_options_set_create_if_missing(opts, 1);

    pmdbRocksdbCFH =
        rocksdb_create_column_family(pmdb_get_rocksdb_instance(), opts,
                                     PMDB_COLUMN_FAMILY_NAME, &err);

    rocksdb_options_destroy(opts);

    if (err)
    {
        rocksdb_readoptions_destroy(pmdbRocksdbReadOpts);
        pmdbRocksdbReadOpts = NULL;

        pmdbRocksdbCFH = NULL;

        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_create_column_family(): %s",
                       err);
    }

    return 0;
}

static rocksdb_column_family_handle_t *
pmdb_get_rocksdb_column_family_handle(void)
{
    if (!pmdbRocksdbCFH)
    {
        int rc = pmdb_init_rocksdb();
        if (rc)
            return NULL;
    }

    return pmdbRocksdbCFH;
}

static void
pmdb_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct pmdb_rpc_msg) <=
                        PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP);

    // enum PmdbOpType must fit into 'uint8_t pmdbrm_op'
    COMPILE_TIME_ASSERT(pmdb_op_any < (1 << sizeof(uint8_t)));
}

#define PMDB_ARG_CHECK(op, rncr)                                        \
    NIOVA_ASSERT(                                                       \
        (rncr) && (rncr)->rncr_type == op &&                            \
        (rncr)->rncr_request && (rncr)->rncr_reply &&                   \
        (rncr)->rncr_reply_data_max_size >= sizeof(struct pmdb_rpc_msg))

#define PMDB_CFH_MUST_GET()
({                                                      \
    rocksdb_column_family_handle_t *cfh =               \
        pmdb_get_rocksdb_column_family_handle();        \
                                                        \
    NIOVA_ASSERT(cfh);                                  \
    cfh;                                                \
})

#define PMDB_ENTRY_KEY_PREFIX "P0."
#define PMDB_ENTRY_KEY_PREFIX_LEN 3
#define PMDB_ENTRY_KEY_STRBUF_LEN (PMDB_ENTRY_KEY_PREFIX_LEN + UUID_STR_LEN)
#define PMDB_ENTRY_KEY_LEN (PMDB_ENTRY_KEY_STRBUF_LEN - 1) //excludes null byte

typedef char pmdb_obj_str_t[PMDB_ENTRY_KEY_PREFIX_LEN];

static void
pmdb_object_fmt_key(const uuid_t obj_uuid, pmdb_obj_str_t obj_str)
{
    NIOVA_ASSERT(obj_str)

    strncpy(&obj_str[0], PMDB_ENTRY_KEY_PREFIX, PMDB_ENTRY_KEY_PREFIX_LEN);
    uuid_unparse(obj_uuid, &key_str[PMDB_ENTRY_KEY_PREFIX_LEN]);
}

static int
pmdb_object_lookup(const uuid_t obj_uuid, struct pmdb_object *obj,
                   const int64_t current_raft_term)
{
    NIOVA_ASSERT(obj && !uuid_is_null(obj_uuid));

    rocksdb_column_family_handle_t *pmdb_cfh = PMDB_CFH_MUST_GET();
    pmdb_obj_str_t pmdb_obj_str;

    pmdb_object_fmt_key(obj_uuid, pmdb_obj_str);

    size_t val_len = 0;
    char *err = NULL;
    int rc = -ENOENT;

    char *get_value =
        rocksdb_get(pmdb_get_rocksdb_instance(), pmdb_get_rocksdb_readopts(),
                    &pmdb_obj_str, PMDB_ENTRY_KEY_LEN, &val_len, &err);

    PMDB_OBJ_DEBUG(LL_NOTIFY, pmdb_obj_str, NULL, "err=%s val=%p",
                   err, get_value);

    if (err || !get_value)
        return rc; //Xxx need a proper error code intepreter

    if (val_len != sizeof(struct pmdb_object))
    {
        PMDB_OBJ_DEBUG(LL_WARN, pmdb_obj_str, NULL,
                       "invalid len (%zu), expected %zu", val_len,
                       sizeof(struct pmdb_object));

        rc = -EUCLEAN;
    }
    else
    {
        memcpy((void *)obj, get_value, sizeof(struct pmdb_object));
        rc = 0;
    }

    // Release buffer allocated by rocksdb C interface
    free(get_value);

    if (obj->pmdb_obj_pending_term >= current_raft_term)
        rc = -ELNRANGE;

    PMDB_OBJ_DEBUG((rc ? LL_WARN : LL_DEBUG), pmdb_obj_str, obj, "")

    return rc;
}

static void
pmdb_obj_to_reply(const struct pmdb_object *obj, struct pmdb_rpc_msg *reply,
                  const int64_t current_raft_term)
{
    NIOVA_ASSERT(obj && reply);

    reply->pmdbrm_write_seqno = obj->pmdb_obj_commit_seqno;

    reply->pmdbrm_write_pending =
        obj->pmdb_obj_pending_term == current_raft_term ? 1 : 0;
}

/**
 * pmdb_sm_handler_client_lookup - perform a key lookup in the PMDB column-
 *    family.
 * RETURN: 0 is always returned so that a reply will be delivered to the client.
 */
static int
pmdb_sm_handler_client_lookup(struct pmdb_rpc_msg *pmdb_reply,
                              const int64_t current_raft_term)
{
    NIOVA_ASSERT(pmdb_reply && !uuid_is_null(pmdb_reply->pmrbrm_uuid));

    struct pmdb_object pmdb_obj = {0};

    int rc = pmdb_object_lookup(pmdb_reply->pmrbrm_uuid, &pmdb_obj, current_raft_term);

    if (!rc)
        pmdb_obj_to_reply(&pmdb_obj, pmdb_reply, current_raft_term);
    else
        raft_client_msg_error_set(raft_net_data_to_rpc_msg(pmdb_reply), rc, 0);

    return 0;
}

static uint32_t
pmdb_get_current_version(void)
{
    return 0;
}

static void
pmdb_prep_raft_entry_write(struct raft_net_client_request *rncr,
                           pmdb_obj_str_t pmdb_obj_str, const struct pmdb_object *obj)
{
    NIOVA_ASSERT(rncr && obj);

    rncr->rncr_write_raft_entry = true;

}

/**
 * pmdb_sm_handler_client_write - lookup the object and ensure that the requested
 *     write sequence number is consistent with the pmdb-object.
 *
 * RETURN:  Returning without an error and with rncr_write_raft_entry=false
 *   will cause an immediate reply to the client.  Returning any non-zero value
 *   causes the request to terminate immediately without any reply being issued.
 */
static int
pmdb_sm_handler_client_write(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    struct pmdb_rpc_msg *pmdb_reply = reply->rcrm_data;

    struct pmdb_object obj = {0};

    pmdb_object_fmt_key(pmdb_reply->pmrbrm_uuid, pmdb_obj_str);

    int rc = pmdb_object_lookup(pmdb_reply->pmrbrm_uuid, &obj,
                                rncr->rncr_current_term);
    if (rc)
    {
        if (rc == -ENOENT)
        {
            pmdb_object_init(&obj, pmdb_get_current_version(),
                             rncr->rncr_current_term);

            rc = 0;
        }
        else
        {
            PMDB_OBJ_DEBUG(LL_NOTIFY, pmdb_obj_str, NULL, "pmdb_object_lookup(): %s",
                           strerror(-rc));

            // This appears to be a system error.  Mark it and reply to the client.
            raft_client_net_request_error_set(rncr, rc, rc, 0);

            return 0;
        }
    }

    // Check if the request was already completed
    if (pmdb_req->pmdbrm_write_seqno <= obj.pmdb_obj_commit_seqno)
    {
        raft_client_net_request_error_set(rncr, -EALREADY, 0, 0);
    }

    else if (pmdb_req->pmdbrm_write_seqno == (obj.pmdb_obj_commit_seqno + 1))
    {
        // Check if request has already been placed into the log.
        if (obj.pmdb_obj_pending_term == rncr->rncr_current_term)
            raft_client_net_request_error_set(rncr, -EINPROGRESS, 0, -EINPROGRESS);

        else // Request sequence is OK and will enter the raft log.
            pmdb_prep_raft_entry_write(rncr, pmdb_obj_str, &obj);
    }

    else // Request sequence is too far ahead
    {
        raft_client_net_request_error_set(rncr, -EBADE, 0, -EBADE);
    }

    // XXX need to place KV items int rncr to be written with the raft log
    //     entry

    PMDB_OBJ_DEBUG(LL_NOTIFY, pmdb_obj_str, obj, "");

    return 0;
}

static int
pmdb_sm_handler_client_read(struct raft_net_client_request *rncr)
{
    PMDB_ARG_CHECK(RAFT_NET_CLIENT_REQ_TYPE_READ, rncr);


    NIOVA_ASSERT(pmdb_req->pmdbrm_data_size <= PMDB_MAX_APP_RPC_PAYLOAD_SIZE);

    const size_t max_reply_size =
        rncr->rncr_reply_data_max_size - PMDB_RESERVED_RPC_PAYLOAD_SIZE_UDP;

    const ssize rrc =
        pmdbApi->pmdb_read(pmdb_req->pmrbrm_uuid, pmdb_req->pmdbrm_data,
                           pmdb_req->pmdbrm_data_size, pmdb_reply->pmdbrm_data,
                           max_reply_size);

    enum log_level log_level = LL_DEBUG;

    if (rrc < 0)
    {
        pmdb_reply->pmdbrm_data_size = 0;
        reply->rcrm_app_error = reply->rcrm_sys_error = (int16_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, rncr->rncr_remote_addr,
                            "pmdbApi::read(): %s", strerror(rrc));
    }
    else if (rrc > (ssize_t)max_reply_size)
    {
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;
        reply->rcrm_app_error = -E2BIG;

        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, rncr->rncr_remote_addr,
                            "pmdbApi::read(): reply too large (%zd)", rrc);
    }
    else
    {
        pmdb_reply->pmdbrm_data_size = (uint32_t)rrc;

        DBG_RAFT_CLIENT_RPC(LL_DEBUG, req, rncr->rncr_remote_addr,
                            "pmdbApi::read(): reply-size=%zd", rrc);
    }

    return 0;
}

static void
pmdb_reply_init(const struct pmdb_rpc_msg *req, struct pmdb_rpc_msg *reply)
{
    NIOVA_ASSERT(req && reply);

    reply->pmdbrm_data_size = 0;

    uuid_copy(reply->pmrbrm_uuid, req->pmrbrm_uuid);
    reply->pmdbrm_op = reply->pmdbrm_op;
}

static int
pmdb_sm_handler_client_op(struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(rncr && rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE &&
                 rncr->rncr_request && rncr->rncr_reply);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    const struct pmdb_rpc_msg *pmdb_reply = req->rcrm_data;

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;
    const enum PmdbOpType op = pmdb_req->pmdbrm_op;

    pmdb_reply_init(pmdb_req, pmdb_reply);

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, req, rncr->rncr_remote_addr, "op=%u", op);

    switch (op)
    {
    case pmdb_op_noop:
        return 0; // noop should be harmless

    case pmdb_op_read:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_READ;
        return pmdb_sm_handler_client_read(rncr);

    case pmdb_op_write:
        rncr->rncr_type = RAFT_NET_CLIENT_REQ_TYPE_WRITE;
        return pmdb_sm_handler_client_write(rncr);

    case pmdb_op_lookup: // type of "read" which does not enter the app API
        return pmdb_sm_handler_client_lookup(rncr);

    default:
        break;
    }

    return -EOPNOTSUPP;
}

static int
pmdb_sm_handler_pmdb_req_check(const struct pmdb_rpc_msg *pmdb_req)
{
    if (pmdb_req->pmdbrm_data_size > PMDB_MAX_APP_RPC_PAYLOAD_SIZE)
        return -EINVAL;

    // Check the uuid for correctness
    if (uuid_is_null(pmdb_req->pmrbrm_uuid) ||
        uuid_compare(pmdb_req->pmrbrm_uuid, req->rcrm_sender_id))
        return -EBADMSG;

    return 0;
}

static int
pmdb_sm_handler(struct raft_net_client_request *rncr)
{
    if (!rncr || !rncr->rncr_request)
        return -EINVAL;

    else if (rncr->rncr_request->rcrm_data_size < sizeof(struct pmdb_rpc_msg))
        return -EBAMSG;

    rncr->rncr_write_raft_entry = false; // default is to not write the raft log

    const struct raft_client_rpc_msg *req = rncr->rncr_request;
    const struct pmdb_rpc_msg *pmdb_req = req->rcrm_data;
    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    if (rncr->rncr_type == RAFT_NET_CLIENT_REQ_TYPE_NONE)
    {
        if (rncr->rncr_reply_data_max_size < sizeof(struct pmdb_rpc_msg))
            return -ENOSPC;

        reply->rcrm_app_error = pmdb_sm_handler_pmdb_req_check(pmdb_req);
        if (reply->rcrm_app_error)
        {
            // There's a problem with the application RPC request
            DBG_RAFT_CLIENT_RPC(LL_NOTIFY, req, rncr->rncr_remote_addr,
                                "pmdb_sm_handler_pmdb_req_check(): %s",
                                strerror(reply->rcrm_app_error));
            return 0;
        }

        return pmdb_sm_handler_client_op(rncr);
    }
    // Still need the commit handler XXX
}

int
PmdbExec(const char *raft_uuid_str, const char *raft_instance_uuid_str,
         const struct PmdbAPI *pmdb_api)
{
    if (!raft_uuid_str || !raft_instance_uuid_str || !pmdb_api ||
        !pmdb_api->pmdb_apply || !pmdb_api->pmdb_read)
        return -EINVAL;

    pmdbApi = pmdb_api;

    return raft_net_server_instance_run(raft_uuid_str, raft_instance_uuid_str,
                                        pmdb_sm_handler,
                                        RAFT_INSTANCE_STORE_ROCKSDB);
}


//XXX todo
/* int */
/* PmdbClose(void) */
/* { */
//pmdb_get_rocksdb_column_family_handle()
/*     raft_net_get_instance(); */
/* } */

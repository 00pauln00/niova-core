/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <rocksdb/c.h>

#include "alloc.h"
#include "common.h"
#include "log.h"
#include "raft.h"
#include "registry.h"

#define RAFT_ROCKSDB_KEY_LEN_MAX 256UL

#define RAFT_LOG_HEADER_ROCKSDB "a0_hdr."
#define RAFT_LOG_HEADER_ROCKSDB_STRLEN 7
#define RAFT_LOG_HEADER_FMT RAFT_LOG_HEADER_ROCKSDB"%s__%s"

#define RAFT_LOG_LASTENTRY_ROCKSDB "z0_last."
#define RAFT_LOG_LASTENTRY_ROCKSDB_STRLEN 8
#define RAFT_LOG_LASTENTRY_FMT RAFT_LOG_LASTENTRY_ROCKSDB"%s__%s"

#define RAFT_HEADER_ENTRY_KEY_FMT "%016zuh"
#define RAFT_ENTRY_KEY_FMT        "%016zue"

#define RAFT_ENTRY_KEY_PREFIX_ROCKSDB "e0."
#define RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN 3
#define RAFT_ENTRY_KEY_PRINTF RAFT_ENTRY_KEY_PREFIX_ROCKSDB RAFT_ENTRY_KEY_FMT

#define RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB RAFT_ENTRY_KEY_PREFIX_ROCKSDB
#define RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB_STRLEN \
    RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN
#define RAFT_ENTRY_HEADER_KEY_PRINTF                            \
    RAFT_ENTRY_KEY_PREFIX_ROCKSDB RAFT_HEADER_ENTRY_KEY_FMT


REGISTRY_ENTRY_FILE_GENERATE;

struct raft_instance_rocks_db
{
    rocksdb_t              *rir_db;
    rocksdb_options_t      *rir_options;
    rocksdb_writeoptions_t *rir_writeoptions;
    rocksdb_readoptions_t  *rir_readoptions;
    rocksdb_writebatch_t   *rir_writebatch;
};

static void
rsbr_entry_write(struct raft_instance *, const struct raft_entry *);

static ssize_t
rsbr_entry_read(struct raft_instance *, struct raft_entry *);

static int
rsbr_entry_header_read(struct raft_instance *, struct raft_entry_header *);

static void
rsbr_log_truncate(struct raft_instance *, const raft_entry_idx_t);

static int
rsbr_header_load(struct raft_instance *);

static int
rsbr_header_write(struct raft_instance *);

static int
rsbr_setup(struct raft_instance *);

static int
rsbr_destroy(struct raft_instance *);

static struct raft_instance_backend ribRocksDB = {
    .rib_entry_write       = rsbr_entry_write,
    .rib_entry_read        = rsbr_entry_read,
    .rib_entry_header_read = rsbr_entry_header_read,
    .rib_log_truncate      = rsbr_log_truncate,
    .rib_header_write      = rsbr_header_write,
    .rib_header_load       = rsbr_header_load,
    .rib_backend_setup     = rsbr_setup,
    .rib_backend_shutdown  = rsbr_destroy,
};

static inline struct raft_instance_rocks_db *
rsbr_ri_to_rirdb(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_backend == &ribRocksDB && ri->ri_backend_arg);

    struct raft_instance_rocks_db *rir =
        (struct raft_instance_rocks_db *)ri->ri_backend_arg;

    NIOVA_ASSERT(rir->rir_writeoptions && rir->rir_writebatch &&
                 rir->rir_readoptions);

    return rir;
}

static rocksdb_iterator_t *
rsbr_create_iterator(struct raft_instance_rocks_db *rir)
{
    if (!rir || !rir->rir_db || !rir->rir_readoptions)
        return NULL;

    rocksdb_iterator_t *iter =
        rocksdb_create_iterator(rir->rir_db, rir->rir_readoptions);

    if (!iter)
        return NULL;

    if (rocksdb_iter_valid(iter)) // The iterator should *not* yet be valid
    {
        rocksdb_iter_destroy(iter);
        return NULL;
    }

    return iter;
}

static int
rsbr_iter_check_error(rocksdb_iterator_t *iter, bool expect_valid)
{
    char *err = NULL;

    rocksdb_iter_get_error(iter, &err);
    if (err)
        return -EIO;

    if (!!rocksdb_iter_valid(iter) == !!expect_valid)
        return 0;

    return expect_valid ? -ENOENT : -EEXIST;
}

static int
rsbr_iter_seek(rocksdb_iterator_t *iter, const char *seek_str,
               size_t seek_strlen, bool expect_valid)
{
    if (!iter || !seek_str || !seek_strlen)
        return -EINVAL;

    rocksdb_iter_seek(iter, seek_str, seek_strlen);

    return rsbr_iter_check_error(iter, expect_valid);
}

static int
rsbr_iter_next_or_prev(rocksdb_iterator_t *iter, bool expect_valid,
                       bool next_or_prev)
{
    if (!iter)
	return -EINVAL;

    next_or_prev ? rocksdb_iter_next(iter) : rocksdb_iter_prev(iter);

    return rsbr_iter_check_error(iter, expect_valid);
}

static bool
rsbr_string_matches_iter_key(const char *str, size_t str_len,
                                    rocksdb_iterator_t *iter, bool exact_len)
{
    size_t iter_key_len = 0;
    const char *iter_key = rocksdb_iter_key(iter, &iter_key_len);

    if (!iter_key)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_iter_key(): returns NULL");
        return false;
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "match key='%s', found key='%.*s'",
                   str, (int)iter_key_len, iter_key);

    if ((exact_len && str_len != iter_key_len) ||
        strncmp(str, iter_key, str_len))
    {
        SIMPLE_LOG_MSG(LL_ERROR, "expected key='%s', got key='%.*s'",
                       str, (int)iter_key_len, iter_key);

        return false;
    }

    return true;
}

static void
rsbr_entry_write(struct raft_instance *ri, const struct raft_entry *re)
{
    NIOVA_ASSERT(ri && re && re->re_header.reh_index >= 0);

    size_t entry_size = re->re_header.reh_data_size;
    raft_entry_idx_t entry_idx = re->re_header.reh_index;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    /* There are 2 items to write here:
     * 1) raft entry header KV
     * 2) raft entry KV
     */
    size_t entry_header_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_header_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_header_key_len,
                                RAFT_ENTRY_HEADER_KEY_PRINTF, entry_idx);

    rocksdb_writebatch_put(rir->rir_writebatch, entry_header_key,
                           entry_header_key_len, (const char *)&re->re_header,
                           sizeof(struct raft_entry_header));

    size_t entry_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_key_len,
                                RAFT_ENTRY_KEY_PRINTF, entry_idx);

    // Store an entry for every header, even if the entry is empty.
    const char x = '\0';
    const char *entry_val = entry_size ? re->re_data : &x;
    if (!entry_size)
        entry_size = 1;

    rocksdb_writebatch_put(rir->rir_writebatch, entry_key, entry_key_len,
                           entry_val, entry_size);

    char *err = NULL;
    rocksdb_write(rir->rir_db, rir->rir_writeoptions, rir->rir_writebatch,
                  &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write():  %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);
}

static int
rsbr_get(struct raft_instance_rocks_db *rir, const char *key, size_t key_len,
         void *value, size_t max_value_len, size_t *ret_value_len)
{
    if (!rir || !key || !key_len || !value || !max_value_len)
        return -EINVAL;

    char *err = NULL;
    size_t val_len = 0;

    if (ret_value_len)
        *ret_value_len = 0;

    char *get_value =
        rocksdb_get(rir->rir_db, rir->rir_readoptions, key, key_len, &val_len,
                    &err);

    if (err || !get_value)
        return -ENOENT;  //Xxx need a proper error code intepreter

    memcpy((char *)value, get_value, MIN(val_len, max_value_len));

    free(get_value);

    if (ret_value_len)
	*ret_value_len = val_len;

    return 0;
}

static int
rsbr_get_exact_val_size(struct raft_instance_rocks_db *rir,
                        const char *key, size_t key_len,
                        void *value, size_t expected_value_len)
{
    if (!rir || !key || !key_len || !value || !expected_value_len)
        return -EINVAL;

    size_t ret_value_len = 0;
    int rc = rsbr_get(rir, key, key_len, value,
                                     expected_value_len, &ret_value_len);
    if (rc)
    {
        return rc;
    }
    else if (ret_value_len != expected_value_len)
    {
        LOG_MSG(
            LL_NOTIFY,
            "rsbr_get('%.*s') expected-sz(%zu), ret-sz(%zu)",
            (int)key_len, key, expected_value_len, ret_value_len);

        return ret_value_len > expected_value_len ? -ENOSPC : -EMSGSIZE;
    }

    return 0;
}

static int
rsbr_entry_header_read(struct raft_instance *ri, struct raft_entry_header *reh)
{
  if (!ri || !reh || reh->reh_index < 0)
      return -EINVAL;

  struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

  size_t entry_header_key_len = 0;
  DECL_AND_FMT_STRING_RET_LEN(entry_header_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                              (ssize_t *)&entry_header_key_len,
                              RAFT_ENTRY_HEADER_KEY_PRINTF, reh->reh_index);

  int rc = rsbr_get_exact_val_size(rir, entry_header_key, entry_header_key_len,
                                   (void *)reh,
                                   sizeof(struct raft_entry_header));
  if (rc)
  {
      LOG_MSG(LL_ERROR, "rsbr_get_exact_val_size('%s'): %s",
              entry_header_key, strerror(rc));
  }

  return rc;
}

static ssize_t
rsbr_entry_read(struct raft_instance *ri, struct raft_entry *re)
{
    if (!ri || !re)
	return -EINVAL;

    int rc = rsbr_entry_header_read(ri, &re->re_header);
    if (rc)
        return rc;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    size_t entry_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_key_len,
                                RAFT_ENTRY_KEY_PRINTF,
                                re->re_header.reh_index);

    rc = rsbr_get_exact_val_size(rir, entry_key, entry_key_len,
                                 (void *)re->re_data,
                                 re->re_header.reh_data_size);
    if (rc)
    {
        LOG_MSG(LL_ERROR, "rsbr_get_exact_val_size('%s'): %s",
                entry_key, strerror(rc));
    }

    return rc < 0 ? rc :
        re->re_header.reh_data_size + sizeof(struct raft_entry_header);
//Xxx this is wonky
}

static int
rsbr_header_load(struct raft_instance *ri)
{
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    size_t header_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(header_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&header_key_len,
                                RAFT_LOG_HEADER_FMT,
                                ri->ri_raft_uuid_str,
                                ri->ri_this_peer_uuid_str);

    int rc = rsbr_get_exact_val_size(rir, header_key, header_key_len,
                                     (void *)&ri->ri_log_hdr,
                                     sizeof(struct raft_log_header));
    if (!rc)
    {
        if (ri->ri_log_hdr.rlh_magic != RAFT_HEADER_MAGIC)
        {
            rc = -EBADMSG;
        }
        else
        {
            DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");
        }
    }
    return rc;
}

static int
rsbr_header_write(struct raft_instance *ri)
{
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    NIOVA_ASSERT(rir->rir_writeoptions && rir->rir_writebatch);
    rocksdb_writebatch_clear(rir->rir_writebatch);

    size_t key_len;
    DECL_AND_FMT_STRING_RET_LEN(header_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&key_len, RAFT_LOG_HEADER_FMT,
                                ri->ri_raft_uuid_str,
                                ri->ri_this_peer_uuid_str);

    rocksdb_writebatch_put(rir->rir_writebatch, header_key, key_len,
                           (const char *)&ri->ri_log_hdr,
                           sizeof(struct raft_log_header));

    DECL_AND_FMT_STRING_RET_LEN(last_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&key_len, RAFT_LOG_LASTENTRY_FMT,
                                ri->ri_raft_uuid_str,
                                ri->ri_this_peer_uuid_str);

    rocksdb_writebatch_put(rir->rir_writebatch, last_key, key_len,
                           (const char *)&ri->ri_log_hdr,
                           sizeof(struct raft_log_header));

    char *err = NULL;
    rocksdb_write(rir->rir_db, rir->rir_writeoptions, rir->rir_writebatch,
                  &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write():  %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    return 0;
}

static int // Call from rib_backend_setup()
rsbr_init_header(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    memset(&ri->ri_log_hdr, 0, sizeof(struct raft_log_header));

    return rsbr_header_write(ri);
}

static ssize_t
rsbr_num_entries_calc(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    rocksdb_iterator_t *iter = rsbr_create_iterator(rir);
    if (!iter)
        return -ENOMEM;

    ssize_t rrc =
        rsbr_iter_seek(iter, RAFT_LOG_LASTENTRY_ROCKSDB,
                                      RAFT_LOG_LASTENTRY_ROCKSDB_STRLEN, true);
    if (rrc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri,
                          "rsbr_iter_seek(%s): %s",
                          RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB,
                          strerror(-rrc));
        rocksdb_iter_destroy(iter);
        return rrc;
    }

    size_t iter_key_len = 0;

    SIMPLE_LOG_MSG(LL_WARN, "last-key='%.*s'",
                   (int)iter_key_len, rocksdb_iter_key(iter, &iter_key_len));

    rrc = rsbr_iter_next_or_prev(iter, true, false);
    if (rrc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri,
                          "rsbr_iter_next_or_prev(%s): %s",
                          RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB,
                          strerror(-rrc));
        rocksdb_iter_destroy(iter);
        return rrc;
    }

    SIMPLE_LOG_MSG(LL_WARN, "prev-last-key='%.*s'",
                   (int)iter_key_len, rocksdb_iter_key(iter, &iter_key_len));

    // There's no key entry or header key here.
    if (rsbr_string_matches_iter_key(RAFT_LOG_HEADER_ROCKSDB,
                                            RAFT_LOG_HEADER_ROCKSDB_STRLEN,
                                            iter, false))
    {
         rocksdb_iter_destroy(iter);
         return 0;
    }
    else if (!rsbr_string_matches_iter_key(
                 RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB,
                 RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB_STRLEN, iter, false))
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "key='%.*s' does not have expected prefix: %s",
                       (int)iter_key_len,
                       rocksdb_iter_key(iter, &iter_key_len),
                       RAFT_ENTRY_HEADER_KEY_PREFIX_ROCKSDB);

        rocksdb_iter_destroy(iter);
        return (ssize_t)-ENOKEY;
    }

    iter_key_len = 0;
    const char *iter_key = rocksdb_iter_key(iter, &iter_key_len);

    if (iter_key_len <= RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN)
        return (ssize_t)-EBADMSG;

    ssize_t last_entry_idx =
        strtoull(&iter_key[RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN], NULL, 10);

    SIMPLE_LOG_MSG(LL_WARN, "last-entry-index=%zd", last_entry_idx + 1);

    return last_entry_idx >= 0UL ? last_entry_idx + 1 : last_entry_idx;
}

static void
rsbr_log_truncate(struct raft_instance *ri, const raft_entry_idx_t entry_idx)
{
    NIOVA_ASSERT(ri);

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    NIOVA_ASSERT(rir->rir_writeoptions && rir->rir_writebatch);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    size_t entry_header_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_header_key, RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_header_key_len,
                                RAFT_ENTRY_KEY_PRINTF, entry_idx);

    rocksdb_writebatch_delete_range(rir->rir_writebatch,
                                    entry_header_key, entry_header_key_len,
                                    RAFT_LOG_LASTENTRY_ROCKSDB,
                                    RAFT_LOG_LASTENTRY_ROCKSDB_STRLEN);

    char *err = NULL;
    rocksdb_write(rir->rir_db, rir->rir_writeoptions, rir->rir_writebatch,
                  &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write(): %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);
}

static int
rsbr_destroy(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    else if (!ri->ri_backend_arg)
        return -EALREADY;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    rocksdb_close(rir->rir_db);

    rocksdb_writeoptions_destroy(rir->rir_writeoptions);
    rocksdb_readoptions_destroy(rir->rir_readoptions);
    rocksdb_options_destroy(rir->rir_options);
    rocksdb_writebatch_destroy(rir->rir_writebatch);

    rir->rir_options = NULL;
    rir->rir_readoptions = NULL;
    rir->rir_writeoptions = NULL;
    rir->rir_writebatch = NULL;

    rir->rir_db = NULL;

    niova_free(ri->ri_backend_arg);

    return 0;
}

static int
rsbr_setup(struct raft_instance *ri)
{
    if (!ri || ri->ri_backend != &ribRocksDB)
	return -EINVAL;

    else if (ri->ri_backend_arg)
        return -EALREADY;

    ri->ri_backend_arg =
        niova_calloc(1UL, sizeof(struct raft_instance_rocks_db));

    if (!ri->ri_backend_arg)
        return -ENOMEM;

    struct raft_instance_rocks_db *rir = ri->ri_backend_arg;
    int rc = 0;

    rir->rir_options = rocksdb_options_create();
    if (!rir->rir_options)
    {
        rsbr_destroy(ri);
        return -ENOMEM;
    }

//     const long int cpus = sysconf(_SC_NPROCESSORS_ONLN);
//    rocksdb_options_increase_parallelism(rir->rir_options, (int)(cpus));

//    rocksdb_options_set_use_direct_reads(rir->rir_options, 1);

    rocksdb_options_set_use_direct_io_for_flush_and_compaction(
        rir->rir_options, 1);

    rir->rir_writeoptions = rocksdb_writeoptions_create();
    if (!rir->rir_writeoptions)
    {
        rsbr_destroy(ri);
        return -ENOMEM;
    }

    rocksdb_writeoptions_set_sync(rir->rir_writeoptions, 1);

    rir->rir_readoptions = rocksdb_readoptions_create();
    if (!rir->rir_readoptions)
    {
        rc = -ENOMEM;
        goto out;
    }

    rir->rir_writebatch = rocksdb_writebatch_create();
    if (!rir->rir_writebatch)
    {
        rc = -ENOMEM;
        goto out;
    }

    char *err = NULL;

    rocksdb_options_set_create_if_missing(rir->rir_options, 0);

    rir->rir_db = rocksdb_open(rir->rir_options, ri->ri_log, &err);
    if (!rir->rir_db || err)
    {
        // DB may not be created
        err = NULL;

        rocksdb_options_set_create_if_missing(rir->rir_options, 1);
        rir->rir_db = rocksdb_open(rir->rir_options, ri->ri_log, &err);
        if (rir->rir_db && !err)
        {
            rc = rsbr_init_header(ri);
            if (rc)
            {
                SIMPLE_LOG_MSG(LL_ERROR, "rsbr_init_header(): %s",
                               strerror(rc));
                goto out;
            }
        }
        else
        {
            SIMPLE_LOG_MSG(LL_ERROR, "rocksdb_open(): %s", err);
            rc = -ENOTCONN;
            goto out;
        }
    }

    /* If all is well to this point, determine the number of entries which
     * this backend instance contains and write that value into the
     * raft_instance structure.
     */
    if (!rc && !err)
    {
        ri->ri_entries_detected_at_startup = rsbr_num_entries_calc(ri);
        if (ri->ri_entries_detected_at_startup < 0)
            rc = ri->ri_entries_detected_at_startup;
    }
out:
    if (rc || err)
        rsbr_destroy(ri);

    return rc;
}

/**
 * raft_server_backend_use_rocksdb - selects the rocksDB raft driver.
 */
void
raft_server_backend_use_rocksdb(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && !ri->ri_backend);

    ri->ri_backend = &ribRocksDB;
}

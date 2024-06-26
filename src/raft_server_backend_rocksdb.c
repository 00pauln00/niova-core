/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h> // Must precede dirent.h
#ifndef __USE_GNU // scandirat
#define __USE_GNU
#endif
#include <dirent.h>
#include <regex.h>
#include <sys/statvfs.h>
#include <unistd.h>

#define __USE_XOPEN_EXTENDED
#include <ftw.h>

#include <rocksdb/c.h>

#include "alloc.h"
#include "common.h"
#include "ctl_svc.h"
#include "file_util.h"
#include "log.h"
#include "popen_cmd.h"
#include "raft.h"
#include "raft_server_backend_rocksdb.h"
#include "regex_defines.h"
#include "registry.h"
#include "fault_inject.h"

#define RAFT_ROCKSDB_KEY_LEN_MAX 256UL

// Can become a tunable in the future
#define RAFT_ENTRY_SIZE_ROCKSDB (4 * 1024 * 1024)

#define RAFT_LOG_HEADER_ROCKSDB "a0_hdr."
#define RAFT_LOG_HEADER_ROCKSDB_STRLEN 7
#define RAFT_LOG_HEADER_FMT RAFT_LOG_HEADER_ROCKSDB"%s__%s"

#define RAFT_LOG_HEADER_ROCKSDB_END "a1_hdr."
#define RAFT_LOG_HEADER_ROCKSDB_END_STRLEN 7

#define RAFT_LOG_HEADER_ROCKSDB_LAST_SYNC       \
    RAFT_LOG_HEADER_ROCKSDB_END"last_sync"
#define RAFT_LOG_HEADER_ROCKSDB_LAST_SYNC_STRLEN 16

#define RAFT_LOG_HEADER_LAST_APPLIED_ROCKSDB    \
    RAFT_LOG_HEADER_ROCKSDB_END"last_applied"
#define RAFT_LOG_HEADER_LAST_APPLIED_ROCKSDB_STRLEN 19

#define RAFT_LOG_HEADER_UUID RAFT_LOG_HEADER_ROCKSDB_END"UUID"
#define RAFT_LOG_HEADER_UUID_STRLEN (RAFT_LOG_HEADER_ROCKSDB_END_STRLEN + 4)

#define RAFT_LOG_HEADER_UUID_PRE_RECOVERY RAFT_LOG_HEADER_UUID".pre-recovery"
#define RAFT_LOG_HEADER_UUID_PRE_RECOVERY_STRLEN \
    (RAFT_LOG_HEADER_UUID_STRLEN + 13)

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
#define RAFT_ENTRY_HEADER_KEY_PRINTF \
    RAFT_ENTRY_KEY_PREFIX_ROCKSDB RAFT_HEADER_ENTRY_KEY_FMT

/* The recovery marker filename will appear as
 * ".recovery_marker.<peer-uuid>_<db-uuid>"
 */
#define RECOVERY_MARKER_NAME "inprogress-recovery-db"
#define RECOVERY_MARKER_REGEX \
    "^"RECOVERY_MARKER_NAME"\\."UUID_REGEX_BASE"_"UUID_REGEX_BASE"$"
#define RECOVERY_MARKER_NAME_LEN 22

/* Rsync related regex's
 */
#define RECOVERY_RSYNC_TOTAL_SIZE_REGEX \
    "Total file size: "COMMA_DELIMITED_UNSIGNED_INTEGER_BASE" bytes"
#define RECOVERY_RSYNC_TOTAL_XFER_SIZE_REGEX \
    "Total transferred file size: "COMMA_DELIMITED_UNSIGNED_INTEGER_BASE" bytes"
#define RECOVERY_RSYNC_PROGRESS_LINE_RATE_REGEX \
    "[0-9]\\+.[0-9][0-9][KMGTkmgt]B/s"

#define RECOVERY_RSYNC_PROGRESS_LINE_REGEX \
    "[\r][ \t]*"COMMA_DELIMITED_UNSIGNED_INTEGER_BASE"[ \t]*[0-9]\\{1,3\\}%[ \t]*"RECOVERY_RSYNC_PROGRESS_LINE_RATE_REGEX

enum recovery_regexes
{
    RECOVERY_MARKER_NAME__regex,
    RECOVERY_RSYNC_TOTAL_SIZE__regex,
    RECOVERY_RSYNC_TOTAL_XFER_SIZE__regex,
    RECOVERY_RSYNC_PROGRESS_LINE__regex,
    RECOVERY_RSYNC_PROGRESS_LINE_RATE__regex,
    RECOVERY_CHKPT_DIRNAME__regex,
    RECOVERY__num_regex,
};

struct regex_pair
{
    regex_t     rp_regex;
    const char *rp_regex_str;
};

struct regex_pair recoveryRegexes[RECOVERY__num_regex] = {
    { .rp_regex_str = RECOVERY_MARKER_REGEX },
    { .rp_regex_str = RECOVERY_RSYNC_TOTAL_SIZE_REGEX },
    { .rp_regex_str = RECOVERY_RSYNC_TOTAL_XFER_SIZE_REGEX },
    { .rp_regex_str = RECOVERY_RSYNC_PROGRESS_LINE_REGEX },
    { .rp_regex_str = RECOVERY_RSYNC_PROGRESS_LINE_RATE_REGEX },
    { .rp_regex_str = RAFT_CHECKPOINT_DIRNAME },
};

REGISTRY_ENTRY_FILE_GENERATE;

struct raft_instance_rocks_db
{
    int                                  rir_log_fd; //dirfd to ri->ri_log
    rocksdb_t                           *rir_db;
    rocksdb_options_t                   *rir_options;
    rocksdb_writeoptions_t              *rir_writeoptions_sync;
    rocksdb_writeoptions_t              *rir_writeoptions_async;
    rocksdb_readoptions_t               *rir_readoptions;
    rocksdb_writebatch_t                *rir_writebatch;
    struct raft_server_rocksdb_cf_table *rir_cf_table;
};

void
rsbr_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(RECOVERY_MARKER_NAME_LEN ==
                        strlen(RECOVERY_MARKER_NAME));
}

static void
rsbr_entry_write(struct raft_instance *, const struct raft_entry *,
                 const struct raft_net_sm_write_supplements *);

static ssize_t
rsbr_entry_read(struct raft_instance *, struct raft_entry *);

static int
rsbr_entry_header_read(struct raft_instance *, struct raft_entry_header *);

static void
rsbr_log_truncate(struct raft_instance *, const raft_entry_idx_t);

static void // runs in checkpoint thread context
rsbr_log_reap(struct raft_instance *, const raft_entry_idx_t);

static int
rsbr_header_load(struct raft_instance *);

static int
rsbr_header_write(struct raft_instance *);

static int
rsbr_setup(struct raft_instance *);

static int
rsbr_destroy(struct raft_instance *);

static void
rsbr_sm_apply_opt(struct raft_instance *,
                  const struct raft_net_sm_write_supplements *);

static int
rsbr_sync(struct raft_instance *);

static int64_t
rsbr_checkpoint(struct raft_instance *);

static int
rsbr_bulk_recover(struct raft_instance *);

static struct raft_instance_backend ribRocksDB = {
    .rib_backend_checkpoint = rsbr_checkpoint,
    .rib_backend_recover    = rsbr_bulk_recover,
    .rib_backend_setup      = rsbr_setup,
    .rib_backend_shutdown   = rsbr_destroy,
    .rib_backend_sync       = rsbr_sync,
    .rib_entry_header_read  = rsbr_entry_header_read,
    .rib_entry_read         = rsbr_entry_read,
    .rib_entry_write        = rsbr_entry_write,
    .rib_header_load        = rsbr_header_load,
    .rib_header_write       = rsbr_header_write,
    .rib_log_reap           = rsbr_log_reap,
    .rib_log_truncate       = rsbr_log_truncate,
    .rib_sm_apply_opt       = rsbr_sm_apply_opt,
};

enum raft_instance_rocks_db_subdirs
{
    RIR_SUBDIR_DB = 0,
    RIR_SUBDIR_CHKPT_ROOT,
    RIR_SUBDIR_CHKPT_SELF,
    RIR_SUBDIR_CHKPT_PEERS,
    RIR_SUBDIR_TRASH,
    RIR_SUBDIR__MAX,
    RIR_SUBDIR__MIN = RIR_SUBDIR_DB,
};

static const char * ribSubDirs[] = {
    "db", "chkpt", "chkpt/self", "chkpt/peers", "trash",
};

static inline struct raft_instance_rocks_db *
rsbr_ri_to_rirdb(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_backend == &ribRocksDB && ri->ri_backend_arg);

    struct raft_instance_rocks_db *rir =
        (struct raft_instance_rocks_db *)ri->ri_backend_arg;

    if (ri->ri_proc_state == RAFT_PROC_STATE_RUNNING)
        NIOVA_ASSERT(rir->rir_writeoptions_sync &&
                     rir->rir_writeoptions_async && rir->rir_writebatch &&
                     rir->rir_readoptions);
    return rir;
}

static int
rsbr_remove_trash_cb(const char *path, const struct stat *stb, int typeflag,
                     struct FTW *ftwbuf)
{
    (void)stb;
    (void)ftwbuf;
    int rc;

    switch (typeflag)
    {
    case FTW_F:
        rc = unlink(path);
        rc = rc < 0 ? errno : rc;
        LOG_MSG((rc ? LL_ERROR : LL_DEBUG), "unlink(`%s'): %s",
                path, strerror(rc));
        break;
    case FTW_D: // fall through
    case FTW_DP:
        if (ftwbuf->level > 0) // Don't remove the top level dir ("trash/")
        {
            rc = rmdir(path);
            rc = rc < 0 ? errno : rc;
            LOG_MSG((rc ? LL_ERROR : LL_DEBUG), "rmdir(`%s'): %s",
                    path, strerror(rc));
        }
        break;
    default:
        LOG_MSG(LL_NOTIFY, "`%s' has unhandled type (%d)", path, typeflag);
        break;
    }

    return 0;
}

static int
rsbr_remove_trash(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    char trash_path[PATH_MAX + 1];
    int rc = snprintf(trash_path, PATH_MAX, "%s/%s",
                      ri->ri_log, ribSubDirs[RIR_SUBDIR_TRASH]);

    if (rc > PATH_MAX)
        return -ENAMETOOLONG;

    if (nftw(trash_path, rsbr_remove_trash_cb, 32,
             FTW_DEPTH | FTW_MOUNT | FTW_PHYS))
        LOG_MSG(LL_WARN, "ntfw(`%s') had non-zero return code.", trash_path);

    return 0;
}

static int
rsbr_log_dir_open_fd(const struct raft_instance *ri)
{
    return ri ? open(ri->ri_log, O_DIRECTORY | O_RDONLY) : -EINVAL;
}

static int
rsbr_try_ri_to_rirdb_logfd(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    if (!ri->ri_backend_arg)
        return -ENOENT;

    return rsbr_ri_to_rirdb(ri)->rir_log_fd;
}

static int
rsbr_move_item_to_trash(struct raft_instance *ri, const char *path)
{
    if (!ri || !path)
        return -EINVAL;

    bool close_fd = false;

    int fd = rsbr_try_ri_to_rirdb_logfd(ri); //rir_log_fd may not yet be here
    if (fd == -ENOENT)
    {
        close_fd = true;
        fd = rsbr_log_dir_open_fd(ri);
    }

    if (fd < 0)
        return -EBADF;

    uuid_t dir_name_uuid;
    uuid_generate_time(dir_name_uuid);
    DECLARE_AND_INIT_UUID_STR(dir_name, dir_name_uuid);

    char tmp_path[PATH_MAX + 1];
    int rc = snprintf(tmp_path, PATH_MAX, "%s/%s",
                      ribSubDirs[RIR_SUBDIR_TRASH], dir_name);

    if (rc > PATH_MAX)
        return -ENAMETOOLONG;

    // Make a dir to hold the trash item to avoid name conflicts
    rc = mkdirat(fd, tmp_path, 0750);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "mkdirat(`%s'): %s", tmp_path, strerror(-rc));
        return rc;
    }

    size_t path_len = strnlen(path, PATH_MAX);
    ssize_t tmp = niova_string_find_last_instance_of_char(path, '/', path_len);
    size_t file_name_idx = MAX(0, tmp);

    rc = snprintf(tmp_path, PATH_MAX, "%s/%s/%s",
                  ribSubDirs[RIR_SUBDIR_TRASH], dir_name,
                  &path[file_name_idx]);

    if (rc > PATH_MAX)
        return -ENAMETOOLONG;

    // renameat() handles case where 'path' is absolute
    rc = renameat(fd, path, fd, tmp_path);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "renameat(`%s' -> `%s'): %s", path, tmp_path,
                       strerror(-rc));
        return rc;
    }

    LOG_MSG(LL_NOTIFY, "path=%s moved to trash", path);

    if (close_fd)
    {
        int rc = close(fd);
        if (rc < 0)
        {
            rc = -errno;
            LOG_MSG(LL_ERROR, "close(%d): %s", fd, strerror(-rc));
        }
    }

    return 0;
}

static inline struct rocksdb_t *
rsbr_ri_to_rocksdb(struct raft_instance *ri)
{
    return rsbr_ri_to_rirdb(ri)->rir_db;
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
        LOG_MSG(LL_NOTIFY, "expected key='%s', got key='%.*s'",
                str, (int)iter_key_len, iter_key);

        return false;
    }

    return true;
}

static void
rsbr_write_supplements_put(const struct raft_net_sm_write_supplements *ws,
                           rocksdb_writebatch_t *wb)
{
    if (!ws || !wb)
        return;

    for (size_t i = 0; i < ws->rnsws_nitems; i++)
    {
        const struct raft_net_wr_supp *supp = &ws->rnsws_ws[i];

        for (size_t j = 0; j < supp->rnws_nkv; j++)
        {
            if (supp->rnws_handle)
                rocksdb_writebatch_put_cf(
                    wb, (rocksdb_column_family_handle_t *)supp->rnws_handle,
                    (const char *)supp->rnws_keys[j],
                    supp->rnws_key_sizes[j],
                    (const char *)supp->rnws_values[j],
                    supp->rnws_value_sizes[j]);
            else
                rocksdb_writebatch_put(wb,
                                       (const char *)supp->rnws_keys[j],
                                       supp->rnws_key_sizes[j],
                                       (const char *)supp->rnws_values[j],
                                       supp->rnws_value_sizes[j]);
        }
    }
}

static void
rsb_sm_apply_add_last_applied_kv(struct raft_instance_rocks_db *rir,
                                 const raft_entry_idx_t apply_idx,
                                 uint64_t apply_cumu_crc)
{
    NIOVA_ASSERT(rir);

    uint64_t vals[2] = {apply_idx, apply_cumu_crc};

    rocksdb_writebatch_put(rir->rir_writebatch,
                           RAFT_LOG_HEADER_LAST_APPLIED_ROCKSDB,
                           RAFT_LOG_HEADER_LAST_APPLIED_ROCKSDB_STRLEN,
                           (const char *)vals, sizeof(uint64_t) * 2);
}

static int
rsbr_get_exact_val_size(struct raft_instance_rocks_db *rir,
                        const char *key, size_t key_len,
                        void *value, size_t expected_value_len);

static void
rsb_sm_get_last_applied_kv_idx(struct raft_instance *ri)
{
    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);
    uint64_t vals[2] = {0};

    int rc =
        rsbr_get_exact_val_size(rir, RAFT_LOG_HEADER_LAST_APPLIED_ROCKSDB,
                                RAFT_LOG_HEADER_LAST_APPLIED_ROCKSDB_STRLEN,
                                (void *)vals, sizeof(uint64_t) * 2);
    if (rc)
    {
        DBG_RAFT_INSTANCE((rc == -ENOENT ? LL_NOTIFY : LL_ERROR), ri,
                          "failed: %s", strerror(-rc));
    }
    else
    {
        DBG_RAFT_INSTANCE(LL_WARN, ri, "rsbr-last-applied-idx=%ld crc=%x",
                          vals[0], (crc32_t)vals[1]);

        raft_server_backend_setup_last_applied(ri, (raft_entry_idx_t)vals[0],
                                               (crc32_t)vals[1]);
    }
}

static void
rsb_sm_get_instance_uuid(struct raft_instance *ri)
{
    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    uuid_t instance_uuid = {0};

    int rc = rsbr_get_exact_val_size(rir, RAFT_LOG_HEADER_UUID,
                                     RAFT_LOG_HEADER_UUID_STRLEN,
                                     (char *)instance_uuid, sizeof(uuid_t));

    DBG_RAFT_INSTANCE_FATAL_IF(rc, ri, "rsbr_get_exact_val_size(): %s",
                               strerror(-rc));

    uuid_copy(ri->ri_db_uuid, instance_uuid);

    // Try to copy the recovery db uuid if it's present.
    rc = rsbr_get_exact_val_size(rir, RAFT_LOG_HEADER_UUID_PRE_RECOVERY,
                                 RAFT_LOG_HEADER_UUID_PRE_RECOVERY_STRLEN,
                                 (char *)instance_uuid, sizeof(uuid_t));
    if (!rc)
        uuid_copy(ri->ri_db_recovery_uuid, instance_uuid);
}

static void
rsbr_sm_apply_opt(struct raft_instance *ri,
                  const struct raft_net_sm_write_supplements *ws)
{
    NIOVA_ASSERT(ri);
    if (!ws)
        return;

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "idx=%ld cumu-crc=%x",
                      ri->ri_last_applied_idx,
                      ri->ri_last_applied_cumulative_crc);

    const uint64_t la_crc = ri->ri_last_applied_cumulative_crc;
    const raft_entry_idx_t la_idx = ri->ri_last_applied_idx;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    rsb_sm_apply_add_last_applied_kv(rir, la_idx, la_crc);

    // Attach any supplemental writes to the rocksdb-writebatch
    rsbr_write_supplements_put(ws, rir->rir_writebatch);

    char *err = NULL;

    /* Apply_opt does not force a sync of the WAL, this is because in the case
     * of a failure, the raft entry will be re-applied.
     * The api may need to accept the write options from the SM at some point,
     * however, the sync WAL option generally be avoided here.
     */
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_async,
                  rir->rir_writebatch, &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write():  %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);
}

/**
 * rsbr_entry_header_write_recovery_scrub - this function writes
 *    a header independently from its entry.  It is only used by recovery.
 */
static void
rsbr_entry_header_write_recovery_scrub(struct raft_instance *ri,
                                       const struct raft_entry_header *reh)
{
    NIOVA_ASSERT(ri && reh && raft_instance_is_recovering(ri));

    raft_entry_idx_t entry_idx = reh->reh_index;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    size_t entry_header_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_header_key,
                                (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_header_key_len,
                                RAFT_ENTRY_HEADER_KEY_PRINTF, entry_idx);

    rocksdb_writebatch_put(rir->rir_writebatch, entry_header_key,
                           entry_header_key_len, (const char *)reh,
                           sizeof(struct raft_entry_header));
    char *err = NULL;
    // Always use synchronous writes here.
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_async,
                  rir->rir_writebatch, &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write():  %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);
}

static void
rsbr_entry_write(struct raft_instance *ri, const struct raft_entry *re,
                 const struct raft_net_sm_write_supplements *ws)
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
    DECL_AND_FMT_STRING_RET_LEN(entry_header_key,
                                (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_header_key_len,
                                RAFT_ENTRY_HEADER_KEY_PRINTF, entry_idx);

    rocksdb_writebatch_put(rir->rir_writebatch, entry_header_key,
                           entry_header_key_len, (const char *)&re->re_header,
                           sizeof(struct raft_entry_header));

    size_t entry_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_key,
                                (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_key_len,
                                RAFT_ENTRY_KEY_PRINTF, entry_idx);

    // Store an entry for every header, even if the entry is empty.
    const char x = '\0';
    const char *entry_val = entry_size ? re->re_data : &x;
    if (!entry_size)
        entry_size = 1;

    rocksdb_writebatch_put(rir->rir_writebatch, entry_key, entry_key_len,
                           entry_val, entry_size);

    // Attach any supplemental writes to the rocksdb-writebatch
    rsbr_write_supplements_put(ws, rir->rir_writebatch);

    char *err = NULL;
    rocksdb_write(rir->rir_db,
                  (raft_server_does_synchronous_writes(ri) ?
                   rir->rir_writeoptions_sync : rir->rir_writeoptions_async),
                  rir->rir_writebatch, &err);

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
    DECL_AND_FMT_STRING_RET_LEN(entry_header_key,
                                (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_header_key_len,
                                RAFT_ENTRY_HEADER_KEY_PRINTF, reh->reh_index);

    int rc = rsbr_get_exact_val_size(rir, entry_header_key,
                                     entry_header_key_len,
                                     (void *)reh,
                                     sizeof(struct raft_entry_header));
    if (rc)
        LOG_MSG(LL_ERROR, "rsbr_get_exact_val_size('%s'): %s",
                entry_header_key, strerror(rc));

    return rc;
}

static ssize_t
rsbr_entry_read(struct raft_instance *ri, struct raft_entry *re)
{
    if (!ri || !re)
        return -EINVAL;

    // XXx if the caller has already obtained the header then bypass this read
    int rc = rsbr_entry_header_read(ri, &re->re_header);
    if (rc)
        return rc;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    size_t entry_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_key, (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_key_len,
                                RAFT_ENTRY_KEY_PRINTF,
                                re->re_header.reh_index);

    rc = rsbr_get_exact_val_size(rir, entry_key, entry_key_len,
                                 (void *)re->re_data,
                                 re->re_header.reh_data_size);
    if (rc)
        LOG_MSG(LL_ERROR, "rsbr_get_exact_val_size('%s'): %s",
                entry_key, strerror(rc));

    return rc < 0 ? rc :
        (ssize_t)(re->re_header.reh_data_size +
                  sizeof(struct raft_entry_header));
//Xxx this is wonky
}

static int
rsbr_header_load(struct raft_instance *ri)
{
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    size_t header_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(header_key, (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
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
            rc = -EBADMSG;
        else
            DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");
    }
    return rc;
}

static int
rsbr_header_write(struct raft_instance *ri)
{
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    NIOVA_ASSERT(rir->rir_writeoptions_sync && rir->rir_writebatch);
    rocksdb_writebatch_clear(rir->rir_writebatch);

    size_t key_len;
    DECL_AND_FMT_STRING_RET_LEN(header_key, (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&key_len, RAFT_LOG_HEADER_FMT,
                                ri->ri_raft_uuid_str,
                                ri->ri_this_peer_uuid_str);

    rocksdb_writebatch_put(rir->rir_writebatch, header_key, key_len,
                           (const char *)&ri->ri_log_hdr,
                           sizeof(struct raft_log_header));

    char *err = NULL;
    // Log header writes are always synchronous
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_sync, rir->rir_writebatch,
                  &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write():  %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    return 0;
}

static int // Call from rib_backend_setup()
rsbr_init_header(struct raft_instance *ri)
{
    if (!ri || !ri->ri_raft_uuid_str || !ri->ri_this_peer_uuid_str)
        return -EINVAL;

    if (raft_instance_is_recovering(ri))
    {
        /* Recovering raft instance must keep the contents of the previous
         * header to maintain the term and voted-for values.
         */
        NIOVA_ASSERT(ri->ri_log_hdr.rlh_magic == RAFT_HEADER_MAGIC);
    }
    else
    {
        memset(&ri->ri_log_hdr, 0, sizeof(struct raft_log_header));

        // Since we're initializing the header block this is ok
        ri->ri_log_hdr.rlh_magic = RAFT_HEADER_MAGIC;
    }

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    NIOVA_ASSERT(rir->rir_writeoptions_sync && rir->rir_writebatch);
    rocksdb_writebatch_clear(rir->rir_writebatch);

    size_t key_len;
    DECL_AND_FMT_STRING_RET_LEN(last_key, (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&key_len, RAFT_LOG_LASTENTRY_FMT,
                                ri->ri_raft_uuid_str,
                                ri->ri_this_peer_uuid_str);

    rocksdb_writebatch_put(rir->rir_writebatch, last_key, key_len,
                           (const char *)&ri->ri_log_hdr,
                           sizeof(struct raft_log_header));

    // Generate and store the db-instance UUID
    uuid_t instance_uuid;
    uuid_generate(instance_uuid);
    rocksdb_writebatch_put(rir->rir_writebatch, RAFT_LOG_HEADER_UUID,
                           RAFT_LOG_HEADER_UUID_STRLEN,
                           (const char *)instance_uuid, sizeof(uuid_t));

    // We must put the current raft-log-header uuid if recovering
    if (raft_instance_is_recovering(ri))
    {
        struct raft_recovery_handle *rrh = &ri->ri_recovery_handle;

        if (uuid_compare(rrh->rrh_peer_db_uuid, ri->ri_db_recovery_uuid))
            rocksdb_writebatch_put(rir->rir_writebatch,
                                   RAFT_LOG_HEADER_UUID_PRE_RECOVERY,
                                   RAFT_LOG_HEADER_UUID_PRE_RECOVERY_STRLEN,
                                   (const char *)rrh->rrh_peer_db_uuid,
                                   sizeof(uuid_t));
    }

    char *err = NULL;
    // Log header writes are always synchronous
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_sync, rir->rir_writebatch,
                  &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write():  %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    return rsbr_header_write(ri);
}

static int
rsbr_lowest_entry_get(struct raft_instance *ri, raft_entry_idx_t *lowest_idx)
{
    if (!ri || !lowest_idx)
        return -EINVAL;

    *lowest_idx = -1ULL;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    rocksdb_iterator_t *iter = rsbr_create_iterator(rir);
    if (!iter)
        return -ENOMEM;

    int rc = rsbr_iter_seek(iter, RAFT_LOG_HEADER_ROCKSDB_END,
                            RAFT_LOG_HEADER_ROCKSDB_END_STRLEN, true);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri,
                          "rsbr_iter_seek(%s): %s",
                          RAFT_LOG_HEADER_ROCKSDB_END,
                          strerror(-rc));
        rocksdb_iter_destroy(iter);
        return rc;
    }

    size_t iter_key_len = 0;
    for (bool found = false; !found;)
    {
        // Iterate forward looking for the first entry key
        rc = rsbr_iter_next_or_prev(iter, true, true);
        if (rc)
        {
            DBG_RAFT_INSTANCE(LL_ERROR, ri,
                              "rsbr_iter_next_or_prev(): %s",
                              strerror(-rc));
            break;
        }
        else if (rsbr_string_matches_iter_key(
                     RAFT_LOG_LASTENTRY_ROCKSDB,
                     RAFT_LOG_LASTENTRY_ROCKSDB_STRLEN, iter, false))
        {
            break; // no entries found in the keyspace
        }
        else if (rsbr_string_matches_iter_key(
                     RAFT_ENTRY_KEY_PREFIX_ROCKSDB,
                     RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN, iter, false))
        {
            const char *key = rocksdb_iter_key(iter, &iter_key_len);

            FATAL_IF(((strncmp(key, RAFT_ENTRY_KEY_PREFIX_ROCKSDB,
                               RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN) &&
                       key[iter_key_len - 1] != 'e')),
                     "unexpected key (`%s'), len=%zu", key, iter_key_len);

            unsigned long long val = 0;
            // The above FATAL_IF guaranteed a non-numeric trailing char

            rc = niova_string_to_unsigned_long_long(
                &key[RAFT_ENTRY_KEY_PREFIX_ROCKSDB_STRLEN], &val);

            NIOVA_ASSERT(!rc);

            *lowest_idx = (raft_entry_idx_t)val;
            found = true;
        }
    }

    SIMPLE_LOG_MSG(LL_NOTIFY, "key='%.*s' lowest-idx=%zd rc=%d",
                   (int)iter_key_len, rocksdb_iter_key(iter, &iter_key_len),
                   *lowest_idx, rc);

    rocksdb_iter_destroy(iter);

    return rc;
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

    SIMPLE_LOG_MSG(LL_NOTIFY, "last-key='%.*s'",
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

    SIMPLE_LOG_MSG(LL_NOTIFY, "prev-last-key='%.*s'",
                   (int)iter_key_len, rocksdb_iter_key(iter, &iter_key_len));

    // There's no key entry or header key here.
    if (rsbr_string_matches_iter_key(RAFT_LOG_HEADER_ROCKSDB_END,
                                     RAFT_LOG_HEADER_ROCKSDB_END_STRLEN,
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

    SIMPLE_LOG_MSG(LL_NOTIFY, "last-entry-index=%zd", last_entry_idx + 1);

    return last_entry_idx >= 0L ? last_entry_idx + 1 : last_entry_idx;
}

static void
rsbr_log_truncate(struct raft_instance *ri, const raft_entry_idx_t entry_idx)
{
    NIOVA_ASSERT(ri);

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    // Log truncate ops are always synchronous
    NIOVA_ASSERT(rir->rir_writeoptions_sync && rir->rir_writebatch);

    rocksdb_writebatch_clear(rir->rir_writebatch);

    size_t entry_header_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(entry_header_key, (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&entry_header_key_len,
                                RAFT_ENTRY_KEY_PRINTF, entry_idx);

    rocksdb_writebatch_delete_range(rir->rir_writebatch,
                                    entry_header_key, entry_header_key_len,
                                    RAFT_LOG_LASTENTRY_ROCKSDB,
                                    RAFT_LOG_LASTENTRY_ROCKSDB_STRLEN);

    char *err = NULL;
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_sync, rir->rir_writebatch,
                  &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write(): %s", err);

    rocksdb_writebatch_clear(rir->rir_writebatch);
}

static void // runs in checkpoint thread context
rsbr_log_reap(struct raft_instance *ri, const raft_entry_idx_t entry_idx)
{
    NIOVA_ASSERT(ri && rsbr_ri_to_rirdb(ri));
    NIOVA_ASSERT(entry_idx >= 0);

    /* Create our own rocksdb_writebatch_t since this runs outside of the
     * main raft thread.
     */
    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();
    NIOVA_ASSERT(wb);

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    NIOVA_ASSERT(rir->rir_writeoptions_sync);

    /* Take care to remove (and preserve) headers with their entries.  Entry
     * keys end in 'e' (versus 'h' for headers) so this operation should use
     * the lower key suffix ('e') for this operation.
     */

    size_t start_entry_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(start_entry_key,
                                (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&start_entry_key_len,
                                RAFT_ENTRY_KEY_PRINTF, (raft_entry_idx_t)0);

    size_t end_entry_key_len = 0;
    DECL_AND_FMT_STRING_RET_LEN(end_entry_key,
                                (ssize_t)RAFT_ROCKSDB_KEY_LEN_MAX,
                                (ssize_t *)&end_entry_key_len,
                                RAFT_ENTRY_KEY_PRINTF, entry_idx);

    rocksdb_writebatch_delete_range(wb, start_entry_key, start_entry_key_len,
                                    end_entry_key, end_entry_key_len);

    char *err = NULL;
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_sync, wb, &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write(): %s", err);
    rocksdb_writebatch_destroy(wb);

    // Compact the items which were just removed.
    rocksdb_compact_range(rir->rir_db, start_entry_key, start_entry_key_len,
                          end_entry_key, end_entry_key_len);

    rocksdb_delete_file_in_range(rir->rir_db, start_entry_key,
                                 start_entry_key_len, end_entry_key,
                                 end_entry_key_len, &err);
    if (err)
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "rocksdb_delete_file_in_range(): %s",
                          err);
}

static int // runs in sync thread context
rsbr_sync(struct raft_instance *ri)
{
    if (!ri || !rsbr_ri_to_rirdb(ri))
        return -EINVAL;

    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();
    NIOVA_ASSERT(wb);

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    NIOVA_ASSERT(rir->rir_writeoptions_sync);

    struct timespec ts;
    niova_realtime_coarse_clock(&ts);

    rocksdb_writebatch_put(wb,
                           RAFT_LOG_HEADER_ROCKSDB_LAST_SYNC,
                           RAFT_LOG_HEADER_ROCKSDB_LAST_SYNC_STRLEN,
                           (const char *)&ts, sizeof(struct timespec));

    char *err = NULL;
    rocksdb_write(rir->rir_db, rir->rir_writeoptions_sync, wb, &err);

    DBG_RAFT_INSTANCE_FATAL_IF((err), ri, "rocksdb_write(): %s", err);

    rocksdb_writebatch_destroy(wb);

    return 0;
}

#define CHKPT_RESTORE_PATH_FMT "%s_%s"
#define CHKPT_RESTORE_PATH_FMT_ARGS(db, peer) db, peer
#define CHKPT_PATH_FMT CHKPT_RESTORE_PATH_FMT"_%020lu"
#define CHKPT_PATH_FMT_ARGS(db, peer, idx) db, peer, idx

static int
rsbr_recovery_rsync_path_build(const char *base, const uuid_t peer_id,
                               const uuid_t db_id, char *restore_path,
                               size_t len)
{
    if (!base || uuid_is_null(peer_id) || uuid_is_null(db_id) ||
        !restore_path || !len)
        return -EINVAL;

    DECLARE_AND_INIT_UUID_STR(peer_uuid, peer_id);
    DECLARE_AND_INIT_UUID_STR(db_uuid, db_id);

    int rc = snprintf(restore_path, len, "%s/%s/"CHKPT_RESTORE_PATH_FMT,
                      base, ribSubDirs[RIR_SUBDIR_CHKPT_PEERS],
                      CHKPT_RESTORE_PATH_FMT_ARGS(db_uuid, peer_uuid));

    return rc >= (ssize_t)len ? -ENAMETOOLONG : 0;
}

static int
rsbr_recovery_inprogress_path_build(const char *base,
                                    const struct raft_recovery_handle *rrh,
                                    char *dest, size_t len)
{
    if (!base || !rrh || !dest || !len || uuid_is_null(rrh->rrh_peer_uuid) ||
        uuid_is_null(rrh->rrh_peer_db_uuid))
        return -EINVAL;

    DECLARE_AND_INIT_UUID_STR(peer_uuid, rrh->rrh_peer_uuid);
    DECLARE_AND_INIT_UUID_STR(db_uuid, rrh->rrh_peer_db_uuid);

    int rc = snprintf(dest, len, "%s/%s."CHKPT_RESTORE_PATH_FMT,
                      base, RECOVERY_MARKER_NAME,
                      CHKPT_RESTORE_PATH_FMT_ARGS(db_uuid, peer_uuid));

    return rc >= (ssize_t)len ? -ENAMETOOLONG : 0;
}

static int
rsbr_checkpoint_path_build(const char *base, const uuid_t peer_id,
                           const uuid_t db_id, raft_entry_idx_t sync_idx,
                           bool local, bool initial, char *chkpt_path,
                           size_t len)
{
    if (!base || uuid_is_null(peer_id) || uuid_is_null(db_id) || !chkpt_path ||
        !len || sync_idx < 0)
        return -EINVAL;

    DECLARE_AND_INIT_UUID_STR(peer_uuid, peer_id);
    DECLARE_AND_INIT_UUID_STR(db_uuid, db_id);

    int rc = snprintf(chkpt_path, len, "%s/%s/%s"CHKPT_PATH_FMT,
                      base, (local ?
                             ribSubDirs[RIR_SUBDIR_CHKPT_SELF] :
                             ribSubDirs[RIR_SUBDIR_CHKPT_PEERS]),
                      initial ? ".in-progress_" : "",
                      CHKPT_PATH_FMT_ARGS(db_uuid, peer_uuid, sync_idx));

    return rc > (ssize_t)len ? -ENAMETOOLONG : 0;
}

static int
rsbr_self_chkpt_scan(struct raft_instance *ri, struct raft_instance_rocks_db *rir);

static void
rsbr_checkpoint_cleanup(struct raft_instance *ri,
                        struct raft_instance_rocks_db *rir)
{
    NIOVA_ASSERT(ri && rir);

    int chkpt_scan_rc = rsbr_self_chkpt_scan(ri, rir);
    if (chkpt_scan_rc)
        LOG_MSG(LL_WARN, "rsbr_self_chkpt_scan(): %s",
                strerror(-chkpt_scan_rc));

    int trash_rc = rsbr_remove_trash(ri);
    if (trash_rc)
        LOG_MSG(LL_WARN, "rsbr_remove_trash(): %s", strerror(-trash_rc));
}

static int64_t // checkpoint thread context
rsbr_checkpoint(struct raft_instance *ri)
{
    if (!ri || !rsbr_ri_to_rirdb(ri))
        return -EINVAL;

    const raft_entry_idx_t chkpt_idx =
        raft_server_instance_chkpt_compact_max_idx(ri);

    if (chkpt_idx < 0) // Don't checkpoint if the db is empty
        return -ENODATA;

    else if (chkpt_idx == ri->ri_checkpoint_last_idx)
        return -EALREADY;

    char chkpt_path[PATH_MAX] = {0};
    char chkpt_tmp_path[PATH_MAX] = {0};

    int64_t rc = rsbr_checkpoint_path_build(ri->ri_log,
                                            RAFT_INSTANCE_2_SELF_UUID(ri),
                                            ri->ri_db_uuid, chkpt_idx,
                                            true, true, chkpt_tmp_path,
                                            PATH_MAX);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "rsbr_checkpoint_path_build(): %s",
                          strerror(-rc));
        return -rc;
    }

    rc = rsbr_checkpoint_path_build(ri->ri_log,
                                    RAFT_INSTANCE_2_SELF_UUID(ri),
                                    ri->ri_db_uuid, chkpt_idx, true, false,
                                    chkpt_path, PATH_MAX);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "rsbr_checkpoint_path_build(): %s",
                          strerror(-rc));
        return -rc;
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "tmp-path=%s final-path=%s",
                   chkpt_tmp_path, chkpt_path);

    struct stat stb;
    // Stale tmp path is placed into the trash
    rc = stat(chkpt_tmp_path, &stb);
    if (!rc)
    {
        rc = rsbr_move_item_to_trash(ri, chkpt_tmp_path);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "rsbr_move_dir_to_trash(`%s'): %s",
                           chkpt_tmp_path, strerror(-rc));
            return rc;
        }
    }

    /* The rename below atomically moves the completed checkpoint into
     * 'chkpt_path', therefore, if 'chkpt_path' exists we can assume it's
     * valid.
     */
    rc = stat(chkpt_path, &stb);
    if (!rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "chkpt_path=%s already exsits", chkpt_path);
        return -EALREADY;
    }

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    char *err = NULL;

    rocksdb_checkpoint_t *cp =
        rocksdb_checkpoint_object_create(rir->rir_db, &err);

    if (err)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri,
                          "rocksdb_checkpoint_object_create(): %s", err);
        return -ENOMEM;
    }

    rocksdb_checkpoint_create(cp, chkpt_tmp_path, 0, &err);

    rocksdb_checkpoint_object_destroy(cp);

    if (err)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "rocksdb_checkpoint_create(): %s",
                          err);
        return -ENOMEM;
    }

    // Move the directory to its intended location.
    rc = rename(chkpt_tmp_path, chkpt_path);
    if (rc)
        rc = -errno;

    DBG_RAFT_INSTANCE((rc ? LL_ERROR : LL_NOTIFY), ri, "checkpoint@%s: %s",
                      chkpt_path, strerror(-rc));

    rsbr_checkpoint_cleanup(ri, rir);

    return rc ? rc : chkpt_idx;
}

static int
rsbr_scandir_recovery_marker_cb(const struct dirent *dent)
{
    SIMPLE_LOG_MSG(LL_NOTIFY, "d_name=%s d_type=%hhu",
                   dent->d_name, dent->d_type);

    return dent->d_type == DT_DIR
        ? (!regexec(&recoveryRegexes[RECOVERY_MARKER_NAME__regex].rp_regex,
                    dent->d_name, 0, NULL, 0))
        : 0; // returning '0' filters entry per scandirat(3)
}

static int
rsbr_recovery_marker_scan(struct raft_instance *ri)
{
    if (!ri || !ri->ri_backend_arg)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = ri->ri_backend_arg;
    if (rir->rir_log_fd < 0)
        return -EBADF;

    // ri_incomplete_recovery conveys this function's result
    ri->ri_incomplete_recovery = false;

    int rc = 0;

    struct dirent **recovery_marker_dents = NULL;
    int nents = scandirat(rir->rir_log_fd, ".", &recovery_marker_dents,
                          rsbr_scandir_recovery_marker_cb, alphasort);
    if (nents < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "scandirat(): %s", strerror(-rc));
        return rc;
    }

    int n = nents;
    if (n > 1)
    {
        rc = -E2BIG;
        LOG_MSG(LL_ERROR, "Multiple recovery markers detected");
    }
    else if (nents == 1)
    {
        LOG_MSG(LL_WARN, "Found lingering recovery marker `%s'",
                recovery_marker_dents[0]->d_name);

        const char *dname = recovery_marker_dents[0]->d_name;
        char peer_uuid_str[UUID_STR_LEN] = {0};
        char db_uuid_str[UUID_STR_LEN] = {0};

        /* These should be safe since rsbr_scandir_recovery_marker_cb()
         * performed a regex check on the dname.
         */
        strncpy(db_uuid_str, &dname[RECOVERY_MARKER_NAME_LEN],
                UUID_STR_LEN - 1);

        strncpy(peer_uuid_str,  // includes "_"
                &dname[RECOVERY_MARKER_NAME_LEN + UUID_STR_LEN],
                UUID_STR_LEN - 1);

        rc = raft_server_init_recovery_handle_from_marker(ri, db_uuid_str,
                                                          peer_uuid_str);
        if (rc)
            LOG_MSG(
                LL_ERROR,
                "raft_server_init_recovery_handle_from_marker(%s): %s (%s:%s)",
                recovery_marker_dents[0]->d_name, strerror(-rc), peer_uuid_str,
                db_uuid_str);
        else
            ri->ri_incomplete_recovery = true; // found valid marker
    }

    // Cleanup scandirat memory allocations
    while (nents--)
        free(recovery_marker_dents[nents]);

    free(recovery_marker_dents);

    return rc;
}

static int
rsbr_startup_self_chkpt_scan_cb(const struct dirent *dent)
{
    SIMPLE_LOG_MSG(LL_NOTIFY, "d_name=%s", dent->d_name);

    return !regexec(&recoveryRegexes[RECOVERY_CHKPT_DIRNAME__regex].rp_regex,
                    dent->d_name, 0, NULL, 0);
}

#define CHKPT_FILENAME_LEN ((UUID_STR_LEN * 2) + 20)

static int
rsbr_chkpt_scan_parse_entry(const struct dirent *dent, uuid_t db_uuid,
                            uuid_t peer_uuid, raft_entry_idx_t *chkpt_idx)
{
    if (!dent || !chkpt_idx)
        return -EINVAL;

    else if (dent->d_reclen < CHKPT_FILENAME_LEN) // Imprecise sanity check
        return -ERANGE;

    else if (dent->d_type != DT_DIR) // Immediately filter non-directories
        return -ENOTDIR;

    char dname[CHKPT_FILENAME_LEN + 1] = {0};
    strncpy(dname, dent->d_name, CHKPT_FILENAME_LEN);
    dname[UUID_STR_LEN - 1] = '\0';
    dname[(UUID_STR_LEN * 2) - 1] = '\0';

    return (uuid_parse(&dname[0], db_uuid) ||
            uuid_parse(&dname[UUID_STR_LEN], peer_uuid) ||
            niova_string_to_unsigned_long_long(
                &dname[UUID_STR_LEN * 2],
                (unsigned long long *)chkpt_idx)) ? -EBADMSG : 0;
}

static int
rsbr_self_chkpt_scan(struct raft_instance *ri, struct raft_instance_rocks_db *rir)
{
    NIOVA_ASSERT(ri && rir && rir->rir_log_fd >= 0 && ri->ri_csn_this_peer);

    struct dirent **self_chkpts = NULL;
    int nents = scandirat(rir->rir_log_fd, ribSubDirs[RIR_SUBDIR_CHKPT_SELF],
                          &self_chkpts, rsbr_startup_self_chkpt_scan_cb,
                          alphasort);
    if (nents < 0)
    {
        int rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "scandirat(): %s", strerror(-rc));
        return rc;
    }

    size_t num_checkpoints_found = 0;
    while (nents--)
    {
        const struct dirent *dent = self_chkpts[nents];
        uuid_t peer_uuid = {0};
        uuid_t db_uuid = {0};
        raft_entry_idx_t chkpt_idx = 0;
        bool trash = false;

        int rc = rsbr_chkpt_scan_parse_entry(dent, db_uuid, peer_uuid,
                                             &chkpt_idx);
        if (rc)
        {
            LOG_MSG(LL_DEBUG, "d_ent=`%s': %s", dent->d_name, strerror(-rc));
            trash = true;
        }
        else
        {
            if (uuid_compare(ri->ri_csn_this_peer->csn_uuid, peer_uuid) ||
                uuid_compare(ri->ri_db_uuid, db_uuid))
            {
                trash = true;
            }
            else
            {
                ++num_checkpoints_found;
                if (num_checkpoints_found == 1)
                {
                    if (raft_instance_is_booting(ri))
                    {
                        ri->ri_checkpoint_last_idx = chkpt_idx;
                        LOG_MSG(LL_WARN, "last-checkpoint-idx=%ld", chkpt_idx);
                    }
                }
                else if (num_checkpoints_found > ri->ri_num_checkpoints)
                {
                    trash = true;
                }
            }

            SIMPLE_LOG_MSG(
                LL_NOTIFY,
                "nchk=%zu trash=%s d_ent=`%s' d_type=%hhu idx=%ld",
                num_checkpoints_found, trash ? "yes" : "no", dent->d_name,
                dent->d_type, chkpt_idx);
        }

        if (trash)
        {
            char path[PATH_MAX + 1];
            /* Convert the dent into a relative pathname which can be used by
             * rsbr_move_item_to_trash().
             */
            snprintf(path, PATH_MAX, "%s/%s",
                     ribSubDirs[RIR_SUBDIR_CHKPT_SELF], dent->d_name);

            rc = rsbr_move_item_to_trash(ri, path);
            if (rc)
                LOG_MSG(LL_WARN, "rsbr_move_item_to_trash(`%s'): %s",
                        path, strerror(-rc));
        }

        free(self_chkpts[nents]); // Release the array entry
    }

    free(self_chkpts); // Release the array

    return 0;
}

static int
rsbr_startup_checkpoint_scan(struct raft_instance *ri)
{
    if (!ri || !ri->ri_backend_arg || uuid_is_null(ri->ri_db_uuid))
        return -EINVAL;

    NIOVA_ASSERT(raft_instance_is_booting(ri));

    struct raft_instance_rocks_db *rir = ri->ri_backend_arg;
    if (rir->rir_log_fd < 0)
        return -EBADF;

    int rc = rsbr_self_chkpt_scan(ri, rir);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_startup_self_chkpt_scan(): %s",
                       strerror(-rc));
        return rc;
    }

    return 0;
}

static int
rsbr_bulk_recover_build_remote_path(const struct raft_recovery_handle *rrh,
                                    char *remote_path, size_t len)
{
    if (!rrh)
        return -EINVAL;

    DECLARE_AND_INIT_UUID_STR(peer_uuid_str, rrh->rrh_peer_uuid);

    // Lookup the csn
    struct ctl_svc_node *csn;
    int rc = ctl_svc_node_lookup(rrh->rrh_peer_uuid, &csn);
    if (rc)
    {
        LOG_MSG(LL_ERROR, "ctl_svc_node_lookup(%s): %s", peer_uuid_str,
                strerror(-rc));
        return rc;
    }

    // Find the remote store db path
    const char *remote_store = ctl_svc_node_peer_2_store(csn);
    if (!remote_store)
    {
        LOG_MSG(LL_ERROR, "ctl_svc_node_peer_2_store(%s): NULL",
                peer_uuid_str);
        rc = -ENOENT;
        goto out;
    }

    char remote_relative_path[PATH_MAX];
    /* Use the path building method.  Note that 'local' is set to true since
     * the path is "local" relative to the remote peer.  In other words, we're
     * building the path to be used on the remote end.
     */
    rc = rsbr_checkpoint_path_build(remote_store, rrh->rrh_peer_uuid,
                                    rrh->rrh_peer_db_uuid,
                                    rrh->rrh_peer_chkpt_idx, true, false,
                                    remote_relative_path, PATH_MAX);
    if (rc)
    {
        LOG_MSG(LL_ERROR, "rsbr_checkpoint_path_build(%s): %s",
                peer_uuid_str, strerror(-rc));
        goto out;
    }

    rc = snprintf(remote_path, len - 1, "%s:%s",
                  ctl_svc_node_peer_2_ipaddr(csn), remote_relative_path);
    if (rc >= (ssize_t)len)
    {
        LOG_MSG(LL_ERROR, "snprintf() overrun (rc=%d)", rc);
        rc = -ENAMETOOLONG;
        goto out;
    }
    else
    {
        rc = 0;
    }
out:
    ctl_svc_node_put(csn); // Must 'put' the csn to avoid leaks
    return rc;
}

static int
rsbr_bulk_recover_try_parse(struct raft_recovery_handle *rrh,
                            const char *output, size_t len,
                            enum recovery_regexes val_type)
{
    NIOVA_ASSERT(rrh && output &&
                 ((val_type == RECOVERY_RSYNC_TOTAL_SIZE__regex) ||
                  (val_type == RECOVERY_RSYNC_TOTAL_XFER_SIZE__regex)));

    regex_t *regex = &recoveryRegexes[val_type].rp_regex;

    int rc = regexec(regex, output, 0, NULL, 0);
    if (rc)
        return 0; // non-match is ok

    ssize_t *val = (val_type == RECOVERY_RSYNC_TOTAL_SIZE__regex) ?
        &rrh->rrh_chkpt_size : &rrh->rrh_remaining;

    rc = niova_parse_comma_delimited_uint_string(output, len,
                                                 (unsigned long long *)val);
    if (rc)
    {
        *val = -1;
        SIMPLE_LOG_MSG(LL_WARN,
                       "niova_parse_comma_delimited_uint_string(): %s",
                       strerror(-rc));
    }

    return rc;
}

static popen_cmd_cb_ctx_t
rsbr_bulk_recover_calculate_remaining_rsync_cb(const char *output, size_t len,
                                               void *arg)
{
    if (!output || !arg || !len)
        return -EINVAL;

    struct raft_recovery_handle *rrh = (struct raft_recovery_handle *)arg;

    LOG_MSG(LL_TRACE, "%zu >> %s", len, output);

    int rc = 0;

    if (rrh->rrh_chkpt_size < 0)
    {
        rc = rsbr_bulk_recover_try_parse(rrh, output, len,
                                         RECOVERY_RSYNC_TOTAL_SIZE__regex);
        if (!rc && rrh->rrh_chkpt_size > 0)
            SIMPLE_LOG_MSG(LL_WARN, "rrh_chkpt_size=%zd",
                           rrh->rrh_chkpt_size);
    }

    if (rrh->rrh_remaining < 0)
    {
        rc = rsbr_bulk_recover_try_parse(
            rrh, output, len, RECOVERY_RSYNC_TOTAL_XFER_SIZE__regex);

        if (!rc && rrh->rrh_remaining >= 0)
            SIMPLE_LOG_MSG(LL_WARN, "rrh_remaining=%zd", rrh->rrh_remaining);
    }

    return rc;
}

/**
 * rsbr_bulk_recover_calculate_remaining - this function calls popen() to
 *    launch an 'rsync' probe which will calculate the size of the remote
 *    checkpoint and the amount requiring transfer.
 */
static popen_cmd_t // performs a fork / exec via popen()
rsbr_bulk_recover_calculate_remaining_rsync(struct raft_recovery_handle *rrh,
                                            const char *remote_path,
                                            const char *local_path)
{
    if (!rrh || !remote_path || !local_path)
        return -EINVAL;

    char cmd[PATH_MAX + 1] = {0};

    int rc = snprintf(cmd, PATH_MAX, "rsync -an --info=stats2 %s/ %s 2>&1",
                      remote_path, local_path);
    if (rc > PATH_MAX)
        return -ENAMETOOLONG;

    LOG_MSG(LL_DEBUG, "cmd=`%s'", cmd);

    rc = popen_cmd_out(cmd, rsbr_bulk_recover_calculate_remaining_rsync_cb,
                       (void *)rrh);
    if (rc)
        LOG_MSG(LL_ERROR, "popen_cmd_out(rsync): %s", strerror(-rc));

    return rc;
}

static void
rsbr_bulk_recover_xfer_rsync_try_rate_parse(const char *output,
                                            struct raft_recovery_handle *rrh)
{
    regex_t *regex =
        &recoveryRegexes[RECOVERY_RSYNC_PROGRESS_LINE_RATE__regex].rp_regex;

    regmatch_t match = {0};
    if (!regexec(regex, output, 1, &match, 0))
    {
        strncpy(rrh->rrh_rate_bytes_per_sec, &output[match.rm_so],
                MIN(BW_RATE_LEN, match.rm_eo - match.rm_so));

        SIMPLE_LOG_MSG(LL_TRACE, "%s", rrh->rrh_rate_bytes_per_sec);
    }
}

static popen_cmd_cb_ctx_t
rsbr_bulk_recover_xfer_rsync_cb(const char *output, size_t len, void *arg)
{
    if (!output || !arg || !len)
        return -EINVAL;

    struct raft_recovery_handle *rrh = (struct raft_recovery_handle *)arg;

    regex_t *regex =
        &recoveryRegexes[RECOVERY_RSYNC_PROGRESS_LINE__regex].rp_regex;

    int rc = regexec(regex, output, 0, NULL, 0);
    if (rc)
    {
        LOG_MSG(LL_TRACE, "%zu >> %s", len, output);

        return 0; // non-match is ok
    }
    unsigned long long val = 0;

    rc = niova_parse_comma_delimited_uint_string(output, len, &val);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN,
                       "niova_parse_comma_delimited_uint_string(): %s",
                       strerror(-rc));
        return rc;
    }

    if (val > rrh->rrh_completed)
        rrh->rrh_completed = val;

    // Grab the rate from the rsync "progress2" output
    rsbr_bulk_recover_xfer_rsync_try_rate_parse(output, rrh);

    SIMPLE_LOG_MSG(LL_TRACE, "val=%llu rate=%s",
                   val, rrh->rrh_rate_bytes_per_sec);

    return 0;
}

static popen_cmd_t // performs a fork / exec via popen()
rsbr_bulk_recover_xfer_rsync(struct raft_recovery_handle *rrh,
                             const char *remote_path, const char *local_path)
{
    if (!rrh || !remote_path || !local_path)
        return -EINVAL;

    char cmd[PATH_MAX + 1] = {0};

    /* Ensure that a '/' is appended to the remote path so that rsync does
     * not apply the remote's parent directory to the local path.
     */
    int rc;
    if (FAULT_INJECT(raft_limit_rsync_bw))
        rc = snprintf(cmd, PATH_MAX,
                      "rsync -a --bwlimit=1 --info=progress2 %s/ %s 2>&1",
                      remote_path, local_path);
    else
        rc = snprintf(cmd, PATH_MAX, "rsync -a --info=progress2 %s/ %s 2>&1",
                      remote_path, local_path);
    if (rc > PATH_MAX)
        return -ENAMETOOLONG;

    LOG_MSG(LL_DEBUG, "cmd=`%s'", cmd);

    rc = popen_cmd_out(cmd, rsbr_bulk_recover_xfer_rsync_cb, (void *)rrh);
    if (rc)
        LOG_MSG(LL_ERROR, "popen_cmd_out(rsync): %s", strerror(-rc));

    return rc;
}


#define BULK_RECOVERY_RSYNC_RETRY_SECS 10
#define BULK_RECOVERY_RSYNC_RETRY_MAX  4

#define RSBR_BULK_RECOVER_RSYNC_CMD(func, rrh, remote_path, local_path)  \
({                                                                  \
    int rc = 0;                                                         \
    int nretries = 0;                                                   \
    do {                                                                \
        rc = func(rrh, remote_path, local_path);                            \
        if (rc)                                                         \
        {                                                               \
            bool retry = ++nretries >= BULK_RECOVERY_RSYNC_RETRY_MAX ?      \
                false : true;                                           \
            SIMPLE_LOG_MSG(                                             \
                LL_ERROR,                                               \
                #func": %s, retry=%s in %u seconds",                    \
                strerror(-rc), retry ? "yes" : "no",                    \
                retry ? (BULK_RECOVERY_RSYNC_RETRY_SECS * nretries) : 0); \
            if (!retry)                                                 \
                break;                                                  \
            niova_sleep(BULK_RECOVERY_RSYNC_RETRY_SECS * nretries);     \
        }                                                               \
    } while (rc);                                                       \
    rc;                                                                 \
})

static int
rsbr_bulk_recover_xfer(struct raft_recovery_handle *rrh,
                       const char *remote_path, const char *local_path)
{
    return RSBR_BULK_RECOVER_RSYNC_CMD(rsbr_bulk_recover_xfer_rsync, rrh,
                                       remote_path, local_path);
}

static ssize_t
rsbr_bulk_recover_get_fs_free_space(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_log != NULL);

    struct statvfs stv;

    int rc = statvfs(ri->ri_log, &stv);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "statvfs(): %s", strerror(-rc));
        return rc;
    }
    else if (stv.f_flag & ST_RDONLY)
    {
        rc = -EPERM;
        SIMPLE_LOG_MSG(LL_ERROR, "%s is a read-only-fs", ri->ri_log);
        return rc;
    }

    ssize_t available_cap = stv.f_bsize * stv.f_bavail;

    SIMPLE_LOG_MSG(LL_WARN, "%s available-capacity=%zd",
                   ri->ri_log, available_cap);

    return available_cap;
}

static int
rsbr_bulk_recover_calculate_remaining(struct raft_recovery_handle *rrh,
                                      const char *remote_path,
                                      const char *local_path,
                                      const ssize_t available_cap)
{
    int rc = RSBR_BULK_RECOVER_RSYNC_CMD(
        rsbr_bulk_recover_calculate_remaining_rsync, rrh, remote_path,
        local_path);

    if (!rc)
    {
        if (rrh->rrh_remaining < 0 || rrh->rrh_chkpt_size < 0)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "Unable to determine size requirements.");
            return -ENODATA;
        }
        else if (rrh->rrh_remaining > available_cap)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "Remaining=%zd > available-capacity=%zd",
                           rrh->rrh_remaining, available_cap);
            return -ENOSPC;
        }
    }
    return rc;
}

static int
rsbr_bulk_recovery_remove_current_db_contents(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    // Check the status of the last checkpoint, if 'ok', remove db/ contents
    if (!(ri->ri_last_chkpt_err == 0 ||
          ri->ri_last_chkpt_err == -ENODATA ||
          ri->ri_last_chkpt_err == -EALREADY))
    {
        SIMPLE_LOG_MSG(LL_WARN,
                       "Removing 'db/' with last_chkpt_err:  %s",
                       strerror(-ri->ri_last_chkpt_err));
    }

    // Move the contents to the trash and empty
    int rc = rsbr_move_item_to_trash(ri, ribSubDirs[RIR_SUBDIR_DB]);
    if (rc && rc != -ENOENT)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_move_item_to_trash(`%s'): %s",
                       ribSubDirs[RIR_SUBDIR_DB], strerror(-rc));
        return rc;
    }

    rc = rsbr_remove_trash(ri);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_remove_trash(): %s", strerror(-rc));

    return rc;
}

static int
rsbr_bulk_recovery_import_remote_db(struct raft_instance *ri,
                                    struct raft_recovery_handle *rrh)
{
    if (!ri || !rrh || ri->ri_incomplete_recovery ||
        rrh->rrh_from_recovery_marker || rrh->rrh_peer_chkpt_idx < 0 ||
        !uuid_compare(rrh->rrh_peer_uuid, ri->ri_csn_this_peer->csn_uuid))
        return -EINVAL;

    // Obtain the available size on the DB fs
    const ssize_t available_cap = rsbr_bulk_recover_get_fs_free_space(ri);
    if (available_cap < 0)
        return (int)available_cap;

    char remote_path[PATH_MAX] = {0};
    int rc = rsbr_bulk_recover_build_remote_path(rrh, remote_path, PATH_MAX);
    if (rc)
        return rc;

    char local_path[PATH_MAX] = {0};
    rc = rsbr_recovery_rsync_path_build(ri->ri_log, rrh->rrh_peer_uuid,
                                 rrh->rrh_peer_db_uuid, local_path, PATH_MAX);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_recovery_rsync_path_build(): %s",
                       strerror(-rc));
        return rc;
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "rem=%s local=%s", remote_path, local_path);

    // Perform a dry-run rsync to determine the amount of data to be xfer'd
    rc = rsbr_bulk_recover_calculate_remaining(rrh, remote_path, local_path,
                                               available_cap);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "rsbr_bulk_recover_calculate_remaining(pre): %s",
                       strerror(-rc));
        return rc;
    }

    // Execute the actual rsync
    rc = rsbr_bulk_recover_xfer(rrh, remote_path, local_path);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_bulk_recover_xfer(): %s",
                       strerror(-rc));
        return rc;
    }

    // Run another dry-run to ensure that everything is in place.
    rrh->rrh_remaining = -1;
    rc = rsbr_bulk_recover_calculate_remaining(rrh, remote_path, local_path,
                                               available_cap);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "rsbr_bulk_recover_calculate_remaining(post): %s",
                       strerror(-rc));
        return rc;
    }
    else if (rrh->rrh_remaining != 0) // Should prove the xfer is complete
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "rrh_remaining(%zd) != 0 after rsbr_bulk_recover_calculate_remaining(post)",
                       rrh->rrh_remaining);

        return -EBADE;
    }

    return 0;
}

static int
rsbr_bulk_recovery_db_scrub_entry_headers(
    struct raft_instance *ri,
    const struct raft_recovery_handle *rrh)
{
    NIOVA_ASSERT(ri && rrh && raft_instance_is_recovering(ri));
    NIOVA_ASSERT(uuid_compare(ri->ri_csn_this_peer->csn_uuid,
                              rrh->rrh_peer_uuid));

    raft_entry_idx_t start = ri->ri_lowest_idx;
    raft_entry_idx_t end = ri->ri_entries_detected_at_startup;

    NIOVA_ASSERT(start >= 0 && end >= 0 && start <= end);
    struct raft_entry_header reh;

    for (raft_entry_idx_t i = start; i < end; i++)
    {
        reh.reh_index = i; // reh_index is an input parameter
        int rc = rsbr_entry_header_read(ri, &reh);
        if (rc || reh.reh_magic != RAFT_ENTRY_MAGIC || reh.reh_index != i)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "");
            return rc;
        }

        // Modify only this item:
        uuid_copy(reh.reh_self_uuid, ri->ri_csn_this_peer->csn_uuid);
        rsbr_entry_header_write_recovery_scrub(ri, &reh);

        DBG_RAFT_ENTRY(LL_DEBUG, &reh, "");
    }

    return 0;
}

/**
 * rsbr_bulk_recovery_db_scrub - this function opens the recovered db and
 *    replaces the db-uuid, log-header, and raft entry headers from the old
 *    with the uuid's from this instance.  The challenge here is that this
 *    process must be idempotent from the perspective of the caller so that
 *    incomplete scrubs may be resumed on the next restart.  One could take
 *    the approach of staging all modifications into a rocksdb writebatch but
 *    that may require large amounts of memory depending on the raft entry
 *    log size.
 */
static int
rsbr_bulk_recovery_db_scrub(struct raft_instance *ri,
                            const struct raft_recovery_handle *rrh)
{
    NIOVA_ASSERT(ri && rrh && raft_instance_is_recovering(ri));

    DECLARE_AND_INIT_UUID_STR(ri_db_uuid_str, ri->ri_db_uuid);
    DECLARE_AND_INIT_UUID_STR(ri_db_recover_uuid_str, ri->ri_db_recovery_uuid);
    DECLARE_AND_INIT_UUID_STR(rrh_db_uuid_str, rrh->rrh_peer_db_uuid);
    DECLARE_AND_INIT_UUID_STR(rrh_peer_uuid_str, rrh->rrh_peer_uuid);

    // The rrh db uuid must match the one from the currently opened db
    if (uuid_compare(ri->ri_db_uuid, rrh->rrh_peer_db_uuid) &&
        uuid_compare(ri->ri_db_recovery_uuid, rrh->rrh_peer_db_uuid))
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "expected db-uuid=%s or recovery-uuid=%s found db=%s",
                       rrh_db_uuid_str, ri_db_recover_uuid_str,
                       ri_db_uuid_str);

        return -ENODEV;
    }

    // Stash our uuid string temporarily for a call to rsbr_header_load()
    const char *tmp_this_peer_uuid_str = ri->ri_this_peer_uuid_str;
    ri->ri_this_peer_uuid_str = rrh_peer_uuid_str;

    int rc = rsbr_header_load(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_header_load(): %s", strerror(-rc));
        return rc;
    }

    rc = rsbr_bulk_recovery_db_scrub_entry_headers(ri, rrh);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR,
                       "rsbr_bulk_recovery_db_scrub_entry_headers(): %s",
                       strerror(-rc));
        return rc;
    }

    /* Restore the original uuid string and re-init the header.  Note that
     * rsbr_init_header() will not clear the current log header contents
     * (read in above) due to the presence of RAFT_PROC_STATE_RECOVERING.
     * rsbr_init_header() regenerates the db-uuid.  This step comes after the
     * entry headers have been replaced.
     */
    ri->ri_this_peer_uuid_str = tmp_this_peer_uuid_str;
    rc = rsbr_init_header(ri); // must be a synchronous write
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_init_header(): %s", strerror(-rc));
        return rc;
    }

    return 0;
}

static int
rsbr_bulk_recovery_stage_remote_db(struct raft_instance *ri,
                                   const struct raft_recovery_handle *rrh)
{
    NIOVA_ASSERT(ri && rrh);

    char rsync_path[PATH_MAX + 1] = {0};
    int rc = rsbr_recovery_rsync_path_build(ri->ri_log, rrh->rrh_peer_uuid,
                                            rrh->rrh_peer_db_uuid, rsync_path,
                                            PATH_MAX);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_recovery_rsync_path_build(): %s",
                       strerror(-rc));
        return rc;
    }

    char stage_path[PATH_MAX + 1] = {0};
    rc = rsbr_recovery_inprogress_path_build(ri->ri_log, rrh, stage_path,
                                             PATH_MAX);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_recovery_inprogress_path_build(): %s",
                       strerror(-rc));
        return rc;
    }

    rc = rename(rsync_path, stage_path);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "rename(`%s' -> `%s'): %s",
                       rsync_path, stage_path, strerror(-rc));
        return rc;
    }

    return 0;
}

static int
rsbr_make_db_pathname(const struct raft_instance *ri, char *path, size_t len)
{
    if (!ri || !path || !len)
        return -EINVAL;

    int rc;
    if (ri->ri_proc_state == RAFT_PROC_STATE_RECOVERING)
    {
        rc = rsbr_recovery_inprogress_path_build(ri->ri_log,
                                                 &ri->ri_recovery_handle,
                                                 path, len);
    }
    else
    {
        rc = snprintf(path, len, "%s/%s", ri->ri_log,
                      ribSubDirs[RIR_SUBDIR_DB]);
        rc = (rc < 0 ? rc :
              rc >= PATH_MAX ? -ENAMETOOLONG : 0);
    }

    return rc;
}

static int
rsbr_bulk_recovery_promote_scrubbed_db(struct raft_instance *ri,
                                       const struct raft_recovery_handle *rrh)
{
    char stage_path[PATH_MAX + 1] = {0};
    int rc = rsbr_recovery_inprogress_path_build(ri->ri_log, rrh, stage_path,
                                                 PATH_MAX);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_recovery_inprogress_path_build(): %s",
                       strerror(-rc));
        return rc;
    }

    // rsbr_make_db_pathname() determines pathname based on proc-state
    if (!raft_instance_is_shutdown(ri))
        ri->ri_proc_state = RAFT_PROC_STATE_SHUTDOWN;

    char db_path[PATH_MAX + 1] = {0};
    rc = rsbr_make_db_pathname(ri, db_path, PATH_MAX);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_make_db_pathname(): %s", strerror(-rc));
        return rc;
    }

    rc = rename(stage_path, db_path);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "rename(`%s' -> `%s'): %s",
                       stage_path, db_path, strerror(-rc));
        return rc;
    }

    return 0;

}

static int
rsbr_destroy(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    else if (!ri->ri_backend_arg)
        return -EALREADY;

    struct raft_instance_rocks_db *rir = rsbr_ri_to_rirdb(ri);

    if (rir->rir_log_fd < 0)
    {
        int rc = close(rir->rir_log_fd);
        if (rc)
            SIMPLE_LOG_MSG(LL_WARN, "close(rir_log_fd): %s",
                           strerror(-errno));
        else
            rir->rir_log_fd = -1;
    }

    if (rir->rir_db)
    {
        rocksdb_close(rir->rir_db);

        // User must call raft_server_rocksdb_release_cf_table()
        if (rir->rir_cf_table) // Handles seem to be freed in rocksdb_close()
        {
            struct raft_server_rocksdb_cf_table *cft = rir->rir_cf_table;

            for (size_t i = 0; i < cft->rsrcfe_num_cf; i++)
                if (cft->rsrcfe_cf_handles[i])
                    cft->rsrcfe_cf_handles[i] = NULL;
        }
    }

    if (rir->rir_writeoptions_sync)
        rocksdb_writeoptions_destroy(rir->rir_writeoptions_sync);

    if (rir->rir_writeoptions_async)
        rocksdb_writeoptions_destroy(rir->rir_writeoptions_async);

    if (rir->rir_readoptions)
        rocksdb_readoptions_destroy(rir->rir_readoptions);

    if (rir->rir_options)
        rocksdb_options_destroy(rir->rir_options);

    if (rir->rir_writebatch)
        rocksdb_writebatch_destroy(rir->rir_writebatch);

    ri->ri_backend_arg = NULL;

    return 0;
}

static int
rsbr_setup_create_subdirs(struct raft_instance_rocks_db *rir)
{
    if (!rir || rir->rir_log_fd < 0)
        return -EINVAL;

    for (enum raft_instance_rocks_db_subdirs i = RIR_SUBDIR__MIN;
         i < RIR_SUBDIR__MAX; i++)
    {
        int rc = mkdirat(rir->rir_log_fd, ribSubDirs[i], 0700);
        if (rc)
        {
            rc = -errno;

            if (rc == -EEXIST)
            {
                struct stat stb;
                rc = fstatat(rir->rir_log_fd, ribSubDirs[i], &stb,
                             AT_SYMLINK_NOFOLLOW);
                if (rc)
                {
                    rc = -errno;
                    SIMPLE_LOG_MSG(LL_ERROR, "fstatat(%s): %s",
                                   ribSubDirs[i], strerror(-rc));
                    return rc;
                }
                else if (!S_ISDIR(stb.st_mode))
                {
                    SIMPLE_LOG_MSG(LL_ERROR, "Path %s: %s",
                                   ribSubDirs[i], strerror(ENOTDIR));
                    return -ENOTDIR;
                }
            }
            else
            {
                SIMPLE_LOG_MSG(LL_ERROR, "mkdirat(%s): %s",
                               ribSubDirs[i], strerror(-rc));
                return rc;
            }
        }
    }
    return 0;
}

static int
rsbr_subdirs_setup(struct raft_instance *ri)
{
    if (!ri || !ri->ri_backend_arg)
        return -EINVAL;

    struct raft_instance_rocks_db *rir = ri->ri_backend_arg;

    int rc = file_util_pathname_build(ri->ri_log);
    if (rc)
    {
         SIMPLE_LOG_MSG(LL_ERROR, "file_util_pathname_build(%s): %s",
                        ri->ri_log, strerror(-rc));
         return rc;
    }

    rir->rir_log_fd = rsbr_log_dir_open_fd(ri);

    if (rir->rir_log_fd < 0)
    {
        int rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "open(%s): %s", ri->ri_log, strerror(-rc));

        return rc;
    }

    // Directories are not created in recovery mode.
    rc = (ri->ri_proc_state != RAFT_PROC_STATE_RECOVERING) ?
        rsbr_setup_create_subdirs(rir) : 0;

    return rc;
}

static int
rsbr_setup_detect_recovery(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = rsbr_recovery_marker_scan(ri);

    if (!rc && ri->ri_incomplete_recovery)
    {
        const struct raft_recovery_handle *rrh =
            raft_instance_2_recovery_handle(ri);

        if (!rrh->rrh_from_recovery_marker ||
            uuid_is_null(rrh->rrh_peer_uuid) ||
            uuid_is_null(rrh->rrh_peer_db_uuid))
        {
            SIMPLE_LOG_MSG(LL_ERROR, "invalid incomplete recovery state");
            return -ENXIO;
        }

        return -EUCLEAN; // Special 'rc', caller will try to resume recovery
    }

    NIOVA_ASSERT(rc != -EUCLEAN); // Reserved rc cannot be used here

    return rc;
}

static int
rsbr_recovery_regex_setup(void)
{
    for (int i = 0; i < RECOVERY__num_regex; i++)
    {
        int rc = regcomp(&recoveryRegexes[i].rp_regex,
                         recoveryRegexes[i].rp_regex_str, 0);
        if (rc)
        {
            char regerr_str[63] = {0};
            regerror(rc, &recoveryRegexes[i].rp_regex, regerr_str, 63);
            SIMPLE_LOG_MSG(LL_ERROR, "regcomp(idx=%d): %s", i, regerr_str);
            return -EINVAL;
        }
    }

    return 0;
}

static void
rsbr_recovery_regex_release(void)
{
    for (int i = 0; i < RECOVERY__num_regex; i++)
        regfree(&recoveryRegexes[i].rp_regex);
}

/**
 * rsbr_setup_rir_rockdsdb_items - configure and allocate rocksdb related
 *    options and handle.
 * NOTE:  caller is responsible for issuing rsbr_destroy() on failure.
 */
static int
rsbr_setup_rir_rockdsdb_items(struct raft_instance_rocks_db *rir)
{
    rir->rir_options = rocksdb_options_create();
    if (!rir->rir_options)
        return -ENOMEM;

    rocksdb_options_set_create_if_missing(rir->rir_options, 0);
    rocksdb_options_set_create_missing_column_families(rir->rir_options, 1);

    // These are options for future consideration
//     const long int cpus = sysconf(_SC_NPROCESSORS_ONLN);
//    rocksdb_options_increase_parallelism(rir->rir_options, (int)(cpus));
//    rocksdb_options_set_use_direct_reads(rir->rir_options, 1);
//    rocksdb_options_set_use_direct_io_for_flush_and_compaction(
//        rir->rir_options, 1);

    /* The documentation around this option is a bit confusing.  At this time,
     * I don't think the option is needed for pumiceDB (which uses multiple
     * CFs) since there's no explicit flushing of WALs or disabling of WALs for
     * specific CF operations.
     */
    /* See https://github.com/facebook/rocksdb/wiki/Atomic-flush
     * Users of this backend are expected to use column families.
     */
//    rocksdb_options_set_atomic_flush(rir->rir_options, 1);


    rir->rir_writeoptions_sync = rocksdb_writeoptions_create();
    if ( rir->rir_writeoptions_sync)
        rocksdb_writeoptions_set_sync(rir->rir_writeoptions_sync, 1);
    else
        return -ENOMEM;

    // Make a non-sync option as well.
    rir->rir_writeoptions_async = rocksdb_writeoptions_create();
    if (!rir->rir_writeoptions_async)
        return -ENOMEM;

    rir->rir_readoptions = rocksdb_readoptions_create();
    if (!rir->rir_readoptions)
        return -ENOMEM;

    rir->rir_writebatch = rocksdb_writebatch_create();
    if (!rir->rir_writebatch)
        return -ENOMEM;

    return 0;
}

/**
 * rsbr_setup_rir - allocate, initialize and configure the
 *    raft_instance_rocks_db structure and members.
 * NOTE:  caller is responsible for issuing rsbr_destroy() on failure.
 */
static int
rsbr_setup_rir(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    else if (ri->ri_backend_arg)
        return -EALREADY;

    ri->ri_backend_arg =
        niova_calloc(1UL, sizeof(struct raft_instance_rocks_db));

    if (!ri->ri_backend_arg)
        return -ENOMEM;

    struct raft_instance_rocks_db *rir = ri->ri_backend_arg;
    rir->rir_log_fd = -1;

    /* The user may have passed in a list of column family names which are to
     * be opened.  These must be specified at db-open() time.
     */
    if (ri->ri_backend_init_arg)
        rir->rir_cf_table =
            (struct raft_server_rocksdb_cf_table *)ri->ri_backend_init_arg;

    int rc = rsbr_subdirs_setup(ri);
    if (rc)
         return rc;

    return rsbr_setup_rir_rockdsdb_items(rir);
}

static int
rsbr_db_open_internal(const struct raft_instance *ri,
                      struct raft_instance_rocks_db *rir, bool create_db)
{
    NIOVA_ASSERT(ri && rir && rir->rir_options);
    NIOVA_ASSERT(ri->ri_proc_state == RAFT_PROC_STATE_BOOTING ||
                 ri->ri_proc_state == RAFT_PROC_STATE_RECOVERING);

    if (create_db)
        rocksdb_options_set_create_if_missing(rir->rir_options, 1);

    char rocksdb_dir[PATH_MAX] = {0};
    int rc = rsbr_make_db_pathname(ri, rocksdb_dir, PATH_MAX);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_setup_db_make_pathname(): %s",
                       strerror(-rc));
        return rc;
    }

    struct raft_server_rocksdb_cf_table *cft = rir->rir_cf_table;

    // Prepare cf array
    const rocksdb_options_t *cft_opts[RAFT_ROCKSDB_MAX_CF];
    if (cft && cft->rsrcfe_num_cf)
    {
        NIOVA_ASSERT(cft->rsrcfe_num_cf <= RAFT_ROCKSDB_MAX_CF);
        for (size_t i = 0; i < cft->rsrcfe_num_cf; i++)
            cft_opts[i] = rir->rir_options;
    }
    // Set prefix extractor for range queries
    rocksdb_options_set_prefix_extractor(rir->rir_options, NULL);
    char *err = NULL;
    rir->rir_db = (cft && cft->rsrcfe_num_cf) ?
        rocksdb_open_column_families(rir->rir_options, rocksdb_dir,
                                     cft->rsrcfe_num_cf, cft->rsrcfe_cf_names,
                                     cft_opts, cft->rsrcfe_cf_handles, &err) :
        rocksdb_open(rir->rir_options, rocksdb_dir, &err);

    rc = (!rir->rir_db || err) ? -ENOENT : 0; // enoent is merely a guess

    SIMPLE_LOG_MSG((rc ? LL_ERROR : LL_WARN), "%s(`%s'): %s (try-create=%s)",
                   (cft && cft->rsrcfe_num_cf) ?
                   "rocksdb_open_column_families" : "rocksdb_open",
                   rocksdb_dir, err ? err : "Success",
                   create_db ? "yes" : "no");

    return rc;
}

/**
 * rsbr_prep_raft_instance_from_db - this function takes the recently opened
 *    rocksdb and scans for "metadata" K/V pairs and checkpoints to prepare
 *    the raft instance.
 */
static int
rsbr_prep_raft_instance_from_db(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    rsb_sm_get_instance_uuid(ri);

    int rc = 0;

    /* Determine the number of entries which this backend instance contains
     * and write that value into the raft_instance structure.
     */
    ri->ri_entries_detected_at_startup = rsbr_num_entries_calc(ri);
    if (ri->ri_entries_detected_at_startup < 0)
    {
        rc = ri->ri_entries_detected_at_startup;
        FATAL_IF(rc, "rsbr_num_entries_calc(): %s", strerror(-rc));
    }

    raft_entry_idx_t lowest_idx = -1;
    if (ri->ri_entries_detected_at_startup > 0)
    {
        rc = rsbr_lowest_entry_get(ri, &lowest_idx);
        FATAL_IF(rc, "rsbr_lowest_entry_get(): %s", strerror(-rc));
    }
    niova_atomic_init(&ri->ri_lowest_idx, lowest_idx);

    /* Applications which store their application data in RocksDB may
     * bypass the entries which have already been applied.
     */
    if (ri->ri_store_type == RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP)
        rsb_sm_get_last_applied_kv_idx(ri);

    SIMPLE_LOG_MSG(LL_WARN, "entry-idxs: lowest=%ld highest=%ld",
                   lowest_idx, ri->ri_entries_detected_at_startup - 1);

    // Scan and possibly clean the checkpoint directories, skip if recovering
    if (ri->ri_proc_state == RAFT_PROC_STATE_BOOTING)
    {
        rc = rsbr_startup_checkpoint_scan(ri);
        FATAL_IF(rc, "rsbr_startup_checkpoint_scan(): %s", strerror(-rc));
    }
    return 0;
}

static int
rsbr_db_open(struct raft_instance *ri, struct raft_instance_rocks_db *rir)
{
    NIOVA_ASSERT(ri && rir);
    NIOVA_ASSERT(ri->ri_proc_state == RAFT_PROC_STATE_BOOTING ||
                 ri->ri_proc_state == RAFT_PROC_STATE_RECOVERING);

    const bool recovering = ri->ri_proc_state == RAFT_PROC_STATE_RECOVERING ?
        true : false;

    int rc = rsbr_db_open_internal(ri, rir, false);

    if (rc && !recovering) // Try to 'create' the db if the first open failed
    {
        rc = rsbr_db_open_internal(ri, rir, true);
        if (!rc)
        {
            rc = rsbr_init_header(ri);
            if (rc)
                SIMPLE_LOG_MSG(LL_ERROR, "rsbr_init_header(): %s",
                               strerror(-rc));
        }
    }

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_db_open_internal(): %s", strerror(-rc));
        return -ENOTCONN;
    }

    // Called in both the booting and recovery contexts
    return rsbr_prep_raft_instance_from_db(ri);
}

static int
rsbr_bulk_recover(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    // The upper level caller
    NIOVA_ASSERT(ri->ri_proc_state == RAFT_PROC_STATE_RECOVERING);

    struct raft_recovery_handle *rrh = raft_instance_2_recovery_handle(ri);
    if (!rrh)
        return -ENOENT;

    // Initialize the values set by this function
    rrh->rrh_remaining = -1;
    rrh->rrh_chkpt_size = -1;

    if (uuid_is_null(rrh->rrh_peer_uuid) ||
        uuid_is_null(rrh->rrh_peer_db_uuid))
    {
        SIMPLE_LOG_MSG(LL_ERROR, "null peer or db-uuid");
        return -EINVAL;
    }

    int rc = 0;
    if (!rrh->rrh_from_recovery_marker)
    {
        // network tranfser of remote db to chkpt/peers/
        rc = rsbr_bulk_recovery_import_remote_db(ri, rrh);
        if (rc)
            return rc;

        /* Move dir from chkpt/peers/ to top level - this creates a "recovery
         * marker" from which the bulk recovery may be resumed should a crash
         * occur.
         */
        rc = rsbr_bulk_recovery_stage_remote_db(ri, rrh);
        if (rc)
            return rc;

    }

    // remove the db/ directory
    rc = rsbr_bulk_recovery_remove_current_db_contents(ri);
    if (rc)
        return rc;

    // allocate and configure the rocksdb handle and accompanying structures
    rc = rsbr_setup_rir(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_setup_rir(): %s", strerror(-rc));
        goto out;
    }

    // Open the rocksdb and obtain system KVs
    rc = rsbr_db_open(ri, (struct raft_instance_rocks_db *)ri->ri_backend_arg);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_db_open(): %s", strerror(-rc));
        goto out;
    }

    /* Replace raft entry headers and the log header KVs with versions
     * containing this peer's UUID.
     */
    rc = rsbr_bulk_recovery_db_scrub(ri, rrh);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsbr_bulk_recovery_db_scrub(): %s",
                       strerror(-rc));
        goto out;
    }

out:
    // rsbr_destroy() regardless of error
    rsbr_destroy(ri);

    if (!rc) // Rename the "in progress" db dir after closing the rocksdb
        rc = rsbr_bulk_recovery_promote_scrubbed_db(ri, rrh);

    return rc;
}

static int
rsbr_setup(struct raft_instance *ri)
{
    if (!ri || ri->ri_backend != &ribRocksDB)
        return -EINVAL;

    /* Set the max-entry-size supported by this backend.  If larger values are
     * needed, this size may increased during initialization through a command
     * line argument or a ctl-interface init cmd.  Modifying during runtime is
     * possible as well but it will require the resizing of buffers owned by
     * raft_server.c.
     */
    CONST_OVERRIDE(size_t, ri->ri_max_entry_size, RAFT_ENTRY_SIZE_ROCKSDB);

    int rc = rsbr_setup_rir(ri);
    if (rc)
        goto out;

    // Remove trash items before starting up
    rsbr_remove_trash(ri);

    /* Check for an existing recovery process.  If present return an error
     * here so that caller may choose to complete the recovery process.
     */
    rc = rsbr_setup_detect_recovery(ri);
    if (rc)
        goto out;

    struct raft_instance_rocks_db *rir = ri->ri_backend_arg;
    rc = rsbr_db_open(ri, rir);

out:
    if (rc)
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

/**
 * raft_server_get_rocksdb_instance - public function used by pumiceDB to
 *    obtain the rocksDB pointer from the raft_instance.
 */
rocksdb_t *
raft_server_get_rocksdb_instance(struct raft_instance *ri)
{
    if (ri &&
        (ri->ri_store_type == RAFT_INSTANCE_STORE_ROCKSDB ||
         ri->ri_store_type == RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP) &&
        ri->ri_backend && ri->ri_backend_arg)
        return rsbr_ri_to_rocksdb(ri);

    return NULL;
}

void
raft_server_rocksdb_release_cf_table(struct raft_server_rocksdb_cf_table *cft)
{
    if (!cft)
        return;

    for (size_t i = 0; i < cft->rsrcfe_num_cf; i++)
    {
        if (cft->rsrcfe_cf_names[i])
        {
            free((char *)cft->rsrcfe_cf_names[i]);
            cft->rsrcfe_cf_names[i] = NULL;
        }
        if (cft->rsrcfe_cf_handles[i])
        {
            rocksdb_column_family_handle_destroy(cft->rsrcfe_cf_handles[i]);
            cft->rsrcfe_cf_handles[i] = NULL;
        }
    }

    cft->rsrcfe_num_cf = 0;
}

int
raft_server_rocksdb_add_cf_name(struct raft_server_rocksdb_cf_table *cft,
                                const char *cf_name, const size_t cf_name_len)
{
    if (!cft || !cf_name || !cf_name_len ||
        cf_name_len > RAFT_ROCKSDB_MAX_CF_NAME_LEN)
        return -EINVAL;

    if (!cft->rsrcfe_num_cf) // First, add the 'default' CF
    {
        cft->rsrcfe_cf_names[0] = strndup("default", 7);
        if (!cft->rsrcfe_cf_names[0])
            return -ENOMEM;

        cft->rsrcfe_num_cf = 1;
    }

    if (cft->rsrcfe_num_cf >= RAFT_ROCKSDB_MAX_CF)
        return -ENOSPC;

    for (size_t i = 1; i < cft->rsrcfe_num_cf; i++)
        if (!strncmp(cf_name, cft->rsrcfe_cf_names[i], RAFT_ROCKSDB_MAX_CF))
            return -EALREADY;

    cft->rsrcfe_cf_names[cft->rsrcfe_num_cf] = strndup(cf_name, cf_name_len);
    if (!cft->rsrcfe_cf_names[cft->rsrcfe_num_cf])
        return -ENOMEM;

    cft->rsrcfe_num_cf++;

    return 0;
}

static init_ctx_t NIOVA_CONSTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
rsbr_subsys_init(void)
{
    int rc = rsbr_recovery_regex_setup();
    NIOVA_ASSERT(!rc);
}

static init_ctx_t NIOVA_DESTRUCTOR(RAFT_SYS_CTOR_PRIORITY)
rsbr_subsys_destroy(void)
{
    rsbr_recovery_regex_release();
}

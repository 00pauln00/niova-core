/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/timerfd.h>
#include <linux/limits.h>

#include "alloc.h"
#include "crc32.h"
#include "ctl_svc.h"
#include "io.h"
#include "log.h"
#include "raft.h"
#include "raft_net.h"
#include "random.h"
#include "registry.h"
#include "util_thread.h"

REGISTRY_ENTRY_FILE_GENERATE;

static int
raft_server_log_file_setup_init_header_posix(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    uuid_t null_uuid;
    uuid_clear(null_uuid);

    for (int i = 0; i < raft_server_instance_get_num_log_headers(ri); i++)
    {
        int rc = raft_server_log_header_write_posix(ri, null_uuid, 0);
        if (rc)
            return rc;
    }

    return 0;
}


static void
rsbp_entry_write(struct raft_instance *ri, const size_t phys_idx,
                 const struct raft_entry *re, const size_t total_entry_size)
{
    const ssize_t write_sz =
        io_pwrite(ri->ri_log_fd, (const char *)re, total_entry_size,
                  (phys_idx * RAFT_ENTRY_SIZE));

    NIOVA_ASSERT(write_sz == total_entry_size);

    //Xxxx pwritev2 can do a sync write with a single syscall.
    int rc = io_fsync(ri->ri_log_fd);
    NIOVA_ASSERT(!rc);
}

static ssize_t
rsbp_entry_read(struct raft_instance *ri, struct raft_entry *re,
                const size_t phys_entry_idx, const size_t total_entry_size)
{
    return io_pread(ri->ri_log_fd, (char *)re, total_entry_size,
                    (phys_entry_idx * RAFT_ENTRY_SIZE));
}

static void //raft_server_udp_cb_ctx_int_t
rsbp_log_truncate(struct raft_instance *ri, const int64_t trunc_phys_entry_idx)
{
    NIOVA_ASSERT(ri);

    int rc = io_ftruncate(ri->ri_log_fd,
                          (trunc_phys_entry_idx * RAFT_ENTRY_SIZE));
    FATAL_IF((rc), "io_ftruncate(): %s", strerror(-rc));

    rc = io_fsync(ri->ri_log_fd);
    FATAL_IF((rc), "io_fsync(): %s", strerror(-rc));
}

static int
rsbp_header_load(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    const size_t num_headers = raft_server_instance_get_num_log_headers(ri);
    NIOVA_ASSERT(num_headers > 0);

    struct raft_log_header rlh[num_headers];
    struct raft_log_header *most_recent_rlh = NULL;

    for (size_t i = 0; i < num_headers; i++)
    {
        memset(&rlh[i], 0, sizeof(struct raft_log_header));

        size_t rc_len = 0;
        char *buf = (char *)((struct raft_log_header *)&rlh[i]);

        int rc = rsbp_entry_read(ri, i, buf, sizeof(struct raft_log_header),
                                 &rc_len);

        if (!rc && rc_len == sizeof(struct raft_log_header))
        {
            if (!most_recent_rlh ||
                rlh[i].rlh_seqno > most_recent_rlh->rlh_seqno)
                most_recent_rlh = &rlh[i];
        }
    }

    if (!most_recent_rlh) // No valid header entries were found
        return -EBADMSG;

    ri->ri_log_hdr = *most_recent_rlh;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");

    return 0;
}

/**
 * raft_server_log_header_write_posix - store the current raft state into the
 *     log header. This function is typically called while casting a vote for a
 *     candidate.
 * @ri:  this raft instance
 * @candidate:  UUID of the candidate being voted for.  May be NULL if the
 *     header is initialized.
 * @candidate_term:  term presented by the candidate
 */
static int
rsbp_header_write(struct raft_instance *ri, const uuid_t candidate,
                  const int64_t candidate_term)
{
    if (!ri)
        return -EINVAL;

    raft_server_log_header_write_prep(ri, candidate, candidate_term);

    const size_t block_num = ri->ri_log_hdr.rlh_seqno %
        MAX(1, raft_server_instance_get_num_log_headers(ri));

    return raft_server_entry_write(ri, block_num, ri->ri_log_hdr.rlh_term,
                                   (const char *)&ri->ri_log_hdr,
                                   sizeof(struct raft_log_header),
                                   RAFT_WR_ENTRY_OPT_NONE);
}

static int
raft_server_stat_log_fd(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = fstat(ri->ri_log_fd, &ri->ri_log_stb);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "fstat(): %s", strerror(-rc));
    }

    return rc;
}

/**
 * raft_server_log_file_setup - open the log file and initialize it if it's
 *    newly created.
 */
static int
raft_server_log_file_setup_posix(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = 0;
    ri->ri_log_fd = open(ri->ri_log, O_CREAT | O_RDWR | O_SYNC, 0600);
    if (ri->ri_log_fd < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "open(`%s'): %s", ri->ri_log, strerror(-rc));
        return rc;
    }

    rc = raft_server_stat_log_fd(ri);
    if (rc)
        return rc;

    /* Initialize the log header if the file was just created.
     */
    if (!ri->ri_log_stb.st_size)
    {
        rc = raft_server_log_file_setup_init_header(ri);
        if (rc)
            SIMPLE_LOG_MSG(LL_ERROR,
                           "raft_server_log_file_setup_init_header(): %s",
                           strerror(-rc));
    }

    return rc;
}

static ssize_t
raft_server_num_entries_calc_posix(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    int rc = raft_server_stat_log_fd(ri);
    if (rc)
        return rc;

    /* Calculate the number of entries based on the size of the log file
     * deducting the number of log header blocks.
     */
    ssize_t num_entries =
        MAX(0, ((ri->ri_log_stb.st_size / RAFT_ENTRY_SIZE) +
                ((ri->ri_log_stb.st_size % RAFT_ENTRY_SIZE) ? 1 : 0)
                - raft_server_instance_get_num_log_headers(ri)));

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "num-block-entries=%zd", num_entries);

    return num_entries;
}

// Part of rib_backend_shutdown
static int
raft_server_log_file_close(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    else if (ri->ri_log_fd < 0)
        return 0;

    int rc = close(ri->ri_log_fd);
    ri->ri_log_fd = -1;

    return rc;
};

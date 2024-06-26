/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "alloc.h"
#include "common.h"
#include "io.h"
#include "log.h"
#include "raft.h"
#include "registry.h"

#define RAFT_ENTRY_SIZE_POSIX 65536
#define NUM_RAFT_LOG_HEADERS 2

REGISTRY_ENTRY_FILE_GENERATE;

struct raft_instance_posix
{
    struct stat rip_stb;
    int         rip_fd;
};

static void
rsbp_entry_write(struct raft_instance *, const struct raft_entry *,
                 const struct raft_net_sm_write_supplements *);

static ssize_t
rsbp_entry_read(struct raft_instance *, struct raft_entry *);

static int
rsbp_entry_header_read(struct raft_instance *, struct raft_entry_header *);

static int
rsbp_header_write(struct raft_instance *);

static void
rsbp_log_truncate(struct raft_instance *, const raft_entry_idx_t);

static int
rsbp_header_load(struct raft_instance *);

static int
rsbp_setup(struct raft_instance *);

static int
rsbp_destroy(struct raft_instance *);

static int
rsbp_sync(struct raft_instance *);

static struct raft_instance_backend ribPosix = {
    .rib_entry_write       = rsbp_entry_write,
    .rib_entry_read        = rsbp_entry_read,
    .rib_entry_header_read = rsbp_entry_header_read,
    .rib_log_truncate      = rsbp_log_truncate,
    .rib_header_write      = rsbp_header_write,
    .rib_header_load       = rsbp_header_load,
    .rib_backend_setup     = rsbp_setup,
    .rib_backend_shutdown  = rsbp_destroy,
    .rib_backend_sync      = rsbp_sync,
};

static inline struct raft_instance_posix *
rsbp_ri_to_rip(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_backend == &ribPosix && ri->ri_backend_arg);

    struct raft_instance_posix *rip =
        (struct raft_instance_posix *)ri->ri_backend_arg;

    return rip;
}

static inline size_t
rsbp_ri_to_log_sz(struct raft_instance *ri)
{
    return rsbp_ri_to_rip(ri)->rip_stb.st_size;
}

static inline raft_entry_idx_t
rsbr_get_num_log_headers(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri->ri_log_hdr.rlh_version == 0 &&
                 ri->ri_store_type == RAFT_INSTANCE_STORE_POSIX_FLAT_FILE);

    return NUM_RAFT_LOG_HEADERS;
}

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
#endif
static inline bool
rsbr_phys_idx_is_log_header(const struct raft_instance *ri, size_t phys_idx)
{
    return (raft_entry_idx_t)phys_idx < rsbr_get_num_log_headers(ri) ?
        true : false;
}
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

static inline raft_entry_idx_t
rsbr_entry_idx_to_phys_idx(const struct raft_instance *ri,
                           raft_entry_idx_t entry_idx)
{
    NIOVA_ASSERT(ri);

    const raft_entry_idx_t num_log_headers = rsbr_get_num_log_headers(ri);

    entry_idx += num_log_headers;

    NIOVA_ASSERT(entry_idx >= 0);

    return entry_idx;
}

static raft_entry_idx_t
rsbr_raft_entry_header_to_phys_idx(const struct raft_instance *ri,
                                   const struct raft_entry_header *reh)
{
    return rsbr_entry_idx_to_phys_idx(ri, reh->reh_index);
}

static off_t
rsbr_raft_index_to_phys_offset(const struct raft_instance *ri,
                               raft_entry_idx_t entry_idx)
{
    NIOVA_ASSERT(ri && ri->ri_max_entry_size == RAFT_ENTRY_SIZE_POSIX);

    return (off_t)(rsbr_entry_idx_to_phys_idx(ri, entry_idx) *
                   RAFT_ENTRY_SIZE_POSIX);
}

static off_t
rsbr_raft_entry_to_phys_offset(const struct raft_instance *ri,
                               const struct raft_entry *re)
{
    NIOVA_ASSERT(ri && ri->ri_max_entry_size == RAFT_ENTRY_SIZE_POSIX);

    return (off_t)
        (rsbr_raft_entry_header_to_phys_idx(ri, &re->re_header) *
         RAFT_ENTRY_SIZE_POSIX);
}

static off_t
rsbr_raft_entry_header_to_phys_offset(const struct raft_instance *ri,
                                      const struct raft_entry_header *reh)
{
    NIOVA_ASSERT(ri && ri->ri_max_entry_size == RAFT_ENTRY_SIZE_POSIX);

    return (off_t)
        (rsbr_raft_entry_header_to_phys_idx(ri, reh) * RAFT_ENTRY_SIZE_POSIX);
}

static void
rsbp_entry_write(struct raft_instance *ri, const struct raft_entry *re,
                 const struct raft_net_sm_write_supplements *unused)
{
    NIOVA_ASSERT(ri && re);

    (void)unused; // posix-backend does not support wr-supp

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    const size_t expected_size = raft_server_entry_to_total_size(re);
    const off_t offset = rsbr_raft_entry_to_phys_offset(ri, re);

    const ssize_t rrc =
        niova_io_pwrite(rip->rip_fd, (const char *)re, expected_size, offset);

    const bool write_ok = (rrc == (ssize_t)expected_size) ? true : false;

    DBG_RAFT_ENTRY((write_ok ? LL_DEBUG : LL_ERROR), &re->re_header,
                   "niova_io_pwrite() %s (rrc=%zd expected-size=%zu offset=%ld)",
                   rrc < 0 ? strerror(-rrc) : "Success", rrc, expected_size,
                   offset);

    NIOVA_ASSERT(rrc == (ssize_t)expected_size);
}

static ssize_t
rsbp_read_common(struct raft_instance *ri, struct raft_entry *re,
                 struct raft_entry_header *reh)
{
    const bool header_only_read = (reh == NULL) ? false : true;

    if (!ri || (header_only_read && !reh) || (!header_only_read && !re))
        return -EINVAL;

    if (!header_only_read)
        reh = &re->re_header;

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    const ssize_t expected_sz = header_only_read ?
        sizeof(struct raft_entry_header) :
        raft_server_entry_to_total_size(re);

    const raft_entry_idx_t idx = reh->reh_index;

    LOG_MSG(LL_DEBUG, "reh=%p reh-idx=%ld reh-data-size=%u total-sz=%zd",
            reh, reh->reh_index, reh->reh_data_size, expected_sz);

    ssize_t read_sz = niova_io_pread(rip->rip_fd, (char *)reh, expected_sz,
                               rsbr_raft_entry_header_to_phys_offset(ri, reh));

    if (read_sz != expected_sz)
    {
        LOG_MSG(
            LL_ERROR,
            "niova_io_pread(): %s (rrc=%zd != %zd idx=%ld off=%ld hdr-only=%s)",
            read_sz < 0 ? strerror((int)-read_sz) : "Success",
            read_sz, expected_sz, idx,
            rsbr_raft_entry_header_to_phys_offset(ri, reh),
            header_only_read ? "true" : "false");
    }
    else
    {
        DBG_RAFT_ENTRY(LL_DEBUG, reh,
                       "entry-sz=%zd idx=%zd offset=%ld hdr-only=%s",
                       expected_sz, idx,
                       rsbr_raft_entry_header_to_phys_offset(ri, reh),
                       header_only_read ? "true" : "false");
    }

    return read_sz;
}


static ssize_t
rsbp_entry_read(struct raft_instance *ri, struct raft_entry *re)
{
    return rsbp_read_common(ri, re, NULL);
}

static int
rsbp_entry_header_read(struct raft_instance *ri, struct raft_entry_header *reh)
{
    if (!ri || !reh || reh->reh_index < 0)
        return -EINVAL;

    const ssize_t rrc = rsbp_read_common(ri, NULL, reh);

    if (rrc < 0)
        return (int)rrc;

    else if (rrc != sizeof(*reh))
        return -EIO;

    return 0;
}

static void
rsbp_log_truncate(struct raft_instance *ri,
                  const raft_entry_idx_t entry_idx)
{
    NIOVA_ASSERT(ri);

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);
    const off_t trunc_off = rsbr_raft_index_to_phys_offset(ri, entry_idx);

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "trunc-off=%ld entry-idx=%ld",
                      trunc_off, entry_idx);

    int rc = niova_io_ftruncate(rip->rip_fd, trunc_off);
    FATAL_IF((rc), "niova_io_ftruncate(): %s", strerror(-rc));

    rc = rsbp_sync(ri);
    FATAL_IF((rc), "rsbp_sync(): %s", strerror(rc));
}

static int
rsbp_header_load(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    const raft_entry_idx_t num_headers = rsbr_get_num_log_headers(ri);
    NIOVA_ASSERT(num_headers > 0);

    struct raft_log_header most_recent_rlh = {0};

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-variable-sized-type-not-at-end"
#endif
    struct
    {
        struct raft_entry      re;
        struct raft_log_header rlh;
    } entry_and_header;
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

    for (raft_entry_idx_t i = 0; i < num_headers; i++)
    {
        memset(&entry_and_header, 0, sizeof(entry_and_header));

        entry_and_header.re.re_header.reh_index = i - num_headers;
        entry_and_header.re.re_header.reh_data_size =
            sizeof(struct raft_log_header);

        ssize_t rrc = rsbp_entry_read(ri, &entry_and_header.re);
        if (rrc != sizeof(entry_and_header))
        {
            DBG_RAFT_INSTANCE(LL_ERROR, ri,
                              "header@idx-%ld read returns rrc=%zd", i, rrc);
            continue;
        }

        int rc = raft_server_entry_check_crc(&entry_and_header.re);
        if (rc)
        {
            DBG_RAFT_INSTANCE(LL_ERROR, ri,
                              "raft_server_entry_check_crc(): %s (idx-%ld)",
                              strerror(-rc), i);
            continue;
        }

        if (most_recent_rlh.rlh_magic != RAFT_HEADER_MAGIC ||
            entry_and_header.rlh.rlh_seqno > most_recent_rlh.rlh_seqno)
            most_recent_rlh = entry_and_header.rlh;
    }

    if (most_recent_rlh.rlh_magic != RAFT_HEADER_MAGIC)
        return -EBADMSG; // No valid header entries were found

    ri->ri_log_hdr = most_recent_rlh;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");

    return 0;
}

/**
 * rsbp_header_write - store the current raft state into the
 *     log header. This function is typically called while casting a vote for a
 *     candidate.
 * @ri:  this raft instance
 * @candidate:  UUID of the candidate being voted for.  May be NULL if the
 *     header is initialized.
 * @candidate_term:  term presented by the candidate
 */
static int
rsbp_header_write(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    const raft_entry_idx_t phys_block_num = ri->ri_log_hdr.rlh_seqno %
        MAX(1, rsbr_get_num_log_headers(ri));

    // log block entry indexes are negative
    raft_entry_idx_t re_idx = phys_block_num - rsbr_get_num_log_headers(ri);

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-variable-sized-type-not-at-end"
#endif
    struct
    {
        struct raft_entry      re;
        struct raft_log_header rlh;
    } entry_and_header;
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

    struct raft_entry *re = &entry_and_header.re;

    uint32_t len = sizeof(struct raft_log_header);

    raft_server_entry_init_for_log_header(ri, re, re_idx,
                                          ri->ri_log_hdr.rlh_term,
                                          (const char *)&ri->ri_log_hdr,
                                          len);

    const ssize_t write_sz =
        niova_io_pwrite(rip->rip_fd, (const char *)&entry_and_header,
                  raft_server_entry_to_total_size(re),
                  rsbr_raft_entry_to_phys_offset(ri, re));

    int rc = (write_sz == (ssize_t)raft_server_entry_to_total_size(re)) ?
        0 : -EIO;

    int sync_rc = rsbp_sync(ri);

    DBG_RAFT_ENTRY(
        ((rc || sync_rc) ? LL_ERROR : LL_DEBUG), &re->re_header,
        "niova_io_pwrite(): %s:%s (rrc=%zd expected-sz=%zu off=%ld)",
        strerror(-rc), strerror(sync_rc), write_sz,
        raft_server_entry_to_total_size(re),
        rsbr_raft_entry_to_phys_offset(ri, re));

    return rc;
}

static int
rsbp_stat_log_fd(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    int rc = fstat(rip->rip_fd, &rip->rip_stb);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "fstat(): %s", strerror(-rc));
    }

    return rc;
}

static ssize_t
rsbp_num_entries_calc(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_max_entry_size == RAFT_ENTRY_SIZE_POSIX);

    int rc = rsbp_stat_log_fd(ri);
    if (rc)
        return rc;

    const ssize_t log_sz = rsbp_ri_to_log_sz(ri);

    /* Calculate the number of entries based on the size of the log file
     * deducting the number of log header blocks.
     */
    ssize_t num_entries =
        MAX(0, ((log_sz / RAFT_ENTRY_SIZE_POSIX) +
                ((log_sz % RAFT_ENTRY_SIZE_POSIX) ? 1 : 0) -
                rsbr_get_num_log_headers(ri)));

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "num-block-entries=%zd", num_entries);

    return num_entries;
}

static int
rsbp_setup_initialize_headers(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    memset(&ri->ri_log_hdr, 0, sizeof(struct raft_log_header));

    // Since we're initializing the header block this is ok
    ri->ri_log_hdr.rlh_magic = RAFT_HEADER_MAGIC;

    for (int i = 0; i < rsbr_get_num_log_headers(ri); i++)
    {
        int rc = rsbp_header_write(ri);
        if (rc)
            return rc;

        /* Force the seqno increment here.  Typically, this is done through
         *   raft_server_log_header_write_prep().
         */
        ri->ri_log_hdr.rlh_seqno++;
    }

    return 0;
}

/**
 * rsbp_log_file_setup - open the log file and initialize it if it's
 *    newly created.  This function also has the role of determining the
 *    number of found raft entries setting ri->ri_entries_detected_at_startup.
 */
static int
rsbp_log_file_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    int flags = O_CREAT | O_RDWR |
        (raft_server_does_synchronous_writes(ri) ? O_SYNC : 0);

    int rc = 0;
    rip->rip_fd = open(ri->ri_log, flags, 0600);
    if (rip->rip_fd < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "open(`%s'): %s", ri->ri_log, strerror(-rc));
        return rc;
    }

    rc = rsbp_stat_log_fd(ri);
    if (rc)
        return rc;

    /* Initialize the log header if the file was just created.
     */
    if (!rip->rip_stb.st_size)
    {
        rc = rsbp_setup_initialize_headers(ri);
        if (rc)
            SIMPLE_LOG_MSG(LL_ERROR,
                           "raft_server_log_file_setup_init_header(): %s",
                           strerror(-rc));
    }

    if (!rc)
    {
        ri->ri_entries_detected_at_startup = rsbp_num_entries_calc(ri);

        if (ri->ri_entries_detected_at_startup < 0)
            rc = ri->ri_entries_detected_at_startup;
    }

    return rc;
}

// Part of rib_backend_shutdown
static int
rsbp_log_file_close(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    if (rip->rip_fd < 0)
        return 0;

    int rc = close(rip->rip_fd);
    rip->rip_fd = -1;

    return rc;
};

static int
rsbp_destroy(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = rsbp_log_file_close(ri);

    niova_free(ri->ri_backend_arg);
    ri->ri_backend = NULL;

    return rc;
}

static int
rsbp_sync(struct raft_instance *ri)
{
    if (!ri)
	return -EINVAL;

    struct raft_instance_posix *rip = rsbp_ri_to_rip(ri);

    int rc = niova_io_fsync(rip->rip_fd);
    if (rc < 0)
        rc = -errno;

    return rc;
}

static void
rsbp_set_db_uuid(struct raft_instance *ri)
{
    const struct ctl_svc_node_raft_peer *csnp =
        &ri->ri_csn_this_peer->csn_peer.csnp_raft_info;

    uuid_copy(ri->ri_db_uuid, csnp->csnrp_member.csrm_peer);
}

static int
rsbp_setup(struct raft_instance *ri)
{
    if (!ri || ri->ri_backend != &ribPosix || !ri->ri_csn_this_peer)
        return -EINVAL;

    else if (ri->ri_backend_arg)
        return -EALREADY;

    CONST_OVERRIDE(size_t, ri->ri_max_entry_size, RAFT_ENTRY_SIZE_POSIX);

    ri->ri_backend_arg = niova_calloc(1UL, sizeof(struct raft_instance_posix));

    if (!ri->ri_backend_arg)
        return -ENOMEM;

    int rc = rsbp_log_file_setup(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "rsbp_log_file_setup(): %s",
                          strerror(rc));

        rsbp_destroy(ri);
        return rc;
    }

    rsbp_set_db_uuid(ri);

    return 0;
}

void
raft_server_backend_use_posix(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && !ri->ri_backend);

    ri->ri_backend = &ribPosix;
}

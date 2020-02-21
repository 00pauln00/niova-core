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

#include "log.h"
#include "udp.h"
#include "epoll_mgr.h"
#include "crc32.h"
#include "alloc.h"
#include "io.h"
#include "random.h"
#include "ctl_svc.h"
#include "raft.h"
#include "raft_net.h"

REGISTRY_ENTRY_FILE_GENERATE;

/**
 * raft_server_entry_calc_crc - calculate the provided entry's crc and return
 *    the result without storing the crc in the entry.
 */
static crc32_t
raft_server_entry_calc_crc(const struct raft_entry *re)
{
    NIOVA_ASSERT(re);

    const struct raft_entry_header *rh = &re->re_header;
    const size_t offset = offsetof(struct raft_entry_header, reh_data_size);
    const unsigned char *buf = (const unsigned char *)re + offset;
    const int crc_len = sizeof(struct raft_entry) + rh->reh_data_size - offset;
    NIOVA_ASSERT(crc_len >= 0);

    crc32_t crc = crc_pcl(buf, crc_len, 0);

    DBG_RAFT_ENTRY(((crc == rh->reh_crc) ? LL_DEBUG : LL_WARN),
                   &re->re_header, "calculated crc=%x", crc);

    return crc;
}

/**
 * raft_server_entry_check_crc - call raft_server_entry_calc_crc() and compare
 *    the result with that in the provided raft_entry.
 */
static int
raft_server_entry_check_crc(const struct raft_entry *re)
{
    NIOVA_ASSERT(re);

    const struct raft_entry_header *reh = &re->re_header;

    return raft_server_entry_calc_crc(re) == reh->reh_crc ? 0 : -EBADMSG;
}

/**
 * raft_server_entry_init - initialize a raft_entry in preparation for writing
 *    it into the raft log file.
 * @re:  raft_entry to be intialized
 * @entry_index:  the physical index at which the block will be stored
 * @current_term:  the term to which this pending write operation belongs
 * @self_uuid:  UUID is this node instance, written into the entry for safety
 * @raft_uuid:  UUID of the raft instance, also written for safety
 * @data:  application data which is being stored in the block.
 * @len:  length of the application data
 */
static void
raft_server_entry_init(struct raft_entry *re, const size_t entry_index,
                       const uint64_t current_term,
                       const uuid_t self_uuid, const uuid_t raft_uuid,
                       const char *data, const size_t len)
{
    NIOVA_ASSERT(re);
    NIOVA_ASSERT(data && len);

    // Should have been checked already
    NIOVA_ASSERT(len <= RAFT_ENTRY_MAX_DATA_SIZE);

    struct raft_entry_header *reh = &re->re_header;

    reh->reh_magic = RAFT_ENTRY_MAGIC;
    reh->reh_data_size = len;
    reh->reh_index = entry_index;
    reh->reh_term = current_term;
    reh->reh_log_hdr_blk = entry_index < NUM_RAFT_LOG_HEADERS ? 1 : 0;

    uuid_copy(reh->reh_self_uuid, self_uuid);
    uuid_copy(reh->reh_raft_uuid, raft_uuid);

    memset(reh->reh_pad, 0, RAFT_ENTRY_PAD_SIZE);

    memcpy(re->re_data, data, len);

    // Checksum the entire entry (which includes the 'data' section
    reh->reh_crc = raft_server_entry_calc_crc(re);
}

static bool
raft_server_entry_next_entry_is_valid(const struct raft_instance *ri,
                                      const struct raft_entry_header *reh);

/**
 * raft_instance_update_newest_entry_hdr - the raft instance stores a copy of
 *    newest entry's header.  This function updates the raft instance with the
 *    contents of the provided entry_header.
 */
static void
raft_instance_update_newest_entry_hdr(struct raft_instance *ri,
                                      const struct raft_entry_header *reh)
{
    NIOVA_ASSERT(ri && reh);

    if (!reh->reh_log_hdr_blk)
        ri->ri_newest_entry_hdr = *reh;
}

/**
 * raft_server_entry_write - safely store an entry into the raft log at the
 *    specified index.  This function writes and syncs the data to the
 *    underlying device and handles partial writes.
 * @ri:  raft instance
 * @entry_index:  the physical index at which the block will be written
 * @data:  the application data buffer
 * @len:  length of the application data buffer.
 */
static int
raft_server_entry_write(struct raft_instance *ri, const size_t entry_index,
                        const char *data, size_t len)
{
    if (!ri || !data || !len || !ri->ri_csn_this_peer || !ri->ri_csn_raft)
        return -EINVAL;

    else if (len > RAFT_ENTRY_MAX_DATA_SIZE)
        return -E2BIG;

    const off_t total_entry_size = sizeof(struct raft_entry) + len;

    struct raft_entry *re = niova_malloc(total_entry_size);
    if (!re)
        return -ENOMEM;

    raft_server_entry_init(re, entry_index, ri->ri_log_hdr.rlh_term,
                           RAFT_INSTANCE_2_SELF_UUID(ri),
                           RAFT_INSTANCE_2_RAFT_UUID(ri), data, len);

    DBG_RAFT_ENTRY(LL_WARN, &re->re_header, "");

    /* Failues of the next set of operations will be fatal:
     * - Ensuring that the index increases by one and term is not decreasing
     * - The entire block was written without error
     * - The block log fd was sync'd without error.
     */
    DBG_RAFT_INSTANCE_FATAL_IF(
        (!raft_server_entry_next_entry_is_valid(ri, &re->re_header)), ri,
        "raft_server_entry_next_entry_is_valid() failed");

    const ssize_t write_sz =
        io_pwrite(ri->ri_log_fd, (const char *)re, total_entry_size,
                  (entry_index * RAFT_ENTRY_SIZE));

    NIOVA_ASSERT(write_sz == total_entry_size);

    int rc = io_fsync(ri->ri_log_fd);
    NIOVA_ASSERT(!rc);

    /* Following the successful writing and sync of the entry, copy the
     * header contents into the raft instance.   Note, this is a noop if the
     * entry is for a log header.
     */
    raft_instance_update_newest_entry_hdr(ri, &re->re_header);

    niova_free(re);

    return 0;
}

/**
 * read_server_entry_validate - checks the entry header contents against
 *    expected values.  This check preceeds the entry's CRC check and is meant
 *    to catch blocks which match their CRC but were not intended for this
 *    particular log instance.
 */
static int
read_server_entry_validate(const struct raft_instance *ri,
                           const struct raft_entry_header *rh,
                           const size_t intended_entry_index)
{
    NIOVA_ASSERT(ri && rh && ri->ri_csn_this_peer && ri->ri_csn_raft);

    // Validate magic and data size.
    if (rh->reh_magic != RAFT_ENTRY_MAGIC ||
        rh->reh_data_size > RAFT_ENTRY_MAX_DATA_SIZE)
        return -EINVAL;

    // Ensure the entry index found in the block matches the argument
    ssize_t my_intended_entry_index =
        intended_entry_index -
        (rh->reh_log_hdr_blk ? 0 : NUM_RAFT_LOG_HEADERS);

    if (my_intended_entry_index < 0 ||
        (size_t)my_intended_entry_index != rh->reh_index)
        return -EBADSLT;

    // Verify that header UUIDs match those of this raft instance.
    if (uuid_compare(rh->reh_self_uuid, RAFT_INSTANCE_2_SELF_UUID(ri)) ||
        uuid_compare(rh->reh_raft_uuid, RAFT_INSTANCE_2_RAFT_UUID(ri)))
        return -EKEYREJECTED;

    return 0;
}

/**
 * raft_server_entry_read - request a read of a raft log entry.
 * @ri:  raft instance pointer
 * @entry_index:  the physical index of the log block to be read.  Note that
 *     the first NUM_RAFT_LOG_HEADERS blocks are for the log header and the
 *     blocks which follow are for application data.  Also note that the
 *     raft_entry::reh_index is the "raft" index NOT the physical index
 *     represented by this parameter.  To obtain the physical index from
 *     raft_entry::reh_index, the NUM_RAFT_LOG_HEADERS value must be added.
 * @data:  sink buffer
 * @len:  size of the sink buffer
 * @rc_len:  the data size of this entry
 */
static int
raft_server_entry_read(struct raft_instance *ri, const size_t entry_index,
                       char *data, const size_t len, size_t *rc_len)
{
    if (!ri || !data || len > RAFT_ENTRY_SIZE)
        return -EINVAL;

    const off_t total_entry_size = sizeof(struct raft_entry) + len;

    struct raft_entry *re = niova_malloc(total_entry_size);
    if (!re)
        return -ENOMEM;

    const ssize_t read_sz =
        io_pread(ri->ri_log_fd, (char *)re, total_entry_size,
                 (entry_index * RAFT_ENTRY_SIZE));

    DBG_RAFT_ENTRY(LL_WARN, &re->re_header, "rrc=%zu", read_sz);

    NIOVA_ASSERT(read_sz == total_entry_size);

    const struct raft_entry_header *rh = &re->re_header;

    int rc = read_server_entry_validate(ri, rh, entry_index);
    if (!rc)
    {
        if (rc_len)
            *rc_len = rh->reh_data_size;

        if (rh->reh_data_size < len)
        {
            rc = -ENOSPC;
        }
        else
        {
            rc = raft_server_entry_check_crc(re);
            if (!rc)
                memcpy(data, re->re_data, len);
        }
    }

    niova_free(re);

    return rc;
}

/**
 * raft_server_entry_header_read - read only a raft log entry's header, the
 *    application contents of the entry are ignored and the crc is not taken.
 * @ri:  raft instance pointer
 * @entry_index:  physical entry to read (includes header blocks)
 * @reh:  the destination entry header buffer
 */
static int
raft_server_entry_header_read(struct raft_instance *ri,
                              const size_t entry_index,
                              struct raft_entry_header *reh)
{
    if (!ri || !reh)
        return -EINVAL;

    const ssize_t read_sz =
        io_pread(ri->ri_log_fd, (char *)reh, sizeof(struct raft_entry_header),
                 (entry_index * RAFT_ENTRY_SIZE));

    DBG_RAFT_ENTRY(LL_WARN, reh, "rrc=%zu", read_sz);

    NIOVA_ASSERT(read_sz == sizeof(struct raft_entry_header));

    return read_server_entry_validate(ri, reh, entry_index);
}

/**
 * raft_server_log_header_write - store the current raft state into the log
 *     header. This function is typically called while casting a vote for a
 *     candidate.
 * @ri:  this raft instance
 * @candidate:  UUID of the candidate being voted for.  May be NULL if the
 *     header is initialized.
 * @candidate_term:  term presented by the candidate
 */
static int
raft_server_log_header_write(struct raft_instance *ri, const uuid_t candidate,
                             const int64_t candidate_term)
{
    if (!ri)
        return -EINVAL;

    DBG_RAFT_INSTANCE_FATAL_IF((!uuid_is_null(candidate) &&
                                ri->ri_log_hdr.rlh_term > candidate_term),
                               ri, "invalid candidate term=%lx",
                               candidate_term);

    /* rlh_seqno is not used for the raft protocol.  It's used to bounce
     * between the different header blocks so that in the case of a partial
     * write, at least one header block remains valid.
     */
    ri->ri_log_hdr.rlh_seqno++;
    ri->ri_log_hdr.rlh_magic = RAFT_HEADER_MAGIC;
    ri->ri_log_hdr.rlh_term = candidate_term;
    uuid_copy(ri->ri_log_hdr.rlh_voted_for, candidate);

    const size_t block_num = ri->ri_log_hdr.rlh_seqno % NUM_RAFT_LOG_HEADERS;

    return raft_server_entry_write(ri, block_num,
                                   (const char *)&ri->ri_log_hdr,
                                   sizeof(struct raft_log_header));
}

static int
raft_server_header_load(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_log_header rlh[NUM_RAFT_LOG_HEADERS] = {0};
    struct raft_log_header *most_recent_rlh = NULL;

    for (size_t i = 0; i < NUM_RAFT_LOG_HEADERS; i++)
    {
        size_t rc_len = 0;
        char *buf = (char *)((struct raft_log_header *)&rlh[i]);

        int rc = raft_server_entry_read(ri, i, buf,
                                        sizeof(struct raft_log_header),
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

    DBG_RAFT_INSTANCE(LL_WARN, ri, "");

    return 0;
}

static int
raft_server_log_file_setup_init_header(struct raft_instance *ri)
{
    if (!ri || ri->ri_log_fd < 0 || ri->ri_log_stb.st_size != 0 ||
        !ri->ri_csn_this_peer || !ri->ri_csn_raft)
        return -EINVAL;

    struct raft_log_header *rlh = &ri->ri_log_hdr;

    memset(rlh, 0, sizeof(struct raft_log_header));

    uuid_t null_uuid;
    uuid_clear(null_uuid);

    for (int i = 0; i < NUM_RAFT_LOG_HEADERS; i++)
    {
        int rc = raft_server_log_header_write(ri, null_uuid, 0);
        if (rc)
            return rc;
    }

    return 0;
}

/**
 * raft_server_log_file_name_setup - copies the log file path into the
 *    raft instance.  Currently, this function uses the ctl-svc config file
 *    as the source of the file name.
 */
static int
raft_server_log_file_name_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    const char *store_path = ctl_svc_node_peer_2_store(ri->ri_csn_this_peer);
    if (!store_path)
        return -EINVAL;

    int rc = snprintf(ri->ri_log, PATH_MAX, "%s", store_path);

    return rc > PATH_MAX ? -ENAMETOOLONG : 0;
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
raft_server_log_file_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = raft_server_log_file_name_setup(ri);
    if (rc)
        return rc;

    SIMPLE_LOG_MSG(LL_WARN, "log-file=%s", ri->ri_log);

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
raft_server_num_entries_calc(struct raft_instance *ri)
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
                - NUM_RAFT_LOG_HEADERS));

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "num-block-entries=%zd", num_entries);

    return num_entries;
}

static size_t
raft_entry_idx_to_phys_idx(size_t raft_entry_idx)
{
    return raft_entry_idx + NUM_RAFT_LOG_HEADERS;
}

static bool
raft_server_entry_next_entry_is_valid(const struct raft_instance *ri,
                                      const struct raft_entry_header *reh)
{
    NIOVA_ASSERT(ri && reh);

    if (reh->reh_log_hdr_blk)
        return (reh->reh_index == 0 || reh->reh_index == 1) ? true : false;

    /* A null UUID means ri_newest_entry_hdr is uninitialized, otherwise,
     * the expected index is the 'newest' + 1.
     */
    const int64_t expected_raft_index =
        uuid_is_null(ri->ri_newest_entry_hdr.reh_self_uuid) ?
        0 : (ri->ri_newest_entry_hdr.reh_index + 1);

//XXX replace with raft_server_get_current_raft_entry_index()?

    /* The index must increase by '1' and the term must never decrease.
     */
    if (reh->reh_index != expected_raft_index ||
        reh->reh_term < raft_server_get_current_raft_entry_term(ri))
    {
        DBG_RAFT_ENTRY(LL_ERROR, &ri->ri_newest_entry_hdr, "invalid entry");
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "");

        return false;
    }

    return true;
}

/**
 * raft_server_entries_scan - reads through the non-header log entries to the
 *    log's end with the purpose of finding the latest valid entry.
 */
static int
raft_server_entries_scan(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    memset(&ri->ri_newest_entry_hdr, 0, sizeof(struct raft_entry_header));

    ssize_t num_entries = raft_server_num_entries_calc(ri);
    if (!num_entries)
        return 0;

    struct raft_entry_header reh;

    for (ssize_t i = 0; i < num_entries; i++)
    {
        int rc = raft_server_entry_read(ri, raft_entry_idx_to_phys_idx(i),
                                        (char *)&reh, sizeof(reh), NULL);
        if (rc)
        {
            DBG_RAFT_ENTRY(LL_WARN, &reh, "raft_server_entry_read():  %s",
                           strerror(-rc));
            break;
        }
        else if (!raft_server_entry_next_entry_is_valid(ri, &reh))
        {
            DBG_RAFT_ENTRY(LL_WARN, &reh,
                           "raft_server_entry_next_entry_is_valid() false");
            break;
        }

        raft_instance_update_newest_entry_hdr(ri, &reh);

        DBG_RAFT_ENTRY(LL_NOTIFY, &ri->ri_newest_entry_hdr, "newest_entry");
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "newest_entry");
    }

    return 0;
}

#if 0
static int
raft_server_log_prune(struct raft_instance *ri)
{
    // ftruncate file to offset of final valid entry.
    return 0;
}
#endif

/**
 * raft_server_log_load - read in the header blocks and scan the entry blocks
 *    to find the latest entry, checking for validity along the way.
 */
static int
raft_server_log_load(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    /* Check the log header
     */
    int rc = raft_server_header_load(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_header_load():  %s",
                          strerror(-rc));
        return rc;
    }

    rc = raft_server_entries_scan(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_entries_scan():  %s",
                          strerror(-rc));
        return rc;
    }

    return 0;
}

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

static void
raft_election_timeout_set(struct timespec *ts)
{
    if (!ts)
        return;

    unsigned long long msec =
        RAFT_ELECTION_MIN_TIME_MS + (random_get() % RAFT_ELECTION_RANGE_MS);

    msec_2_timespec(ts, msec);
}

static void
raft_heartbeat_timeout_sec(struct timespec *ts)
{
    msec_2_timespec(ts, RAFT_HEARTBEAT_TIME_MS);
}

/**
 * raft_server_timerfd_settime - set the timerfd based on the state of the
 *    raft instance.
 */
static void
raft_server_timerfd_settime(struct raft_instance *ri)
{
    struct itimerspec its = {0};

    if (ri->ri_state == RAFT_STATE_LEADER)
    {
        raft_heartbeat_timeout_sec(&its.it_value);
        its.it_interval = its.it_value;
    }
    else
    {
        raft_election_timeout_set(&its.it_value);
    }

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "msec=%llu",
                      nsec_2_msec(its.it_value.tv_nsec));

    int rc = timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);
    if (rc)
    {
        rc = -errno;
        DBG_RAFT_INSTANCE(LL_FATAL, ri, "timerfd_settime(): %s",
                          strerror(-rc));
    }
}

static int
raft_server_send_msg_to_client(struct udp_socket_handle *ush,
                               const struct sockaddr_in *dest,
                               struct raft_client_rpc_msg *rcm,
                               const char *reply_buf,
                               const size_t reply_buf_size)
{
    if (!ush || !dest || !rcm)
        return -EINVAL;

    struct iovec iov[2] = {
        [0].iov_len = sizeof(*rcm),
        [0].iov_base = (void *)rcm,
        [1].iov_len = reply_buf_size,
        [1].iov_base = (void *)reply_buf,
    };

    ssize_t size_rc = udp_socket_send(ush, iov, 2, dest);

    return (int)size_rc;
}

static int
raft_server_send_msg(struct udp_socket_handle *ush,
                     struct ctl_svc_node *rp, const struct raft_rpc_msg *rrm)
{
    if (!ush || !rp || !rrm)
        return -EINVAL;

    struct sockaddr_in dest;
    int rc = udp_setup_sockaddr_in(ctl_svc_node_peer_2_ipaddr(rp),
                                   ctl_svc_node_peer_2_port(rp), &dest);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "udp_setup_sockaddr_in(): %s (peer=%s:%hu)",
                strerror(-rc), ctl_svc_node_peer_2_ipaddr(rp),
                ctl_svc_node_peer_2_port(rp));

        return rc;
    }

    struct iovec iov = {
        .iov_len = sizeof(*rrm),
        .iov_base = (void *)rrm
    };

    ssize_t size_rc = udp_socket_send(ush, &iov, 1, &dest);

    return rc ? rc : size_rc;
}

static void
raft_server_broadcast_msg(struct raft_instance *ri,
                          const struct raft_rpc_msg *rrm)
{
    const raft_peer_t num_peers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    NIOVA_ASSERT(num_peers <= CTL_SVC_MAX_RAFT_PEERS);

    for (int i = 0; i < num_peers; i++)
    {
        struct ctl_svc_node *rp = ri->ri_csn_raft_peers[i];

        if (rp == ri->ri_csn_this_peer)
            continue;

        raft_server_send_msg(&ri->ri_ush[RAFT_UDP_LISTEN_SERVER], rp, rrm);
    }
}

/**
 * raft_server_sync_vote_choice - this server has decided to vote for a
 *    candidate.  Before replying to that candidate, the choice must be stored
 *    locally in the log header.
 * @ri:  raft instance
 * @candidate:  UUID of the candidate being voted for
 * @candidate_term:  the term presented by the candidate
 */
static int
raft_server_sync_vote_choice(struct raft_instance *ri,
                             const uuid_t candidate, int64_t candidate_term)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft);

    // These checks should have been done prior to entering this function!
    DBG_RAFT_INSTANCE_FATAL_IF((candidate_term <= ri->ri_log_hdr.rlh_term),
                               ri, "candidate_term=%lx", candidate_term);

    DBG_RAFT_INSTANCE_FATAL_IF(
        (raft_peer_2_idx(ri, candidate) >=
         ctl_svc_node_raft_2_num_members(ri->ri_csn_raft)), ri,
        "invalid candidate uuid");

    return raft_server_log_header_write(ri, candidate, candidate_term);
}

static raft_net_timerfd_cb_ctx_t
raft_server_init_candidate_state(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    ri->ri_state = RAFT_STATE_CANDIDATE;

    struct raft_candidate_state *rcs = &ri->ri_candidate;

    rcs->rcs_term = ri->ri_log_hdr.rlh_term + 1;

    for (raft_peer_t i = 0; i < CTL_SVC_MAX_RAFT_PEERS; i++)
        rcs->rcs_results[i] = RATE_VOTE_RESULT_UNKNOWN;
}

static raft_peer_t
raft_server_candidate_count_votes(struct raft_instance *ri,
                                  enum raft_vote_result result)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft);
    raft_peer_t cnt = 0;

    const raft_peer_t npeers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    NIOVA_ASSERT(npeers <= CTL_SVC_MAX_RAFT_PEERS);

    for (raft_peer_t i = 0; i < npeers; i++)
        if (ri->ri_candidate.rcs_results[i] == result)
            cnt++;

    return cnt;
}

static bool
raft_server_candidate_is_viable(const struct raft_instance *ri)
{
    if (ri &&
        (ri->ri_state != RAFT_STATE_CANDIDATE ||
         ri->ri_candidate.rcs_term != ri->ri_log_hdr.rlh_term))
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri,
                          "!candidate OR candidate-term (%lx) != ht",
                          ri->ri_candidate.rcs_term);
        return false;
    }

    return true;
}

static bool
raft_server_candidate_can_become_leader(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    // Perform some sanity checks prior to counting the 'yes' votes.
    if (!raft_server_candidate_is_viable(ri))
        return false;

    const raft_peer_t num_yes_votes =
        raft_server_candidate_count_votes(ri, RATE_VOTE_RESULT_YES);

    const raft_peer_t npeers_majority =
        (ctl_svc_node_raft_2_num_members(ri->ri_csn_raft) / 2) + 1;

    return num_yes_votes >= npeers_majority ? true : false;
}

/**
 * raft_server_candidate_reg_vote_result - called when this raft instance is a
 *     candidate and a vote reply needs to be registered in the local table.
 */
static int
raft_server_candidate_reg_vote_result(struct raft_instance *ri,
                                      uuid_t voter,
                                      enum raft_vote_result result)
{
    if (!ri || ri->ri_state != RAFT_STATE_CANDIDATE ||
        result == RATE_VOTE_RESULT_UNKNOWN)
        return -EINVAL;

    raft_peer_t peer_idx = raft_peer_2_idx(ri, voter);

    if (peer_idx >= ctl_svc_node_raft_2_num_members(ri->ri_csn_raft))
        return -ERANGE;

    struct raft_candidate_state *rcs = &ri->ri_candidate;

    DBG_RAFT_INSTANCE_FATAL_IF((rcs->rcs_term != ri->ri_log_hdr.rlh_term), ri,
                               "rcs->rcs_term (%lx) != ri_log_hdr",
                               rcs->rcs_term);

    rcs->rcs_results[peer_idx] = result;

    DBG_RAFT_INSTANCE(LL_WARN, ri, "peer-idx=%hhu voted=%s",
                      peer_idx, result == RATE_VOTE_RESULT_YES ? "yes" : "no");

    return 0;
}

static raft_server_timerfd_cb_ctx_int_t
raft_server_vote_for_self(struct raft_instance *ri)
{
    int rc = raft_server_sync_vote_choice(ri, RAFT_INSTANCE_2_SELF_UUID(ri),
                                          ri->ri_log_hdr.rlh_term + 1);
    if (rc)
        return rc;

    rc =
        raft_server_candidate_reg_vote_result(ri,
                                              RAFT_INSTANCE_2_SELF_UUID(ri),
                                              RATE_VOTE_RESULT_YES);
    return rc;
}

/**
 * raft_server_become_candidate - called when the raft instance is either in
 *    follower or candidate mode and the leader has not provided a heartbeat
 *    within the timeout threshold.
 */
static raft_net_timerfd_cb_ctx_t
raft_server_become_candidate(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_this_peer);
    NIOVA_ASSERT(ri->ri_state != RAFT_STATE_LEADER);

    // Reset vote counters
    raft_server_init_candidate_state(ri);

    int rc = raft_server_vote_for_self(ri);

    if (rc) // Failed to sync our own log header!
        DBG_RAFT_INSTANCE(LL_FATAL, ri, "raft_server_log_header_write(): %s",
                          strerror(-rc));

    struct raft_rpc_msg rrm = {
      //.rrm_rrm_sender_id = ri->ri_csn_this_peer.csn_uuid,
        .rrm_type = RAFT_RPC_MSG_TYPE_VOTE_REQUEST,
        .rrm_version = 0,
        .rrm_vote_request.rvrqm_proposed_term = ri->ri_log_hdr.rlh_term, //XXx
        .rrm_vote_request.rvrqm_last_log_term =
            raft_server_get_current_raft_entry_term(ri),
        .rrm_vote_request.rvrqm_last_log_index =
            raft_server_get_current_raft_entry_index(ri),
    };

    uuid_copy(rrm.rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(rrm.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    DBG_RAFT_INSTANCE(LL_WARN, ri, "");

    raft_server_broadcast_msg(ri, &rrm);
}

/**
 * raft_server_candidate_becomes_follower - handle the transition from a
 *    candidate to a follower for the case where a voter has informed us of
 *    a higher term number than our own.  This function sets the new term
 *    in ri_log_hdr but does not sync it to the raft log header.  Should this
 *    instance become a candidate before voting for another peer, the
 *    new_term provided here will be the new base term value.
 */
static void
raft_server_candidate_becomes_follower(struct raft_instance *ri,
                                       int64_t new_term,
                                       const uuid_t peer_with_newer_term,
                                       const bool append_entries_recv_ctx)
{
    NIOVA_ASSERT(ri &&
                 (new_term > ri->ri_log_hdr.rlh_term ||
                  (append_entries_recv_ctx &&
                   new_term >= ri->ri_log_hdr.rlh_term)));

    DECLARE_AND_INIT_UUID_STR(peer_uuid_str, peer_with_newer_term);

    if (append_entries_recv_ctx)
    {
        DBG_RAFT_INSTANCE(LL_WARN, ri, "%s became leader before us (term %lx)",
                          peer_uuid_str, new_term);
    }
    else
    {
        DBG_RAFT_INSTANCE(LL_WARN, ri, "%s has >= term %lx",
                          peer_uuid_str, new_term);
    }

    ri->ri_log_hdr.rlh_term = new_term;
    ri->ri_state = RAFT_STATE_FOLLOWER;
}

/**
 * raft_server_leader_init_state - setup the raft instance for leader duties.
 */
static raft_server_udp_cb_ctx_t
raft_server_leader_init_state(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    ri->ri_state = RAFT_STATE_LEADER;

    ri->ri_leader.rls_leader_term = ri->ri_log_hdr.rlh_term;

    const raft_peer_t num_raft_peers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    struct raft_leader_state *rls = &ri->ri_leader;

    for (raft_peer_t i = 0; i < num_raft_peers; i++)
    {
        rls->rls_next_idx[i] =
            raft_server_get_current_raft_entry_index(ri) + 1;

        // XXX When a peer NACKs an append-entry request, rls_prev_idx_term
        //     must be set to -1.
        rls->rls_prev_idx_term[i] =
            raft_server_get_current_raft_entry_term(ri);
    }
}

static raft_server_udp_cb_ctx_t
raft_server_candidate_becomes_leader(struct raft_instance *ri)
{
    DBG_RAFT_INSTANCE_FATAL_IF((!raft_server_candidate_is_viable(ri)), ri,
                               "!raft_server_candidate_is_viable()");

    raft_server_leader_init_state(ri);

    // Modify timer_fd timeout for the leader role.
    raft_server_timerfd_settime(ri);

    DBG_RAFT_INSTANCE(LL_WARN, ri, "");
}

/**
 * raft_server_process_vote_reply - handle a peer's response to our vote
 *    request.
 */
static raft_server_udp_cb_ctx_t
raft_server_process_vote_reply(struct raft_instance *ri,
                               struct ctl_svc_node *sender_csn,
                               const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);

    // The caller *should* have already checked this.
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    /* Do not proceed if this instance's candidate status has changed.  It's
     * possible that the process has received this reply after demotion or
     * promotion (to leader).
     */
    if (ri->ri_state != RAFT_STATE_CANDIDATE)
        return;

    const struct raft_vote_reply_msg *vreply = &rrm->rrm_vote_reply;

    enum raft_vote_result result =
        rrm->rrm_vote_reply.rvrpm_voted_granted ?
        RATE_VOTE_RESULT_YES : RATE_VOTE_RESULT_NO;

    int rc = raft_server_candidate_reg_vote_result(ri, sender_csn->csn_uuid,
                                                   result);
    if (rc)
    {
        DBG_RAFT_MSG(LL_ERROR, rrm,
                     "raft_server_candidate_reg_vote_result() %s",
                     strerror(-rc));
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "");
    }
    else if (result == RATE_VOTE_RESULT_NO &&
             ri->ri_log_hdr.rlh_term < vreply->rvrpm_term)
    {
        // The peer has replied that our term is stale
        raft_server_candidate_becomes_follower(ri, vreply->rvrpm_term,
                                               rrm->rrm_sender_id, false);
    }
    else if (result == RATE_VOTE_RESULT_YES &&
             raft_server_candidate_can_become_leader(ri))
    {
        // We have enough votes in this term to become the leader!
        raft_server_candidate_becomes_leader(ri);
    }
}

/**
 * raft_server_refresh_follower_prev_log_term - called while in leader mode,
 *     this function performs the role of reading and storing the term value
 *     for a given log index.  The index is determined by the follower's
 *     'next-idx' value.  The prev_log_term value for the next-index - 1 is
 *     stored in the raft leader structure.  This is so that retries for the
 *     same append entry do not incur extra I/O.
 * @ri:  raft instance
 * @follower:  the numeric position of the follower peer
 */
static raft_server_leader_mode_int_t
raft_server_refresh_follower_prev_log_term(struct raft_instance *ri,
                                           const raft_peer_t follower)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft &&
                 follower < ctl_svc_node_raft_2_num_members(ri->ri_csn_raft));

    // If the next_idx is '0' this means that no block have ever been written.
    if (ri->ri_leader.rls_next_idx[follower] == 0)
        ri->ri_leader.rls_prev_idx_term[follower] = 0;

    bool refresh = ri->ri_leader.rls_prev_idx_term[follower] < 0 ?
        true : false;

    if (refresh)
    {
        struct raft_entry_header reh = {0};

        const size_t phys_entry_idx =
            ri->ri_leader.rls_next_idx[follower] - 1 + NUM_RAFT_LOG_HEADERS;

        NIOVA_ASSERT(phys_entry_idx > NUM_RAFT_LOG_HEADERS);

        int rc = raft_server_entry_header_read(ri, phys_entry_idx, &reh);
        if (rc < 0)
            return rc;

        ri->ri_leader.rls_prev_idx_term[follower] = reh.reh_term;
    }

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri,
                      "peer=%hhx refresh=%s pt=%lx ni=%lx",
                      follower, refresh ? "yes" : "no",
                      ri->ri_leader.rls_prev_idx_term[follower],
                      ri->ri_leader.rls_next_idx[follower]);

    return 0;
}

/**
 * raft_server_issue_heartbeat_to_peer - tailors a partially initialized
 *     raft_rpc_msg for a specific peer.
 * @ri:  raft instance pointer
 * @peer:  destination follower peer
 * @raerm:  a preconfigured rpc_msg which needs some peer-specific adjustments
 */
static raft_server_leader_mode_t
raft_server_prep_append_entries_for_follower(
    struct raft_instance *ri,
    const raft_peer_t follower,
    struct raft_append_entries_request_msg *raerm)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft && raerm &&
                 follower < ctl_svc_node_raft_2_num_members(ri->ri_csn_raft));

    int rc = raft_server_refresh_follower_prev_log_term(ri, follower);

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                             "raft_server_refresh_follower_prev_log_term() %s",
                               strerror(-rc));

    raerm->raerqm_prev_log_index = ri->ri_leader.rls_next_idx[follower] - 1;
    raerm->raerqm_prev_log_term = ri->ri_leader.rls_prev_idx_term[follower];
}

static raft_net_timerfd_cb_ctx_t
raft_server_issue_heartbeat(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_this_peer);
    NIOVA_ASSERT(ri->ri_state == RAFT_STATE_LEADER);

    struct raft_rpc_msg rrm = {
        .rrm_type = RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST,
        .rrm_version = 0,
        .rrm_append_entries_request.raerqm_term = ri->ri_log_hdr.rlh_term,
        .rrm_append_entries_request.raerqm_commit_index = ri->ri_commit_idx,
        .rrm_append_entries_request.raerqm_entries_sz = 0, //heartbeat
    };

    uuid_copy(rrm.rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(rrm.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    const raft_peer_t num_peers =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    NIOVA_ASSERT(num_peers <= CTL_SVC_MAX_RAFT_PEERS);

    for (raft_peer_t i = 0; i < num_peers; i++)
    {
        struct ctl_svc_node *rp = ri->ri_csn_raft_peers[i];

        // Skip ourself
        if (rp == ri->ri_csn_this_peer)
            continue;

        struct raft_append_entries_request_msg *raerq =
            &rrm.rrm_append_entries_request;

        /* Append entry msgs must be tailored per peer / follower.  Note, that
         * raft_server_prep_append_entries_for_follower() may read from the
         * raft log to obtain the prev_log_term for an older index.
         */
        raft_server_prep_append_entries_for_follower(ri, i, raerq);

        raft_server_send_msg(&ri->ri_ush[RAFT_UDP_LISTEN_SERVER], rp, &rrm);
    }
}

static raft_net_timerfd_cb_ctx_t
raft_server_timerfd_cb(struct raft_instance *ri)
{
    switch (ri->ri_state)
    {
    case RAFT_STATE_FOLLOWER: // fall through
    case RAFT_STATE_CANDIDATE:
        raft_server_become_candidate(ri);
        break;

    case RAFT_STATE_LEADER:
        raft_server_issue_heartbeat(ri);
        break;
    default:
        break;
    }

    raft_server_timerfd_settime(ri);
}

/**
 * raft_server_process_vote_request_decide - determine if this peer should
 *    vote for the candidate.
 */
static bool
raft_server_process_vote_request_decide(const struct raft_instance *ri,
                                      const struct raft_vote_request_msg *vreq)
{
    NIOVA_ASSERT(ri && vreq);

    // "allow at most one winner per term"
    if (vreq->rvrqm_proposed_term <= ri->ri_log_hdr.rlh_term)
        return false;

    else if (vreq->rvrqm_last_log_term <
             raft_server_get_current_raft_entry_term(ri))
        return false;

    else if (vreq->rvrqm_last_log_index <
             raft_server_get_current_raft_entry_index(ri))
        return false;

    return true;
}

/**
 * raft_server_process_vote_request - peer has requested that we vote for
 *    them.
 */
static raft_server_udp_cb_ctx_t
raft_server_process_vote_request(struct raft_instance *ri,
                                 struct ctl_svc_node *sender_csn,
                                 const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);

    // The caller *should* have already checked this.
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    const struct raft_vote_request_msg *vreq = &rrm->rrm_vote_request;

    struct raft_rpc_msg rreply_msg = {0};
    int rc = 0;

    /* Do some initialization on the reply message.
     */
    uuid_copy(rreply_msg.rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(rreply_msg.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    rreply_msg.rrm_type = RAFT_RPC_MSG_TYPE_VOTE_REPLY;
    rreply_msg.rrm_vote_reply.rvrpm_term = ri->ri_log_hdr.rlh_term;

    /* Check the vote criteria - do we vote 'yes' or 'no'?
     */
    rreply_msg.rrm_vote_reply.rvrpm_voted_granted =
        raft_server_process_vote_request_decide(ri, vreq) ? 1 : 0;

    DBG_RAFT_MSG(LL_WARN, rrm, "vote=%s my term=%lx last=%lx:%lx",
                 rreply_msg.rrm_vote_reply.rvrpm_voted_granted ? "yes" : "no",
                 ri->ri_log_hdr.rlh_term,
                 raft_server_get_current_raft_entry_term(ri),
                 raft_server_get_current_raft_entry_index(ri));

    /* We intend to vote 'yes' - sync the candidate's term and UUID to our
     * log header.
     */
    if (rreply_msg.rrm_vote_reply.rvrpm_voted_granted)
    {
        /// XXX there's one other case where this call needs to be made!
        //      this is where / when a candidate gets an append entry RPC
        //      from a peer!
        raft_server_candidate_becomes_follower(ri, vreq->rvrqm_proposed_term,
                                               rrm->rrm_sender_id, false);

        rc = raft_server_log_header_write(ri, rrm->rrm_sender_id,
                                          vreq->rvrqm_proposed_term);
        DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                                   "raft_server_log_header_write() %s",
                                   strerror(-rc));
    }

    /* Inform the candidate of our vote.
     */
    raft_server_send_msg(&ri->ri_ush[RAFT_UDP_LISTEN_SERVER], sender_csn,
                         &rreply_msg);
}

static raft_server_udp_cb_ctx_t
raft_server_process_append_entries_request(struct raft_instance *ri,
                                           struct ctl_svc_node *sender_csn,
                                           const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    DBG_RAFT_MSG(LL_WARN, rrm, "");

    struct raft_rpc_msg rreply_msg = {0};

    rreply_msg.rrm_type = RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY;

    uuid_copy(rreply_msg.rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(rreply_msg.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    struct raft_append_entries_reply_msg *rae_reply =
        &rreply_msg.rrm_append_entries_reply;

    rae_reply->raerpm_term = ri->ri_log_hdr.rlh_term;

    const struct raft_append_entries_request_msg *raerq =
        &rrm->rrm_append_entries_request;

    // XXx for now, don't allow anything other than heartbeat msgs
    NIOVA_ASSERT(!raerq->raerqm_entries_sz);

    bool reset_timerfd = true;

    // My term is newer which means this sender is a stale leader.
    if (ri->ri_log_hdr.rlh_term > raerq->raerqm_term)
    {
        rae_reply->raerpm_err_stale_term = 1;
        reset_timerfd = false;
    }
    // Sender's term is greater than or equal, demote myself if candidate.
    else if (ri->ri_state == RAFT_STATE_CANDIDATE)
    {
        raft_server_candidate_becomes_follower(ri, raerq->raerqm_term,
                                               sender_csn->csn_uuid, true);
    }

    // XXX compare last_log_index and term

    if (reset_timerfd)
        raft_server_timerfd_settime(ri);

    raft_server_send_msg(&ri->ri_ush[RAFT_UDP_LISTEN_SERVER], sender_csn,
                         &rreply_msg);
}

/**
 * raft_server_process_received_server_msg - called following the arrival of
 *    a udp message on the server <-> server socket.  After verifying
 *    that the sender's UUID and its raft UUID are known, this function will
 *    call the appropriate function handler based on the msg type.
 */
static raft_net_udp_cb_ctx_t
raft_server_process_received_server_msg(struct raft_instance *ri,
	                                const struct raft_rpc_msg *rrm,
                                        struct ctl_svc_node *sender_csn)
{
    NIOVA_ASSERT(ri && rrm && sender_csn);

    raft_net_update_last_comm_time(ri, sender_csn->csn_uuid, false);

    switch (rrm->rrm_type)
    {
    case RAFT_RPC_MSG_TYPE_VOTE_REQUEST:
        return raft_server_process_vote_request(ri, sender_csn, rrm);

    case RAFT_RPC_MSG_TYPE_VOTE_REPLY:
        return raft_server_process_vote_reply(ri, sender_csn, rrm);

    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST:
	return raft_server_process_append_entries_request(ri, sender_csn, rrm);

#if 0
    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY:
        return raft_server_process_append_entries_reply(ri, sender_csn, rrm);
#endif

    default:
        DBG_RAFT_MSG(LL_NOTIFY, rrm, "unhandled msg type %d", rrm->rrm_type);
        break;
    }
}

static raft_net_udp_cb_ctx_t
raft_server_udp_peer_recv_handler(struct raft_instance *ri,
                                  const char *recv_buffer,
                                  ssize_t recv_bytes,
                                  const struct sockaddr_in *from)
{
    NIOVA_ASSERT(ri && from);

    if (!recv_buffer || !recv_bytes)
        return;

    const struct raft_rpc_msg *rrm = (const struct raft_rpc_msg *)recv_buffer;

    /* Server <-> server messages do not have additional payloads.
     */
    if (recv_bytes != sizeof(struct raft_rpc_msg))
    {
        DBG_RAFT_MSG(LL_WARN, rrm,
                     "Invalid msg size (%zd) from peer %s:%d",
                     recv_bytes, inet_ntoa(from->sin_addr),
                     ntohs(from->sin_port));
        return;
    }

    /* Verify the sender's id before proceeding.
     */
    struct ctl_svc_node *sender_csn =
        raft_net_verify_sender_server_msg(ri, rrm->rrm_sender_id,
                                          rrm->rrm_raft_id, from);
    if (!sender_csn)
        return;

    DBG_RAFT_MSG(LL_DEBUG, rrm, "msg-size=(%zd) peer %s:%d",
                 recv_bytes, inet_ntoa(from->sin_addr),
                 ntohs(from->sin_port));

    raft_server_process_received_server_msg(ri, rrm, sender_csn);
}

/**
 * raft_server_may_process_client_request - this function checks the state of
 *    this raft instance to determine if it's qualified to accept a client
 *    request.
 */
static raft_net_udp_cb_ctx_int_t
raft_server_may_accept_client_request(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    // XXX Need several checks here!

    /* Not the leader, then cause a redirect reply to be done.
     */
    if (!raft_instance_is_leader(ri)) // 1. am I the raft leader?
        return -ENOSYS;

#if 0
    // 2. am I a fresh raft leader?
    else if (!raft_leader_instance_is_fresh(ri))
        return -EAGAIN;

    // 3. have I applied all of the lastApplied entries that I need -
    //    including a fake AE command (which is written to the logs)?
    else if (!raft_leader_has_applied_txn_in_my_term(ri))
        return -EBUSY;
#endif

    return 0;
}

static raft_net_udp_cb_ctx_t
raft_server_reply_to_client(struct raft_instance *ri,
                            const struct sockaddr_in *dest,
                            const struct raft_client_rpc_msg *request_rcm,
                            const int reply_error_code, const char *reply_buf,
                            const size_t reply_buf_sz)
{
    if (!ri || !ri->ri_csn_this_peer || !dest || !request_rcm ||
        reply_buf_sz > RAFT_NET_MAX_RPC_SIZE)
        return;

    // Copy the source msg and then tailor it accordingly
    struct raft_client_rpc_msg reply_rcm = *request_rcm;

    uuid_clear(reply_rcm.rcrm_gmsg.rcrgm_redirect_id);

    reply_rcm.rcrm_gmsg.rcrgm_msg_size = reply_buf_sz;

    // may be reset below
    reply_rcm.rcrm_gmsg.rcrgm_msg_type = RAFT_CLIENT_RPC_MSG_TYPE_REPLY;
    reply_rcm.rcrm_gmsg.rcrgm_error = reply_error_code;

    switch (reply_error_code)
    {
    case -ENOSYS:
        reply_rcm.rcrm_gmsg.rcrgm_msg_type = RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT;

        if (ri->ri_csn_leader)
            uuid_copy(reply_rcm.rcrm_gmsg.rcrgm_redirect_id,
                      ri->ri_csn_leader->csn_uuid);
        break;
    default:
        break;
    }

    DBG_RAFT_CLIENT_RPC(LL_WARN, &reply_rcm, dest, "");

    /* Set the sender_id AFTER logging so dest UUID is logged not our UUID.
     */
    uuid_copy(reply_rcm.rcrm_sender_id, ri->ri_csn_this_peer->csn_uuid);

    raft_server_send_msg_to_client(&ri->ri_ush[RAFT_UDP_LISTEN_CLIENT],
                                   dest, &reply_rcm, reply_buf, reply_buf_sz);
}

static raft_net_udp_cb_ctx_t
raft_server_leader_write_new_entry(struct raft_instance *ri,
                                   const char *data, const size_t len)
{
    if (!raft_instance_is_leader(ri))
        return;

    size_t entry_index =
        raft_entry_idx_to_phys_idx(ri->ri_newest_entry_hdr.reh_index);

    int rc = raft_server_entry_write(ri, entry_index, data, len);
    if (rc)
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_entry_write(): %s",
                          strerror(-rc));

    // We need to schedule ourselves to potentially deliver this new block
    // to the followers.  However, we should attempt to only send this
    // followers which are ready for this block.  Otherwise, if a follower
    // does see a msg at an index > its max index, it should NACK with its
    // max index so that the rollback protocal can complete without starting
    // over.
}

static raft_net_udp_cb_ctx_t
raft_server_udp_client_recv_handler(struct raft_instance *ri,
                                    const char *recv_buffer,
                                    ssize_t recv_bytes,
                                    const struct sockaddr_in *from)
{
    static char reply_buf[RAFT_NET_MAX_RPC_SIZE];

    NIOVA_ASSERT(ri && from);

    if (!recv_buffer || !recv_bytes || !ri->ri_server_sm_request_cb ||
        recv_bytes < sizeof(struct raft_client_rpc_msg))
        return;

    const struct raft_client_rpc_msg *rcm =
        (const struct raft_client_rpc_msg *)recv_buffer;

    if (raft_net_verify_sender_client_msg(ri, rcm->rcrm_raft_id))
        return;

    int rc = raft_server_may_accept_client_request(ri);
    if (rc)
    {
        raft_server_reply_to_client(ri, from, rcm, rc, NULL, 0);
        return;
    }

    bool write_op = false;
    size_t reply_size = 0;

    /* Call into the application state machine logic.  There are several
     * outcomes here:
     * 1. SM detects a new write, here it may store sender info for reply
     *    post-commit.
     * 2. SM detects a write which had already been committed, here we reply
     *    to the client notifying it of the completion.
     * 3. SM detects a write which is still in progress, here no reply is sent.
     * 4. SM processes a read request, returning the requested application
     *    data. (XXX need a buffer for this data!)
     */
    rc = ri->ri_server_sm_request_cb(rcm, from, &write_op, reply_buf,
                                     &reply_size);

    DBG_RAFT_CLIENT_RPC(LL_WARN, rcm, from, "rc=%d wr=%d rbuf-sz=%zu",
                        rc, write_op, reply_size);

//Xxx should do this again 'if (raft_server_may_accept_client_request(ri))'
//    since cb's may run for a long time and the server may have been deposed

    /* Read operation or an already committed + applied write operation.
     */
    if (!write_op || (write_op && rc == -EALREADY))
        raft_server_reply_to_client(ri, from, rcm, 0, reply_buf, reply_size);

    /* Store the request as an entry in the Raft log.  Do not reply to the
     * client until the write is committed.
     */
    else if (write_op && !rc)
        raft_server_leader_write_new_entry(ri, rcm->rcrm_gmsg.rcrgm_data,
                                           rcm->rcrm_gmsg.rcrgm_msg_size);
}

int
raft_server_instance_startup(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_state == RAFT_STATE_FOLLOWER);

    /* Assign the timer_fd and udp_recv callbacks.
     */
    raft_net_instance_apply_callbacks(ri, raft_server_timerfd_cb,
                                      raft_server_udp_client_recv_handler,
                                      raft_server_udp_peer_recv_handler);

    int rc = raft_server_log_file_setup(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_log_file_setup(): %s",
                          strerror(-rc));
        return rc;
    }

    rc = raft_server_log_load(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_log_load(): %s",
                          strerror(-rc));

        raft_server_instance_shutdown(ri);
        return rc;
    }

    return 0;
}

int
raft_server_instance_shutdown(struct raft_instance *ri)
{
    raft_server_log_file_close(ri);

    return 0;
}

int
raft_server_main_loop(struct raft_instance *ri)
{
    raft_server_timerfd_settime(ri);

    int rc = 0;

    do
    {
        rc = epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);
        if (rc == -EINTR)
            rc = 0;
    } while (rc > 0);

    return rc;
}

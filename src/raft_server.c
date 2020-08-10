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
#include "epoll_mgr.h"
#include "fault_inject.h"
#include "io.h"
#include "log.h"
#include "net_ctl.h"
#include "raft.h"
#include "raft_net.h"
#include "random.h"
#include "registry.h"
#include "util_thread.h"

LREG_ROOT_ENTRY_GENERATE(raft_root_entry, LREG_USER_TYPE_RAFT);

enum raft_write_entry_opts
{
    RAFT_WR_ENTRY_OPT_NONE                 = 0,
    RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER = 1,
    RAFT_WR_ENTRY_OPT_LOG_HEADER           = 2,
    RAFT_WR_ENTRY_OPT_ANY                  = 255,
};

REGISTRY_ENTRY_FILE_GENERATE;

static const char *
raft_server_may_accept_client_request_reason(const struct raft_instance *ri);

static raft_peer_t
raft_server_instance_self_idx(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_this_peer);

    return  raft_peer_2_idx(ri, ri->ri_csn_this_peer->csn_uuid);
}

static const char *
raft_follower_reason_2_str(enum raft_follower_reasons reason)
{
    switch (reason)
    {
    case RAFT_BFRSN_NONE:
        return "none";
    case RAFT_BFRSN_VOTED_FOR_PEER:
        return "voted-for-peer";
    case RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE:
        return "lost-election";
    case RAFT_BFRSN_STALE_TERM_WHILE_LEADER:
        return "stale-leader";
    case RAFT_BFRSN_LEADER_ALREADY_PRESENT:
        return "leader-already-present";
    default:
        break;
    }

    return NULL;
}

enum raft_instance_lreg_entry_values
{
    RAFT_LREG_RAFT_UUID,          // string
    RAFT_LREG_PEER_UUID,          // string
    RAFT_LREG_VOTED_FOR_UUID,     // string
    RAFT_LREG_LEADER_UUID,        // string
    RAFT_LREG_PEER_STATE,         // string
    RAFT_LREG_FOLLOWER_REASON,    // string
    RAFT_LREG_CLIENT_REQUESTS,    // string
    RAFT_LREG_TERM,               // int64
    RAFT_LREG_COMMIT_IDX,         // int64
    RAFT_LREG_LAST_APPLIED,       // int64
    RAFT_LREG_LAST_APPLIED_CCRC,  // int64
    RAFT_LREG_NEWEST_ENTRY_IDX,   // int64
    RAFT_LREG_NEWEST_ENTRY_TERM,  // int64
    RAFT_LREG_NEWEST_ENTRY_SIZE,  // uint32
    RAFT_LREG_NEWEST_ENTRY_CRC,   // uint32
    RAFT_LREG_HIST_DEV_READ_LAT,  // hist object
    RAFT_LREG_HIST_DEV_WRITE_LAT, // hist object
    RAFT_LREG_FOLLOWER_VSTATS,    // varray - last follower node
    RAFT_LREG_HIST_COMMIT_LAT,    // hist object
    RAFT_LREG_HIST_READ_LAT,      // hist object
    RAFT_LREG_MAX,
    RAFT_LREG_MAX_FOLLOWER = RAFT_LREG_FOLLOWER_VSTATS,
};

static util_thread_ctx_reg_int_t
raft_instance_lreg_peer_vstats_cb(enum lreg_node_cb_ops op,
                                  struct lreg_node *lrn,
                                  struct lreg_value *lv);

static util_thread_ctx_reg_int_t
raft_instance_lreg_multi_facet_cb(enum lreg_node_cb_ops op,
                                  struct raft_instance *ri,
                                  struct lreg_value *lv)
{
    if (!lv || !ri)
        return -EINVAL;

    else if (lv->lrv_value_idx_in >= RAFT_LREG_MAX)
        return -ERANGE;

    int rc = 0;
#if 0
    bool tmp_bool = false;
#endif

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NODE_INFO: // fall through
    case LREG_NODE_CB_OP_INSTALL_NODE:  // fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        rc = -EOPNOTSUPP;
        break;

    case LREG_NODE_CB_OP_READ_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case RAFT_LREG_RAFT_UUID:
            lreg_value_fill_string(lv, "raft-uuid", ri->ri_raft_uuid_str);
            break;
        case RAFT_LREG_PEER_UUID:
            lreg_value_fill_string(lv, "peer-uuid", ri->ri_this_peer_uuid_str);
            break;
        case RAFT_LREG_VOTED_FOR_UUID:
            lreg_value_fill_string_uuid(lv, "voted-for-uuid",
                                        ri->ri_log_hdr.rlh_voted_for);
            break;
        case RAFT_LREG_LEADER_UUID:
            if (ri->ri_csn_leader)
                lreg_value_fill_string_uuid(lv, "leader-uuid",
                                            ri->ri_csn_leader->csn_uuid);
            else
                lreg_value_fill_string(lv, "leader-uuid", NULL);
            break;
        case RAFT_LREG_PEER_STATE:
            lreg_value_fill_string(lv, "state",
                                   raft_server_state_to_string(ri->ri_state));
            break;
        case RAFT_LREG_FOLLOWER_REASON:
            lreg_value_fill_string(
                lv, "follower-reason",
                (raft_instance_is_candidate(ri) ||
                 raft_instance_is_leader(ri)) ? "none" :
                raft_follower_reason_2_str(ri->ri_follower_reason));
            break;
        case RAFT_LREG_CLIENT_REQUESTS:
            lreg_value_fill_string(
                lv, "client-requests",
                raft_server_may_accept_client_request_reason(ri));
            break;
        case RAFT_LREG_TERM:
            lreg_value_fill_signed(lv, "term", ri->ri_log_hdr.rlh_term);
            break;
        case RAFT_LREG_COMMIT_IDX:
            lreg_value_fill_signed(lv, "commit-idx", ri->ri_commit_idx);
            break;
        case RAFT_LREG_LAST_APPLIED:
            lreg_value_fill_signed(lv, "last-applied",
                                   ri->ri_last_applied_idx);
            break;
        case RAFT_LREG_LAST_APPLIED_CCRC:
            lreg_value_fill_signed(lv, "last-applied-cumulative-crc",
                                   ri->ri_last_applied_cumulative_crc);
            break;
        case RAFT_LREG_NEWEST_ENTRY_IDX:
            lreg_value_fill_signed(
                lv, "newest-entry-idx",
                raft_server_get_current_raft_entry_index(ri));
            break;
        case RAFT_LREG_NEWEST_ENTRY_TERM:
            lreg_value_fill_signed(
                lv, "newest-entry-term",
                raft_server_get_current_raft_entry_term(ri));
            break;
        case RAFT_LREG_NEWEST_ENTRY_SIZE:
            lreg_value_fill_unsigned(lv, "newest-entry-data-size",
                                     ri->ri_newest_entry_hdr.reh_data_size);
            break;
        case RAFT_LREG_NEWEST_ENTRY_CRC:
            lreg_value_fill_unsigned(lv, "newest-entry-crc",
                                     ri->ri_newest_entry_hdr.reh_crc);
            break;
#if 0
        case RAFT_LREG_IGNORE_TIMER_EVENTS:
            lreg_value_fill_bool(lv, "ignore_timer_events",
                                 ri->ri_ignore_timerfd ? true : false);
            break;
#endif
        case RAFT_LREG_HIST_COMMIT_LAT:
            lreg_value_fill_object(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC),
                RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC);
            break;
        case RAFT_LREG_HIST_READ_LAT:
            lreg_value_fill_object(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_READ_LAT_MSEC),
                RAFT_INSTANCE_HIST_READ_LAT_MSEC);
            break;
        case RAFT_LREG_HIST_DEV_READ_LAT:
            lreg_value_fill_object(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC),
                RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC);
            break;
        case RAFT_LREG_HIST_DEV_WRITE_LAT:
            lreg_value_fill_object(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC),
                RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC);
            break;
        case RAFT_LREG_FOLLOWER_VSTATS:
            lreg_value_fill_varray(lv, "follower-vstats",
                                   LREG_USER_TYPE_RAFT_PEER_STATS,
                                   raft_num_members_validate_and_get(ri) - 1,
                                   raft_instance_lreg_peer_vstats_cb);
            break;
        default:
            break;
        }
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
#if 0
        if (lv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lv), &tmp_bool);
        if (rc)
            return rc;
#endif
        switch (lv->lrv_value_idx_in)
        {
        default:
            rc = -EPERM;
            break;
        }
    }

    return rc;
}

enum raft_peer_stats_items
{
    RAFT_PEER_STATS_ITEM_UUID,
//    RAFT_PEER_STATS_LAST_SEND,
    RAFT_PEER_STATS_LAST_ACK,
#if 0
//    RAFT_PEER_STATS_BYTES_SENT,
//    RAFT_PEER_STATS_BYTES_RECV,
#endif
    RAFT_PEER_STATS_PREV_LOG_IDX,
    RAFT_PEER_STATS_PREV_LOG_TERM,
    RAFT_PEER_STATS_MAX,
};

static util_thread_ctx_reg_t
raft_instance_lreg_peer_stats_multi_facet_handler(
    enum lreg_node_cb_ops op,
    const struct raft_instance *ri,
    const raft_peer_t peer,
    struct lreg_value *lv)
{
    if (!lv ||
        lv->lrv_value_idx_in >= RAFT_PEER_STATS_MAX ||
        op != LREG_NODE_CB_OP_READ_VAL)
        return;

    const struct raft_follower_info *rfi =
        raft_server_get_follower_info((struct raft_instance *)ri, peer);

    NIOVA_ASSERT(raft_member_idx_is_valid(ri, peer) &&
                 ri->ri_csn_raft_peers[peer]);

    switch (lv->lrv_value_idx_in)
    {
    case RAFT_PEER_STATS_ITEM_UUID:
        lreg_value_fill_string_uuid(lv, "peer-uuid",
                                    ri->ri_csn_raft_peers[peer]->csn_uuid);
        break;
#if 0
    case RAFT_PEER_STATS_LAST_SEND:
        lreg_value_fill_unsigned(lv, "last-send",
                                 ri->ri_last_send[peer].tv_sec);
        break;
#endif
    case RAFT_PEER_STATS_LAST_ACK:
        lreg_value_fill_string_time(lv, "last-ack", rfi->rfi_last_ack.tv_sec);
        break;
#if 0
    case RAFT_PEER_STATS_BYTES_SENT:
        break;
    case RAFT_PEER_STATS_BYTES_RECV:
        break;
#endif
    case RAFT_PEER_STATS_PREV_LOG_IDX:
        lreg_value_fill_unsigned(lv, "next-idx", rfi->rfi_next_idx);
        break;
    case RAFT_PEER_STATS_PREV_LOG_TERM:
        lreg_value_fill_signed(lv, "prev-idx-term", rfi->rfi_prev_idx_term);
        break;
    default:
        break;
    }
}

static util_thread_ctx_reg_int_t
raft_instance_lreg_peer_vstats_cb(enum lreg_node_cb_ops op,
                                  struct lreg_node *lrn,
                                  struct lreg_value *lv)
{
    const struct raft_instance *ri = lrn->lrn_cb_arg;
    if (!ri || !ri->ri_csn_raft)
        return -EINVAL;

    NIOVA_ASSERT(lrn->lrn_vnode_child);

    if (lv)
        lv->get.lrv_num_keys_out =
            raft_instance_is_leader(ri) ? RAFT_PEER_STATS_MAX : 0;

    // This peer is not listed in the follower output.
    const raft_peer_t peer = lrn->lrn_lvd.lvd_index +
        (lrn->lrn_lvd.lvd_index >= raft_server_instance_self_idx(ri) ? 1 : 0);

    if (!raft_member_idx_is_valid(ri, peer))
    {
        SIMPLE_LOG_MSG(LL_ERROR, "invalid peer index passed to raft_instance_lreg_peer_vstats_cb: %d", peer)
        return -EINVAL;
    }

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;
        strncpy(lv->lrv_key_string, "follower-stats",
                LREG_VALUE_STRING_MAX);
        strncpy(LREG_VALUE_TO_OUT_STR(lv), ri->ri_raft_uuid_str,
                LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        if (!lv)
            return -EINVAL;

        raft_instance_lreg_peer_stats_multi_facet_handler(op, ri, peer, lv);
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
        return -ENOENT;
    }

    return 0;
}

static util_thread_ctx_reg_int_t
raft_instance_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                      struct lreg_value *lv)
{
    struct raft_instance *ri = lrn->lrn_cb_arg;
    if (!ri)
        return -EINVAL;

    int rc = 0;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;

        lv->get.lrv_num_keys_out = (raft_instance_is_leader(ri) ?
                                    RAFT_LREG_MAX : RAFT_LREG_MAX_FOLLOWER);

        strncpy(lv->lrv_key_string, "raft_instance", LREG_VALUE_STRING_MAX);
        strncpy(LREG_VALUE_TO_OUT_STR(lv), ri->ri_raft_uuid_str,
                LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        rc = lv ? raft_instance_lreg_multi_facet_cb(op, ri, lv) : -EINVAL;
        break;

    case LREG_NODE_CB_OP_INSTALL_NODE: //fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
        rc = -ENOENT;
        break;
    }

    return rc;
}

/**
 * raft_server_entry_calc_crc - calculate the provided entry's crc and return
 *    the result without storing the crc in the entry.
 */
static crc32_t
raft_server_entry_calc_crc(const struct raft_entry *re)
{
    NIOVA_ASSERT(re);

    const struct raft_entry_header *rh = &re->re_header;
    const size_t offset =
        offsetof(struct raft_entry_header, reh_data_size);
    const unsigned char *buf = (const unsigned char *)re + offset;
    const int crc_len = sizeof(struct raft_entry) + rh->reh_data_size - offset;
    NIOVA_ASSERT(crc_len >= 0);

    crc32_t crc = crc_pcl(buf, crc_len, 0);

    DBG_RAFT_ENTRY(((rh->reh_crc && crc != rh->reh_crc) ? LL_WARN : LL_DEBUG),
                   &re->re_header, "calculated crc=%u", crc);

    return crc;
}

/**
 * raft_server_entry_check_crc - call raft_server_entry_calc_crc() and compare
 *    the result with that in the provided raft_entry.
 */
int
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
 * @re_idx:  the raft-entry index at which the block will be stored
 * @current_term:  the term to which this pending write operation belongs
 * @self_uuid:  UUID is this node instance, written into the entry for safety
 * @raft_uuid:  UUID of the raft instance, also written for safety
 * @data:  application data which is being stored in the block.
 * @len:  length of the application data
 */
void
raft_server_entry_init(const struct raft_instance *ri,
                       struct raft_entry *re, const raft_entry_idx_t re_idx,
                       const uint64_t current_term,
                       const char *data, const size_t len,
                       enum raft_write_entry_opts opts)
{
    NIOVA_ASSERT(re);
    NIOVA_ASSERT(opts == RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER ||
                 (data && len));

    if (opts == RAFT_WR_ENTRY_OPT_LOG_HEADER)
    {
        NIOVA_ASSERT(re_idx < 0);
    }
    else
    {
        NIOVA_ASSERT(re_idx >= 0);
    }

    // Should have been checked already
    NIOVA_ASSERT(len <= RAFT_ENTRY_MAX_DATA_SIZE);

    struct raft_entry_header *reh = &re->re_header;

    reh->reh_magic = RAFT_ENTRY_MAGIC;
    reh->reh_data_size = len;
    reh->reh_index = re_idx;
    reh->reh_term = current_term;
    reh->reh_leader_change_marker =
        (opts == RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER) ? 1 : 0;
    reh->reh_crc = 0;

    uuid_copy(reh->reh_self_uuid, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(reh->reh_raft_uuid, RAFT_INSTANCE_2_RAFT_UUID(ri));

    memset(reh->reh_pad, 0, RAFT_ENTRY_PAD_SIZE);

    memcpy(re->re_data, data, len);

    // Checksum the entire entry - including the 'data' section
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
    if  (reh->reh_index < 0)
        return;  // ignore log blocks

    ri->ri_newest_entry_hdr = *reh;

    DBG_RAFT_ENTRY(LL_TRACE, &ri->ri_newest_entry_hdr, "");
    DBG_RAFT_INSTANCE(LL_TRACE, ri, "");
}

static void
raft_server_entry_write_by_store(struct raft_instance *ri,
                                 const struct raft_entry *re)
{
    struct timespec io_op[2];
    niova_unstable_clock(&io_op[0]);

    ri->ri_backend->rib_entry_write(ri, re);

    niova_unstable_clock(&io_op[1]);

    struct binary_hist *bh =
        &ri->ri_rihs[RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC].rihs_bh;

    const long long elapsed_usec =
        (long long)(timespec_2_usec(&io_op[1]) - timespec_2_usec(&io_op[0]));

    if (elapsed_usec > 0)
        binary_hist_incorporate_val(bh, elapsed_usec);
}

/**
 * raft_server_entry_write - safely store an entry into the raft log at the
 *    specified index.  This function writes and syncs the data to the
 *    underlying device and handles partial writes.  NOTE:  it's critical that
 *    the ri_log_hdr is up-to-date with the correct term prior to calling
 *    this function.
 * @ri:  raft instance
 * @re_idx:  the raft_entry index at which the block will be written
 * @data:  the application data buffer
 * @len:  length of the application data buffer.
 */
static int
raft_server_entry_write(struct raft_instance *ri,
                        const raft_entry_idx_t re_idx,
                        const int64_t term, const char *data, size_t len,
                        enum raft_write_entry_opts opts)
{
    if (!ri || !ri->ri_csn_this_peer || !ri->ri_csn_raft ||
        (opts != RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER && (!data || !len)))
        return -EINVAL;

    else if (len > RAFT_ENTRY_MAX_DATA_SIZE)
        return -E2BIG;

    const size_t total_entry_size = sizeof(struct raft_entry) + len;

    struct raft_entry *re = niova_malloc(total_entry_size);
    if (!re)
        return -ENOMEM;

    raft_server_entry_init(ri, re, re_idx, term, data, len, opts);

    DBG_RAFT_ENTRY(LL_NOTIFY, &re->re_header, "");

    /* Failues of the next set of operations will be fatal:
     * - Ensuring that the index increases by one and term is not decreasing
     * - The entire block was written without error
     * - The block log fd was sync'd without error.
     */
    DBG_RAFT_INSTANCE_FATAL_IF(
        (!raft_server_entry_next_entry_is_valid(ri, &re->re_header)), ri,
        "raft_server_entry_next_entry_is_valid() failed");

    raft_server_entry_write_by_store(ri, re);

    /* Following the successful writing and sync of the entry, copy the
     * header contents into the raft instance.   Note, this is a noop if the
     * entry is for a log header.
     */
    raft_instance_update_newest_entry_hdr(ri, &re->re_header);

    niova_free(re);

    return 0;
}

// May be used by backends to prepare a header block
void
raft_server_entry_init_for_log_header(const struct raft_instance *ri,
                                      struct raft_entry *re,
                                      const raft_entry_idx_t re_idx,
                                      const uint64_t current_term,
                                      const char *data, const size_t len)
{
    NIOVA_ASSERT(re_idx < 0);

    return raft_server_entry_init(ri, re, re_idx, current_term, data, len,
                                  RAFT_WR_ENTRY_OPT_LOG_HEADER);
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
                           const raft_entry_idx_t expected_reh_idx)
{
    NIOVA_ASSERT(ri && rh && ri->ri_csn_this_peer && ri->ri_csn_raft);

    // Validate magic and data size.
    if (rh->reh_magic != RAFT_ENTRY_MAGIC ||
        rh->reh_data_size > RAFT_ENTRY_MAX_DATA_SIZE)
        return -EINVAL;

    // reh_index should be the same as the expected index.
    if (rh->reh_index != expected_reh_idx)
        return -EBADSLT;

    // Verify that header UUIDs match those of this raft instance.
    if (uuid_compare(rh->reh_self_uuid, RAFT_INSTANCE_2_SELF_UUID(ri)) ||
        uuid_compare(rh->reh_raft_uuid, RAFT_INSTANCE_2_RAFT_UUID(ri)))
        return -EKEYREJECTED;

    return 0;
}

static void
raft_server_entry_read_by_store(struct raft_instance *ri, struct raft_entry *re)
{
    NIOVA_ASSERT(ri && re && re->re_header.reh_index >= 0);

    struct timespec io_op[2];
    niova_unstable_clock(&io_op[0]);

    ssize_t read_sz = ri->ri_backend->rib_entry_read(ri, re);

    DBG_RAFT_ENTRY_FATAL_IF((read_sz != raft_server_entry_to_total_size(re)),
                            &re->re_header,
                            "invalid read size rrc=%zu, expected %zu",
                            read_sz, raft_server_entry_to_total_size(re));

    niova_unstable_clock(&io_op[1]);

    struct binary_hist *bh =
        &ri->ri_rihs[RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC].rihs_bh;

    const long long elapsed_usec =
        (long long)(timespec_2_usec(&io_op[1]) - timespec_2_usec(&io_op[0]));

    if (elapsed_usec > 0)
        binary_hist_incorporate_val(bh, elapsed_usec);

    DBG_RAFT_ENTRY(LL_DEBUG, &re->re_header, "sz=%zu usec=%lld",
                   raft_server_entry_to_total_size(re), elapsed_usec);
}

/**
 * raft_server_entry_read - request a read of a raft log entry.
 * @ri:  raft instance pointer
 * @re_idx: raft entry index
 * @data:  sink buffer
 * @len:  size of the sink buffer
 * @rc_len:  the data size of this entry
 */
static int
raft_server_entry_read(struct raft_instance *ri, const raft_entry_idx_t re_idx,
                       char *data, const size_t len, size_t *rc_len)
{
    if (!ri || !data || len > RAFT_ENTRY_SIZE)
        return -EINVAL;

    const size_t total_entry_size = sizeof(struct raft_entry) + len;

    struct raft_entry *re = niova_malloc(total_entry_size);
    if (!re)
        return -ENOMEM;

    struct raft_entry_header *rh = &re->re_header;

    // Set the necessary header fields -- Xxx this should allow for a previously
    //                                        read header to be supplied as an arg
    rh->reh_data_size = len;
    rh->reh_index = re_idx;

    raft_server_entry_read_by_store(ri, re);

    int rc = read_server_entry_validate(ri, rh, re_idx);
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
                /* Xxx at some point this can be removed if the CRC is managed
                 *     separately for the header and entry
                 */
                memcpy(data, re->re_data, len);
        }
    }

    //Xxx this malloc can be removed as well..
    niova_free(re);

    return rc;
}

/**
 * raft_server_entry_header_read - read only a raft log entry's header, the
 *    application contents of the entry are ignored and the crc is not taken.
 * @ri:  raft instance pointer
 * @reh:  the destination entry header buffer
 * @reh_index:  logical raft entry to read
 */
static int
raft_server_entry_header_read_by_store(struct raft_instance *ri,
                                       struct raft_entry_header *reh,
                                       raft_entry_idx_t reh_index)
{
    if (!ri || !reh || reh_index < 0)
        return -EINVAL;

    else if (!raft_instance_is_booting(ri) &&
             raft_server_get_current_raft_entry_index(ri) < reh_index)
        return -ERANGE;

    reh->reh_index = reh_index;

    int rc = ri->ri_backend->rib_entry_header_read(ri, reh);

    FATAL_IF((rc), "rib_entry_header_read(): %s", strerror(-rc));

    DBG_RAFT_ENTRY(LL_DEBUG, reh, "");

    return read_server_entry_validate(ri, reh, reh_index);
}

static int
raft_server_header_load(struct raft_instance *ri)
{
    return ri->ri_backend->rib_header_load(ri);
}

static void
raft_server_log_header_write_prep(struct raft_instance *ri,
                                  const uuid_t candidate,
                                  const int64_t candidate_term)
{
    DBG_RAFT_INSTANCE_FATAL_IF((!uuid_is_null(candidate) &&
                                ri->ri_log_hdr.rlh_term > candidate_term),
                               ri, "invalid candidate term=%ld",
                               candidate_term);

    /* rlh_seqno is not used for the raft protocol.  It's used to bounce
     * between the different header blocks so that in the case of a partial
     * write, at least one header block remains valid.
     */
    ri->ri_log_hdr.rlh_seqno++;
    ri->ri_log_hdr.rlh_magic = RAFT_HEADER_MAGIC;
    ri->ri_log_hdr.rlh_term = candidate_term;
    uuid_copy(ri->ri_log_hdr.rlh_voted_for, candidate);
}

static int
raft_server_log_header_write(struct raft_instance *ri,
                             const uuid_t candidate, int64_t candidate_term)
{
    if (!ri || !ri->ri_csn_raft)
        return -EINVAL;

    raft_server_log_header_write_prep(ri, candidate, candidate_term);

    return ri->ri_backend->rib_header_write(ri);
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
raft_server_backend_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    switch (ri->ri_store_type)
    {
    case RAFT_INSTANCE_STORE_POSIX_FLAT_FILE:
        raft_server_backend_use_posix(ri);
        break;

    case RAFT_INSTANCE_STORE_ROCKSDB:
        raft_server_backend_use_rocksdb(ri);
        break;

    default:
        SIMPLE_LOG_MSG(LL_FATAL, "invalid store type %d", ri->ri_store_type);
        break;
    }

    int rc = raft_server_log_file_name_setup(ri);
    if (rc)
        return rc;

    SIMPLE_LOG_MSG(LL_NOTIFY, "log-file=%s", ri->ri_log);

    return ri->ri_backend->rib_backend_setup(ri);
}

static void
raft_instance_initialize_newest_entry_hdr(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    memset(&ri->ri_newest_entry_hdr, 0, sizeof(struct raft_entry_header));
}

/**
 * raft_server_entry_next_entry_is_valid - this function is used when a caller
 *    wants to verify that an entry header correctly falls into the raft log
 *    sequence.  The function compares the prospective header with the known
 *    newest log header, ri->ri_newest_entry_hdr.
 * @ri:  raft instance
 * @next_reh:  the raft entry header being validated
 */
static bool
raft_server_entry_next_entry_is_valid(const struct raft_instance *ri,
                                      const struct raft_entry_header *next_reh)
{
    NIOVA_ASSERT(ri && next_reh);

    if (next_reh->reh_index < 0)
        return true;

    /* A null UUID means ri_newest_entry_hdr is uninitialized, otherwise,
     * the expected index is the 'newest' + 1.
     */
    const int64_t expected_raft_index =
        raft_server_get_current_raft_entry_index(ri) + 1;

    /* The index must increase by '1' and the term must never decrease.
     */
    if (next_reh->reh_index != expected_raft_index ||
        next_reh->reh_term < raft_server_get_current_raft_entry_term(ri))
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

    raft_instance_initialize_newest_entry_hdr(ri);

    const raft_entry_idx_t num_entries = ri->ri_entries_detected_at_startup;
    if (!num_entries)
        return 0;

    else if (num_entries < 0)
        return (int)num_entries;

    struct raft_entry_header reh;

    for (raft_entry_idx_t i = 0; i < num_entries; i++)
    {
        int rc = raft_server_entry_header_read_by_store(ri, &reh, i);

        DBG_RAFT_ENTRY(LL_DEBUG, &reh, "i=%lx rc=%d", i, rc);

        if (rc)
        {
            DBG_RAFT_ENTRY(LL_WARN, &reh,
                           "raft_server_entry_header_read_by_store():  %s",
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
    }

    return 0;
}

/**
 * raft_server_log_truncate - prune the log to the point after which the last
 *    "valid" entry has been found.  The contents of ri_newest_entry_hdr
 *    determine index of the last valid entry.  Note that this function will
 *    abort if there is an I/O error.
 */
static void //raft_server_udp_cb_ctx_int_t
raft_server_log_truncate(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    const raft_entry_idx_t trunc_entry_idx =
        raft_server_get_current_raft_entry_index(ri) + 1;

    NIOVA_ASSERT(trunc_entry_idx >= 0);

    ri->ri_backend->rib_log_truncate(ri, trunc_entry_idx);

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "new-max-raft-idx=%ld",
                      raft_server_get_current_raft_entry_index(ri));
}

/**
 * raft_server_log_load - read in the header blocks and scan the entry blocks
 *    to find the latest entry, checking for validity along the way.  After
 *    scanning the log entries, one by one, raft_server_log_load() will
 *    truncate any log space which may exist beyond highest validated log
 *    block.  raft_server_log_load() ensures that ri->ri_newest_entry_hdr
 *    contains the last written, valid block header.
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

    raft_server_log_truncate(ri);

    return 0;
}

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
                      timespec_2_msec(&its.it_value));

    int rc = timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);
    if (rc)
    {
        rc = -errno;
        DBG_RAFT_INSTANCE(LL_FATAL, ri, "timerfd_settime(): %s",
                          strerror(-rc));
    }
}

static int
raft_server_send_msg_to_client_tcp(struct raft_instance *ri,
                                   struct raft_net_client_request *rncr)
{
    if (!ri || !rncr || !rncr->rncr_reply ||
        raft_net_sockaddr_is_valid(&rncr->rncr_remote_addr)) {
        SIMPLE_LOG_MSG(LL_NOTIFY, "invalid rncr");
        return -EINVAL;
    }

    struct ctl_svc_node *csn;
    // XXX commit type doesn't have rncr_request, but only user of this fn references rncr_request so is ok?
    int rc = ctl_svc_node_lookup(rncr->rncr_request->rcrm_sender_id, &csn);
    if (rc || !csn) {
        SIMPLE_LOG_MSG(LL_NOTIFY, "csn not found");
        return -EINVAL;
    }

    const ssize_t msg_size = (sizeof(struct raft_client_rpc_msg) +
                              rncr->rncr_reply->rcrm_data_size);

    if (msg_size > RAFT_NET_MAX_TCP_RPC_SIZE) {
        SIMPLE_LOG_MSG(LL_NOTIFY, "msg too big");
        return -E2BIG;
    }

    struct raft_net_connection *rnc = csn->csn_peer.csnp_net_data;
    NIOVA_ASSERT(rnc);

    struct iovec iov[1] = {
        [0].iov_len = msg_size,
        [0].iov_base = rncr->rncr_reply,
    };

    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_server_send_msg_to_client_tcp()");
    ssize_t size_rc;
    if (net_ctl_can_send(&rncr->rncr_nc))
    {
        size_rc = tcp_socket_send(&rnc->rnc_tsh, iov, 1);
    }
    else
    {
        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, rncr->rncr_reply,
                            &rncr->rncr_remote_addr,
                            "send to this client UUID is disabled");
        size_rc = msg_size;
    }

    return size_rc == msg_size ? 0 : -ECOMM;
}

static int
raft_server_send_msg_to_client_udp(struct raft_instance *ri,
                               struct raft_net_client_request *rncr)
{
    if (!ri || !rncr || !rncr->rncr_reply ||
        raft_net_sockaddr_is_valid(&rncr->rncr_remote_addr))
        return -EINVAL;

    const ssize_t msg_size = (sizeof(struct raft_client_rpc_msg) +
                              rncr->rncr_reply->rcrm_data_size);

    if (msg_size > RAFT_NET_MAX_RPC_SIZE)
        return -E2BIG;

    struct udp_socket_handle *ush = &ri->ri_ush[RAFT_UDP_LISTEN_CLIENT];

    struct iovec iov[1] = {
        [0].iov_len = msg_size,
        [0].iov_base = rncr->rncr_reply,
    };

    ssize_t size_rc;
    if (net_ctl_can_send(&rncr->rncr_nc))
    {
        size_rc = udp_socket_send(ush, iov, 1, &rncr->rncr_remote_addr);
    }
    else
    {
        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, rncr->rncr_reply,
                            &rncr->rncr_remote_addr,
                            "send to this client UUID is disabled");
        size_rc = msg_size;
    }

    return size_rc == msg_size ? 0 : -ECOMM;
}

static int
raft_server_send_msg_to_client(struct raft_instance *ri,
                               struct raft_net_client_request *rncr)
{
    if (ri->ri_net_type == RAFT_INSTANCE_NET_UDP)
        return raft_server_send_msg_to_client_udp(ri, rncr);
    else
        return raft_server_send_msg_to_client_tcp(ri, rncr);
}

static int
raft_server_send_msg_tcp(struct raft_instance *ri,
                         struct ctl_svc_node *rp, const struct raft_rpc_msg *rrm)
{
    SIMPLE_LOG_MSG(LL_NOTIFY, "raft_server_send_msg_tcp()");
    if (!ri || !rp || !rrm || !ctl_svc_node_is_peer(rp))
        return -EINVAL;

    /* For now, there's no use of multiple-IOVs since the only msg type
     * requiring a payload is RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST.
     */
#if 0 //XXX reinvestigate!
    const size_t msg_size = sizeof(struct raft_rpc_msg) +
        (rrm->rrm_type == RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST ?
         rrm->rrm_append_entries_request.raerqm_entries_sz : 0);
#else
    size_t msg_size = sizeof(struct raft_rpc_msg);
    if (rrm->rrm_type == RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST)
        msg_size += rrm->rrm_append_entries_request.raerqm_entries_sz;

#endif
    if (msg_size > RAFT_NET_MAX_TCP_RPC_SIZE)
        return -E2BIG;

    struct raft_net_connection *rnc = raft_net_remote_connect(ri, rp);
    if (!rnc)
    {
        return -ECOMM;
    }

    struct iovec iov = {
        .iov_len = msg_size,
        .iov_base = (void *)rrm
    };

    // The csn is checked to determine if send has been disabled for testing.
    ssize_t size_rc;
    if (!net_ctl_can_send(&rp->csn_peer.csnp_net_ctl))
    {
        DBG_CTL_SVC_NODE(LL_DEBUG, rp, "net_ctl_can_send() is false");
        size_rc = msg_size;
    }
    else
    {
        size_rc = tcp_socket_send(&rnc->rnc_tsh, &iov, 1);
    }

    if (size_rc < 0) // Return with system error here
        return size_rc;

    DBG_RAFT_MSG(LL_DEBUG, rrm, "");

    if (rp->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER)
        raft_net_update_last_comm_time(ri, rp->csn_uuid, true);

    // Error if expected size was not produced
    return size_rc == msg_size ? 0 : -ECOMM;
}

static int
raft_server_send_msg_udp(struct raft_instance *ri,
                         const enum raft_udp_listen_sockets sock_src,
                         struct ctl_svc_node *rp, const struct raft_rpc_msg *rrm)
{
    if (!ri || !rp || !rrm || !ctl_svc_node_is_peer(rp) ||
        (sock_src != RAFT_UDP_LISTEN_SERVER &&
         sock_src != RAFT_UDP_LISTEN_CLIENT))
        return -EINVAL;

    /* For now, there's no use of multiple-IOVs since the only msg type
     * requiring a payload is RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST.
     */
#if 0 //XXX reinvestigate!
    const size_t msg_size = sizeof(struct raft_rpc_msg) +
        (rrm->rrm_type == RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST ?
         rrm->rrm_append_entries_request.raerqm_entries_sz : 0);
#else
    size_t msg_size = sizeof(struct raft_rpc_msg);
    if (rrm->rrm_type == RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST)
        msg_size += rrm->rrm_append_entries_request.raerqm_entries_sz;

#endif
    if (msg_size > RAFT_NET_MAX_RPC_SIZE)
        return -E2BIG;

    struct udp_socket_handle *ush = &ri->ri_ush[sock_src];

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
        .iov_len = msg_size,
        .iov_base = (void *)rrm
    };

    // The csn is checked to determine if send has been disabled for testing.
    ssize_t size_rc;
    if (!net_ctl_can_send(&rp->csn_peer.csnp_net_ctl))
    {
        DBG_CTL_SVC_NODE(LL_DEBUG, rp, "net_ctl_can_send() is false");
        size_rc = msg_size;
    }
    else
    {
        size_rc = udp_socket_send(ush, &iov, 1, &dest);
    }

    if (size_rc < 0) // Return with system error here
        return size_rc;

    DBG_RAFT_MSG(LL_DEBUG, rrm, "");

    if (sock_src == RAFT_UDP_LISTEN_SERVER)
        raft_net_update_last_comm_time(ri, rp->csn_uuid, true);

    // Error if expected size was not produced
    return size_rc == msg_size ? 0 : -ECOMM;
}

static int
raft_server_send_msg(struct raft_instance *ri,
                         const enum raft_udp_listen_sockets sock_src,
                         struct ctl_svc_node *rp, const struct raft_rpc_msg *rrm)
{
    if (rp->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER) {
        NIOVA_ASSERT(sock_src == RAFT_UDP_LISTEN_SERVER);
    }
    else
    {
        NIOVA_ASSERT(sock_src == RAFT_UDP_LISTEN_CLIENT);
    }

    if (ri->ri_net_type == RAFT_INSTANCE_NET_UDP)
        return raft_server_send_msg_udp(ri, sock_src, rp, rrm);
    else
        return raft_server_send_msg_tcp(ri, rp, rrm);
}


static void
raft_server_broadcast_msg(struct raft_instance *ri,
                          const struct raft_rpc_msg *rrm)
{
    const raft_peer_t num_peers = raft_num_members_validate_and_get(ri);

    for (int i = 0; i < num_peers; i++)
    {
        struct ctl_svc_node *rp = ri->ri_csn_raft_peers[i];

        if (rp == ri->ri_csn_this_peer)
            continue;

        int rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, rp, rrm);
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_server_send_msg(): %d", rc);

/*      XXX if node refuses connection, what should we do?
        DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                                   "raft_server_send_msg(): %s", strerror(rc));
                                   */
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
                               ri, "candidate_term=%ld", candidate_term);

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

    const raft_peer_t npeers = raft_num_members_validate_and_get(ri);

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
                          "!candidate OR candidate-term (%ld) != ht",
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
                               "rcs->rcs_term (%ld) != ri_log_hdr",
                               rcs->rcs_term);

    rcs->rcs_results[peer_idx] = result;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "peer-idx=%hhu voted=%s",
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
        .rrm_vote_request.rvrqm_proposed_term = ri->ri_log_hdr.rlh_term,
        .rrm_vote_request.rvrqm_last_log_term =
            raft_server_get_current_raft_entry_term(ri),
        .rrm_vote_request.rvrqm_last_log_index =
            raft_server_get_current_raft_entry_index(ri),
    };

    uuid_copy(rrm.rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(rrm.rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");

    raft_server_broadcast_msg(ri, &rrm);
}

static void
raft_server_update_log_header(struct raft_instance *ri, int64_t new_term,
                              const uuid_t peer_with_newer_term)
{
    NIOVA_ASSERT(new_term > ri->ri_log_hdr.rlh_term);

    int rc = raft_server_log_header_write(ri, peer_with_newer_term, new_term);

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                               "raft_server_log_header_write() %s",
                               strerror(-rc));
}

static void
raft_server_try_update_log_header_null_voted_for_peer(struct raft_instance *ri,
                                                      int64_t new_term)
{
    if (ri->ri_log_hdr.rlh_term < new_term)
    {
        const uuid_t null_uuid = {0};

        raft_server_update_log_header(ri, new_term, null_uuid);
    }
}

/**
 * raft_server_becomes_follower - handle the transition from a
 *    a follower either from candidate or leader state.  This function sets
 *    the new term and sync's it to the header.  The peer-uuid is not set
 *    in the log header unless the caller specifies it so.  This is generally
 *    only done when the caller is raft_server_process_vote_reply().
 *    Otherwise, this function is typically called when the term changes
 *    elsewhere in the cluster and this node becomes a stale leader or
 *    candidate.
 * @ri:  raft instance
 * @new_term:  the higher term provided by a peer
 * @peer_with_newer_term:  peer uuid which provided the higher term
 * @reason:  the reason why this instance becomes a follower
 */
static void
raft_server_becomes_follower(struct raft_instance *ri,
                             int64_t new_term,
                             const uuid_t peer_with_newer_term,
                             enum raft_follower_reasons reason)
{
    NIOVA_ASSERT(ri);

    ri->ri_state = RAFT_STATE_FOLLOWER;
    ri->ri_follower_reason = reason;

    /* Generally, in raft we become a follower when a higher term is observed.
     * However when 2 or more peers become candidates for the same term, the
     * losing peer may only be notified of a successful election completion
     * when it recv's a AE RPC.
     */
    if (reason == RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE)
    {
        NIOVA_ASSERT(new_term >= ri->ri_log_hdr.rlh_term);
    }
    else
    {
        NIOVA_ASSERT(new_term > ri->ri_log_hdr.rlh_term);
    }

    DECLARE_AND_INIT_UUID_STR(peer_uuid_str, peer_with_newer_term);

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "sender-uuid=%s term=%ld rsn=%s",
                      peer_uuid_str, new_term,
                      raft_follower_reason_2_str(reason));

    // No need to sync the new term.
    if (new_term == ri->ri_log_hdr.rlh_term)
        return;

    /* Use a null uuid since we didn't actually vote for this leader.
     * Had we voted for this leader, the ri_log_hdr term would have been
     * in sync already.
     */
    const uuid_t null_uuid = {0};
    const bool sync_uuid =
        (reason == RAFT_BFRSN_VOTED_FOR_PEER) ? true : false;

    raft_server_update_log_header(ri, new_term,
                                  (sync_uuid ?
                                   peer_with_newer_term : null_uuid));
}

static bool
raft_leader_has_applied_txn_in_my_term(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    if (raft_instance_is_leader(ri))
    {
        const struct raft_leader_state *rls = &ri->ri_leader;

        DBG_RAFT_INSTANCE_FATAL_IF((rls->rls_leader_term !=
                                    ri->ri_log_hdr.rlh_term), ri,
                                   "leader-term=%ld != log-hdr-term",
                                   rls->rls_leader_term);

        return ri->ri_last_applied_idx > rls->rls_initial_term_idx ?
            true : false;
    }

    DBG_RAFT_INSTANCE(LL_WARN, ri, "not-leader");

    return false;
}

/**
 * raft_server_leader_init_state - setup the raft instance for leader duties.
 */
static raft_server_cb_ctx_t
raft_server_leader_init_state(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    ri->ri_state = RAFT_STATE_LEADER;

    struct raft_leader_state *rls = &ri->ri_leader;
    memset(rls, 0, sizeof(*rls));

    rls->rls_leader_term = ri->ri_log_hdr.rlh_term;

    const raft_peer_t num_raft_peers = raft_num_members_validate_and_get(ri);

    /* Stash the current raft-entry index.  In general, this leader should
     * place the block @(current-raft-entry-idx + 1).  When this next index
     * has been committed and APPLIED by this leader, or in other words, when
     * ri_commit_idx >= rls_initial_term_idx, then this leader can reply to
     * clients.
     */
    rls->rls_initial_term_idx = raft_server_get_current_raft_entry_index(ri);

    for (raft_peer_t i = 0; i < num_raft_peers; i++)
    {
        struct raft_follower_info *rfi = raft_server_get_follower_info(ri, i);

        rfi->rfi_next_idx = raft_server_get_current_raft_entry_index(ri) + 1;
        rfi->rfi_prev_idx_term = raft_server_get_current_raft_entry_term(ri);
        rfi->rfi_prev_idx_crc = raft_server_get_current_raft_entry_crc(ri);
        rfi->rfi_current_idx_term = -1;
        rfi->rfi_current_idx_crc = 0;
    }
}

static raft_net_cb_ctx_t
raft_server_write_next_entry(struct raft_instance *ri, const int64_t term,
                             const char *data, const size_t len,
                             enum raft_write_entry_opts opts)
{
    NIOVA_ASSERT(term >= raft_server_get_current_raft_entry_term(ri));

    const raft_entry_idx_t next_entry_idx =
        raft_server_get_current_raft_entry_index(ri) + 1;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri,
                      "next-entry-idx=%ld term=%ld len=%zd opts=%d",
                      next_entry_idx, term, len, opts);

    DBG_RAFT_INSTANCE_FATAL_IF((next_entry_idx < 0), ri,
                               "negative next-entry-idx=%ld", next_entry_idx);

    int rc = raft_server_entry_write(ri, next_entry_idx, term, data, len, opts);
    if (rc)
        DBG_RAFT_INSTANCE(LL_FATAL, ri, "raft_server_entry_write(): %s",
                          strerror(-rc));
}

static raft_net_cb_ctx_t
raft_server_leader_write_new_entry(struct raft_instance *ri,
                                   const char *data,
                                   const size_t len,
                                   enum raft_write_entry_opts opts)
{
#if 1
    NIOVA_ASSERT(raft_instance_is_leader(ri));
#else
    if (!raft_instance_is_leader(ri))
        return;
#endif

    /* The leader always appends to the end of its log so
     * ri->ri_log_hdr.rlh_term must be used.  This contrasts with recovering
     * followers which may not always be able to use the current term when
     * rebuilding their log.
     */
    raft_server_write_next_entry(ri, ri->ri_log_hdr.rlh_term, data, len, opts);

    // Schedule ourselves to send this entry to the other members.
    ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_AE_SEND]);
}

static raft_server_cb_leader_t
raft_server_write_leader_change_marker(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_leader(ri));

    raft_server_leader_write_new_entry(ri, NULL, 0,
                                       RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER);
}

static void
raft_server_set_leader_csn(struct raft_instance *ri,
                           struct ctl_svc_node *leader_csn);

static raft_server_cb_ctx_t
raft_server_candidate_becomes_leader(struct raft_instance *ri)
{
    DBG_RAFT_INSTANCE_FATAL_IF((!raft_server_candidate_is_viable(ri)), ri,
                               "!raft_server_candidate_is_viable()");

    raft_server_leader_init_state(ri);

    // Modify timer_fd timeout for the leader role.
    raft_server_timerfd_settime(ri);

    /* Deliver a "dummy" commit to the followers - we cannot respond to client
     * until this commit has been applied. -- what should the dummy app handler
     * look like and what should the entry and request msg look like?
     */
    raft_server_write_leader_change_marker(ri);

    raft_server_set_leader_csn(ri, ri->ri_csn_this_peer);

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");
}

/**
 * raft_server_process_vote_reply - handle a peer's response to our vote
 *    request.
 */
static raft_server_cb_ctx_t
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
        raft_server_becomes_follower(ri, vreply->rvrpm_term,
                                     rrm->rrm_sender_id,
                                     RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE);
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
                 raft_member_idx_is_valid(ri, follower));

    NIOVA_ASSERT(raft_instance_is_leader(ri));

    struct raft_follower_info *rfi =
        raft_server_get_follower_info(ri, follower);

    NIOVA_ASSERT(rfi->rfi_next_idx >= 0);

    // If the next_idx is '0' this means that no block have ever been written.
    if (rfi->rfi_next_idx == 0)
    {
        rfi->rfi_prev_idx_term = 0;
        rfi->rfi_current_idx_term = -1;
    }

    // Grab the current idx info if the follower is behind
    const int64_t my_raft_idx =
        raft_server_get_current_raft_entry_index(ri);

    const bool refresh_prev = rfi->rfi_prev_idx_term < 0 ? true : false;
#if 0
    const bool refresh_current =
        (my_raft_idx >= rfi->rfi_next_idx &&
         (refresh_prev || rfi->rfi_current_idx_term < 0)) ? true : false;
#else
    const bool refresh_current = my_raft_idx >= rfi->rfi_next_idx ? true : false;
#endif

    struct raft_entry_header reh = {0};

    if (refresh_prev)
    {
        const int64_t follower_prev_entry_idx = rfi->rfi_next_idx - 1;

        NIOVA_ASSERT(follower_prev_entry_idx >= -1);

        // Test that the follower's prev-idx is not ahead of this leader's idx
        NIOVA_ASSERT(follower_prev_entry_idx <=
                     raft_server_get_current_raft_entry_index(ri));

        int rc =
            raft_server_entry_header_read_by_store(ri, &reh, follower_prev_entry_idx);

        if (rc < 0)
            return rc;

        DBG_RAFT_ENTRY_FATAL_IF((reh.reh_term < 0), &reh,
                                "invalid reh.reh_term=%ld", reh.reh_term);

        rfi->rfi_prev_idx_term = reh.reh_term;
        rfi->rfi_prev_idx_crc = reh.reh_crc;
    }

    if (refresh_current)
    {
        NIOVA_ASSERT(my_raft_idx >= rfi->rfi_next_idx);

        int rc = raft_server_entry_header_read_by_store(ri, &reh, rfi->rfi_next_idx);
        DBG_RAFT_INSTANCE_FATAL_IF(
            (rc), ri, "raft_server_entry_header_read_by_store(%ld): %s",
            rfi->rfi_next_idx, strerror(-rc));

        rfi->rfi_current_idx_term = reh.reh_term;
        rfi->rfi_current_idx_crc = reh.reh_crc;
    }

    DBG_RAFT_INSTANCE(((refresh_prev || refresh_current) ?
                       LL_NOTIFY : LL_DEBUG), ri,
                      "peer=%hhx refresh=%d:%d pti=%ld:%ld ct=%ld ccrc=%lu",
                      follower, refresh_prev, refresh_current,
                      rfi->rfi_prev_idx_term, rfi->rfi_next_idx,
                      rfi->rfi_current_idx_term, rfi->rfi_current_idx_crc);

    return 0;
}

static raft_server_leader_mode_int64_t
raft_server_leader_get_current_term(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_leader(ri));
    NIOVA_ASSERT(ri->ri_leader.rls_leader_term == ri->ri_log_hdr.rlh_term);

    return ri->ri_log_hdr.rlh_term;
}

static raft_server_leader_mode_t
raft_server_leader_init_append_entry_msg(struct raft_instance *ri,
                                         struct raft_rpc_msg *rrm,
                                         const raft_peer_t follower,
                                         bool heartbeat)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft && rrm &&
                 raft_member_idx_is_valid(ri, follower));

    const struct raft_follower_info *rfi =
        raft_server_get_follower_info(ri, follower);

    rrm->rrm_type = RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST;
    rrm->rrm_version = 0;

    uuid_copy(rrm->rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(rrm->rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));

    struct raft_append_entries_request_msg *raerq =
        &rrm->rrm_append_entries_request;

    int rc = raft_server_refresh_follower_prev_log_term(ri, follower);

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                             "raft_server_refresh_follower_prev_log_term() %s",
                               strerror(-rc));

    raerq->raerqm_heartbeat_msg = heartbeat ? 1 : 0;

    raerq->raerqm_leader_term = raft_server_leader_get_current_term(ri);
    raerq->raerqm_commit_index = ri->ri_commit_idx;
    raerq->raerqm_log_term = rfi->rfi_current_idx_term;
    raerq->raerqm_this_idx_crc = rfi->rfi_current_idx_crc;
    raerq->raerqm_entries_sz = 0;
    raerq->raerqm_leader_change_marker = 0;

    // Previous log index is the address of the follower's last write.
    raerq->raerqm_prev_log_index = rfi->rfi_next_idx - 1;

    // OK to copy the rls_prev_idx_term[] since it was refreshed above.
    raerq->raerqm_prev_log_term = rfi->rfi_prev_idx_term;

    raerq->raerqm_prev_idx_crc = rfi->rfi_prev_idx_crc;
}

static raft_server_epoll_ae_sender_t
raft_server_append_entry_sender(struct raft_instance *ri, bool heartbeat);

static raft_net_timerfd_cb_ctx_t
raft_server_issue_heartbeat(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_this_peer);
    NIOVA_ASSERT(raft_instance_is_leader(ri));

    raft_server_append_entry_sender(ri, true);
}

static raft_net_timerfd_cb_ctx_t
raft_server_timerfd_cb(struct raft_instance *ri)
{
    FUNC_ENTRY(LL_TRACE);

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
static raft_server_cb_ctx_t
raft_server_process_vote_request(struct raft_instance *ri,
                                 struct ctl_svc_node *sender_csn,
                                 const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);

    // The caller *should* have already checked this.
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    const struct raft_vote_request_msg *vreq = &rrm->rrm_vote_request;

    struct raft_rpc_msg rreply_msg = {0};

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

    DBG_RAFT_MSG(LL_NOTIFY, rrm, "vote=%s my term=%ld last=%ld:%ld",
                 rreply_msg.rrm_vote_reply.rvrpm_voted_granted ? "yes" : "no",
                 ri->ri_log_hdr.rlh_term,
                 raft_server_get_current_raft_entry_term(ri),
                 raft_server_get_current_raft_entry_index(ri));

    /* We intend to vote 'yes' - sync the candidate's term and UUID to our
     * log header.
     */
    if (rreply_msg.rrm_vote_reply.rvrpm_voted_granted)
        raft_server_becomes_follower(ri, vreq->rvrqm_proposed_term,
                                     rrm->rrm_sender_id,
                                     RAFT_BFRSN_VOTED_FOR_PEER);

    /* Inform the candidate of our vote.
     */
    int rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, sender_csn,
                         &rreply_msg);

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_send_msg(): %s",
                               strerror(rc));
}

/**
 * raft_server_append_entry_check_already_stored - this function takes an
 *    AE request msg and reads the log to determine if the entry had been
 *    stored at an earlier time.  This function is called when the follower
 *    detects that its log index is > than the index value in the AE request.
 *    AFAICT, this situation can occur in two instances:  first, when the
 *    follower was either a deposed leader or follower of a deposed leader and
 *    it accepted entries which the new leader does not possess (rollback);
 *    or secondly, an old / retried / stale AE request arrives at this follower
 *    for an index which had already been written.
 */
static raft_server_cb_follower_ctx_bool_t
raft_server_append_entry_check_already_stored(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);
    NIOVA_ASSERT(raft_instance_is_follower(ri));

    // raerqm_prev_log_index can be -1 if no writes have ever been done.
    NIOVA_ASSERT(raerq->raerqm_prev_log_index >= RAFT_MIN_APPEND_ENTRY_IDX);

    const raft_entry_idx_t raft_current_idx =
        raft_server_get_current_raft_entry_index(ri);

    const raft_entry_idx_t leaders_next_idx_for_me = raerq->raerqm_prev_log_index + 1;

    // The condition for entering this function should have been checked prior.
    NIOVA_ASSERT(raft_current_idx >= leaders_next_idx_for_me);

    struct raft_entry_header reh = {0};

    /* In a corner-case, the leader's msg may be stale and contain contents
     * from a period where this peer and the leader were caught up.  As a
     * result, the current log_term would not exist and we should not read
     * this block.
     */
    if (raerq->raerqm_log_term > 0)
    {
        int rc = raft_server_entry_header_read_by_store(ri, &reh,
                                                        leaders_next_idx_for_me);
        FATAL_IF((rc), "raft_server_header_entry_read(): %s", strerror(-rc));

        if (reh.reh_term != raerq->raerqm_log_term)
            return false;

        FATAL_IF((raerq->raerqm_this_idx_crc != reh.reh_crc),
                 "crc (%u) does not match leader (%u) for idx=%ld",
                 reh.reh_crc, raerq->raerqm_this_idx_crc, leaders_next_idx_for_me);
    }
    else
    {
        DBG_RAFT_INSTANCE(
            LL_WARN, ri,
            "negative log-term %ld rci=%ld leader-prev-[idx:term]=%ld:%ld",
            raerq->raerqm_log_term, raft_current_idx,
            raerq->raerqm_prev_log_index, raerq->raerqm_prev_log_term);
    }
    /* Check raerq->raerqm_prev_log_term - this is more of a sanity check to
     * ensure that the verified idx, leaders_next_idx_for_me, proceeds a valid
     * term of the prev-idx.
     */
    if (raerq->raerqm_prev_log_index >= 0)
    {
        int rc = raft_server_entry_header_read_by_store(ri, &reh,
                                                        raerq->raerqm_prev_log_index);

        FATAL_IF((rc), "raft_server_entry_read(): %s", strerror(-rc));
        FATAL_IF((reh.reh_term != raerq->raerqm_prev_log_term),
                 "raerq->raerqm_prev_log_term=%ld != reh.reh_term=%ld",
                 raerq->raerqm_prev_log_term, reh.reh_term);
        FATAL_IF((raerq->raerqm_prev_idx_crc != reh.reh_crc),
                 "crc (%u) does not match leader (%u) for idx=%ld",
                 reh.reh_crc, raerq->raerqm_this_idx_crc,
                 raerq->raerqm_prev_log_index);
    }

    DBG_RAFT_INSTANCE(
        LL_DEBUG, ri,
        "already-stored=yes rci=%ld leader-prev-[idx:term]=%ld:%ld",
        raft_current_idx, raerq->raerqm_prev_log_index,
        raerq->raerqm_prev_log_term);

    return true;
}

/**
 * raft_server_append_entry_log_prune_if_needed - the local raft instance's
 *    log may need to be pruned if it extends beyond the prev_log_index
  *    presented by our leader.  Follower-ctx is assert here.
 */
static raft_server_cb_follower_ctx_t
raft_server_append_entry_log_prune_if_needed(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);
    NIOVA_ASSERT(raft_instance_is_follower(ri));
    // This value must have already been checked by the caller.
    NIOVA_ASSERT(raerq->raerqm_prev_log_index >= RAFT_MIN_APPEND_ENTRY_IDX);

    const int64_t raft_entry_idx_prune = raerq->raerqm_prev_log_index + 1;

    // We must not prune already committed transactions.
    DBG_RAFT_INSTANCE_FATAL_IF(
        (ri->ri_commit_idx >= raft_entry_idx_prune ||
         ri->ri_last_applied_idx >= raft_entry_idx_prune),
        ri, "cannot prune committed entry raerq-nli=%ld",
        raft_entry_idx_prune);

    if (raerq->raerqm_prev_log_index < 0)
    {
        raft_instance_initialize_newest_entry_hdr(ri);
    }
    else
    {
        struct raft_entry_header reh;

        /* Read the block at the leader's index and apply it to our header.
         * We don't call raft_server_entry_next_entry_is_valid() since the log
         * sequence had been verified already at startup.
         */
        int rc = raft_server_entry_header_read_by_store(ri, &reh,
                                                        raerq->raerqm_prev_log_index);
        FATAL_IF((rc), "raft_server_entry_header_read_by_store(): %s", strerror(-rc));

        raft_instance_update_newest_entry_hdr(ri, &reh);
    }

    // truncate the log.
    raft_server_log_truncate(ri);
}

/**
 * raft_server_append_entry_log_prepare_and_check - determine if the current
 *    append entry command can proceed to this follower's log.  This function
 *    returns two errors to the caller but in both cases the caller will
 *    reply to the leader with the "general" error 'non_matching_prev_term'.
 *    causing the leader to decrement its prev_log_index value for this
 *    follower and retry.  NOTE:  this function will truncate / prune the log
 *    according to the index value presented in the raerq.
 */
static raft_server_cb_follower_ctx_int_t
raft_server_append_entry_log_prepare_and_check(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);

    int64_t raft_current_idx = raft_server_get_current_raft_entry_index(ri);

    if (raft_current_idx > raerq->raerqm_prev_log_index)
    {
        /* If this follower's index is ahead of the leader's then we must check
         * for a retried AE which has already been stored in our log.
         * Note that this AE may have been delayed in the network or may have
         * retried due to a dropped reply.  It's important that we try to ACK
         * this request and not proceed with modifying our log.
         */
        if (raft_server_append_entry_check_already_stored(ri, raerq))
            return -EALREADY;

        else // Otherwise, the log needs to be pruned.
            raft_server_append_entry_log_prune_if_needed(ri, raerq);
    }

    // Re-obtain the current_idx, it may have changed if a prune occurred.
    raft_current_idx = raft_server_get_current_raft_entry_index(ri);

    // At this point, current_idx should not exceed the one from the leader.
    NIOVA_ASSERT(raft_current_idx <= raerq->raerqm_prev_log_index);

    /* In this case, the leader's and follower's indexes have yet to converge
     * which implies a "non_matching_prev_term" since the term isn't testable
     * until the indexes match.
     */
    int rc = 0;

    if (raft_current_idx < raerq->raerqm_prev_log_index)
        rc = -ERANGE;

    /* Equivalent log indexes but the terms do not match.  Note that this cond
     * will likely lead to more pruning as the leader continues to decrement
     * its raerqm_prev_log_index value for this follower.
     */
    else if (raft_server_get_current_raft_entry_term(ri) !=
             raerq->raerqm_prev_log_term)
        rc = -EEXIST;

    DBG_RAFT_INSTANCE((raerq->raerqm_heartbeat_msg ? LL_DEBUG : LL_NOTIFY), ri,
                      "rci=%ld leader-prev-[idx:term]=%ld:%ld rc=%d",
                      raft_current_idx,
                      raerq->raerqm_prev_log_index,
                      raerq->raerqm_prev_log_term, rc);

    return rc;
}

static void
raft_server_set_leader_csn(struct raft_instance *ri,
                           struct ctl_svc_node *leader_csn)
{
    NIOVA_ASSERT(ri && leader_csn);

    if (ri->ri_csn_leader != leader_csn)
    {
        ri->ri_csn_leader = leader_csn;
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "csn=%p", leader_csn);
    }
}

/**
 * raft_server_process_append_entries_term_check_ops - this function handles
 *    important details for the AE request.  It will return -ESTALE if the
 *    sender is not a valid leader (per term check).  Once the term has been
 *    validated, this function will take care of self-demotion (if this
 *    instance is a candidate) and will sync the term number to the log header
 *    if the provided term had not yet been seen.  Lastly, it will apply the
 *    csn pointer to the raft-instance if the leader is newly minted.
 * @ri:  raft instance
 * @sender_csn:  the ctl-svc-node for sender of the AE request.
 * @raerq:  contents of the AE message.
 */
static raft_server_cb_ctx_int_t
raft_server_process_append_entries_term_check_ops(
    struct raft_instance *ri,
    struct ctl_svc_node *sender_csn,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && sender_csn && raerq);

    // My term is newer which means this sender is a stale leader.
    if (ri->ri_log_hdr.rlh_term > raerq->raerqm_leader_term)
        return -ESTALE;

    // -- Sender's term is greater than or equal to my own --

    // Demote myself if candidate.
    if (ri->ri_state == RAFT_STATE_CANDIDATE)
        raft_server_becomes_follower(ri, raerq->raerqm_leader_term,
                                     sender_csn->csn_uuid,
                                     RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE);

    // Apply leader csn pointer.
    raft_server_set_leader_csn(ri, sender_csn);

    return 0;
}

/**
 * raft_server_write_new_entry_from_leader - the log write portion of the
 *    AE operation.  The log index is derived from the raft-instance which
 *    must match the index provided by the leader in raerq,
 */
static raft_server_cb_follower_ctx_t
raft_server_write_new_entry_from_leader(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);
    NIOVA_ASSERT(raft_instance_is_follower(ri));

    if (raerq->raerqm_heartbeat_msg)
        return; // This is a heartbeat msg which does not enter the log

    NIOVA_ASSERT(raerq->raerqm_log_term > 0);
    NIOVA_ASSERT(raerq->raerqm_log_term >= raerq->raerqm_prev_log_term);
    NIOVA_ASSERT(raerq->raerqm_log_term >=
                 raft_server_get_current_raft_entry_term(ri));

    const size_t entry_size = raerq->raerqm_entries_sz;

    // Msg size of '0' is OK.
    NIOVA_ASSERT(entry_size <= RAFT_ENTRY_MAX_DATA_SIZE);

    // Sanity check on the 'next' idx to be written.
    NIOVA_ASSERT(raft_server_get_current_raft_entry_index(ri) ==
                 raerq->raerqm_prev_log_index);

    enum raft_write_entry_opts opts = raerq->raerqm_leader_change_marker ?
        RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER : RAFT_WR_ENTRY_OPT_NONE;

    raft_server_write_next_entry(ri, raerq->raerqm_log_term,
                                 raerq->raerqm_entries, entry_size, opts);
}

/**
 * raft_server_process_append_entries_request_prep_reply - helper function for
 *    raft_server_process_append_entries_request() which does some general
 *    AE reply setup.
 */
static raft_server_cb_ctx_t
raft_server_process_append_entries_request_prep_reply(
    struct raft_instance *ri,
    struct raft_rpc_msg *reply,
    const struct raft_append_entries_request_msg *raerq)
{
    reply->rrm_type = RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY;
    reply->rrm_append_entries_reply.raerpm_leader_term =
        ri->ri_log_hdr.rlh_term;
    reply->rrm_append_entries_reply.raerpm_prev_log_index =
        raerq->raerqm_prev_log_index;
    reply->rrm_append_entries_reply.raerpm_heartbeat_msg =
        raerq->raerqm_heartbeat_msg;

    uuid_copy(reply->rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
    uuid_copy(reply->rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));
}

static raft_server_cb_ctx_int_t
raft_server_process_append_entries_request_validity_check(
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(raerq);

    // Do some basic verification of the AE msg contents.
    if (raerq->raerqm_prev_log_index < RAFT_MIN_APPEND_ENTRY_IDX ||
        raerq->raerqm_entries_sz > RAFT_ENTRY_MAX_DATA_SIZE)
	return -EINVAL;

    return 0;
}

static raft_server_cb_ctx_t
raft_server_advance_commit_idx(struct raft_instance *ri,
                               int64_t new_commit_idx)
{
    NIOVA_ASSERT(ri);

    /* This peer may be behind, don't advance the commit index past our
     * current raft index.
     */
    if (ri->ri_commit_idx < new_commit_idx &&
        raft_server_get_current_raft_entry_index(ri) >= new_commit_idx)
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "new_commit_idx=%ld", new_commit_idx);

        ri->ri_commit_idx = new_commit_idx;

        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_SM_APPLY]);
    }
}

static raft_server_cb_ctx_t
raft_server_process_append_entries_request(struct raft_instance *ri,
                                           struct ctl_svc_node *sender_csn,
                                           const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    DBG_RAFT_MSG(LL_DEBUG, rrm, "");

    struct raft_rpc_msg rreply_msg = {0};

    struct raft_append_entries_reply_msg *rae_reply =
        &rreply_msg.rrm_append_entries_reply;

    const struct raft_append_entries_request_msg *raerq =
        &rrm->rrm_append_entries_request;

    raft_server_process_append_entries_request_prep_reply(ri, &rreply_msg,
                                                          raerq);

    if (raft_server_process_append_entries_request_validity_check(raerq))
    {
        DBG_RAFT_MSG(LL_WARN, rrm,
          "raft_server_process_append_entries_request_validity_check() fails");
        return;
    }

    // Try to update the term if the leader has a higher one.
    const int64_t leader_term = raerq->raerqm_leader_term;
    raft_server_try_update_log_header_null_voted_for_peer(ri, leader_term);

    // Candidate timer - reset if this operation is valid.
    bool reset_timerfd = true;

    int rc =
        raft_server_process_append_entries_term_check_ops(ri, sender_csn,
                                                          raerq);
    if (rc)
    {
        NIOVA_ASSERT(rc == -ESTALE);
        reset_timerfd = false;

        /* raerpm_term was already set by
         * raft_server_process_append_entries_request_prep_reply().
         */
        rae_reply->raerpm_err_stale_term = 1;
    }
    else
    {
        rc = raft_server_append_entry_log_prepare_and_check(ri, raerq);
        if (rc)
        {
            if (rc != -EALREADY)
                rae_reply->raerpm_err_non_matching_prev_term = 1;
        }
        else
        {
            if (!raerq->raerqm_heartbeat_msg)
                raft_server_write_new_entry_from_leader(ri, raerq);

            /* Update our commit-idx based on the value sent from the leader.
             */
            raft_server_advance_commit_idx(ri, raerq->raerqm_commit_index);
        }
    }

    if (reset_timerfd)
        raft_server_timerfd_settime(ri);

    rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, sender_csn,
                              &rreply_msg);

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_send_msg(): %s",
                               strerror(rc));
}

static raft_server_cb_leader_ctx_int64_t
raft_server_leader_calculate_committed_idx(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft);
    NIOVA_ASSERT(raft_instance_is_leader(ri));

    raft_peer_t num_raft_members =
        ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    raft_peer_t this_peer_num = raft_server_instance_self_idx(ri);

    NIOVA_ASSERT(raft_member_idx_is_valid(ri, this_peer_num));

    uint8_t done_peers[CTL_SVC_MAX_RAFT_PEERS] = {0};
    int64_t sorted_indexes[CTL_SVC_MAX_RAFT_PEERS] =
        {[0 ... (CTL_SVC_MAX_RAFT_PEERS - 1)] = RAFT_MIN_APPEND_ENTRY_IDX};

    /* The leader doesn't update its own rfi_next_idx slot so do that here
     */
    struct raft_follower_info *self =
        raft_server_get_follower_info(ri, this_peer_num);

    self->rfi_next_idx = raft_server_get_current_raft_entry_index(ri) + 1;
    self->rfi_prev_idx_term = raft_server_get_current_raft_entry_term(ri);

    /* Sort the group member's next-idx values - note that these are the NEXT
     * index to be written not the already written idx value.
     */
    for (raft_peer_t i = 0; i < num_raft_members; i++)
    {
        raft_peer_t tmp_peer = RAFT_PEER_ANY;

        for (raft_peer_t j = 0; j < num_raft_members; j++)
        {
            const struct raft_follower_info *rfi =
                raft_server_get_follower_info(ri, j);

            if (!done_peers[j] &&
                (sorted_indexes[i] == RAFT_MIN_APPEND_ENTRY_IDX ||
                 rfi->rfi_next_idx < sorted_indexes[i]))
            {
                sorted_indexes[i] = rfi->rfi_next_idx;
                tmp_peer = j;
            }
        }
        NIOVA_ASSERT(tmp_peer < num_raft_members && !done_peers[tmp_peer]);
        done_peers[tmp_peer] = 1;
    }

    // simple sanity check.
    for (raft_peer_t i = 0; i < num_raft_members; i++)
        NIOVA_ASSERT(sorted_indexes[i] != RAFT_MIN_APPEND_ENTRY_IDX);

    const raft_peer_t majority_idx =
        raft_majority_index_value(num_raft_members);

    NIOVA_ASSERT(majority_idx < num_raft_members);

    /* Be sure to subtract one from the majority value since that value is the
     * 'next-idx'.
     */
    const int64_t committed_raft_idx =
        MAX(sorted_indexes[majority_idx] - 1, 0);

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "committed_raft_idx=%ld",
                      committed_raft_idx);

    // Ensure the ri_commit_idx is not moving backwards!
    NIOVA_ASSERT(committed_raft_idx >= ri->ri_commit_idx);

    return committed_raft_idx;
}

/**
 * raft_server_leader_try_advance_commit_idx -
 *     After receiving a successful AE reply,
 *     one where the follower was able to append the entry to its log, the
 *     leader now checks to see if can commit any older entries.  The
 *     determination for 'committed' relies on a majority of peers ACK'ing the
 *     AE in this leader's term - the leader may only tally ACKs for AEs sent
 *     in its term!
 *     raft_leader_has_applied_txn_in_my_term() cannot be used here since the
 *     data used by it must first be updated through a commit + apply
 *     operation.
 */
static raft_server_cb_leader_ctx_t
raft_server_leader_try_advance_commit_idx(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    NIOVA_ASSERT(raft_instance_is_leader(ri));

    const struct raft_leader_state *rls = &ri->ri_leader;

    const int64_t committed_raft_idx =
        raft_server_leader_calculate_committed_idx(ri);

    /* Only increase the commit index if the majority has ACKd this leader's
     * "leader_change_marker" AE.
     */
    if (committed_raft_idx >= rls->rls_initial_term_idx &&
        committed_raft_idx > ri->ri_commit_idx)
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "updating ri_commit_idx to %ld",
                          committed_raft_idx);

        raft_server_advance_commit_idx(ri, committed_raft_idx);
    }
}

static raft_server_cb_leader_ctx_t
raft_server_apply_append_entries_reply_result(
    struct raft_instance *ri,
    const uuid_t follower_uuid,
    const struct raft_append_entries_reply_msg *raerp)
{
    NIOVA_ASSERT(ri && raerp);
    NIOVA_ASSERT(raft_instance_is_leader(ri));
    NIOVA_ASSERT(!raerp->raerpm_err_stale_term);

    const raft_peer_t follower_idx = raft_peer_2_idx(ri, follower_uuid);
    NIOVA_ASSERT(follower_idx != RAFT_PEER_ANY);

    struct raft_follower_info *rfi =
        raft_server_get_follower_info(ri, follower_idx);

    // Update the last ack value for this follower.
    niova_realtime_coarse_clock(&rfi->rfi_last_ack);

    DBG_RAFT_INSTANCE((raerp->raerpm_heartbeat_msg ? LL_DEBUG : LL_NOTIFY), ri,
                      "follower=%x next-idx=%ld err=%hhx rp-pli=%ld",
                      follower_idx, rfi->rfi_next_idx,
                      raerp->raerpm_err_non_matching_prev_term,
                      raerp->raerpm_prev_log_index);

    /* Do not modify the rls->rls_next_idx[follower_idx] value unless the
     * reply corresponds to it.  This is to handle cases where replies get
     * delayed by the network.  If the follower still needs to have its
     * rls_next_idx decreased, it's ok, subsequent AE requests will eventually
     * cause it happen.  Note, this situation is common due to heartbeat msgs
     * running concurrently with pending AE's.  Heartbeat replies may meet the
     * criteria for advancing next-idx which will cause the non-hb AE reply
     * to appear stale.
     */
    if (raerp->raerpm_prev_log_index + 1 != rfi->rfi_next_idx)
    {
        DBG_RAFT_INSTANCE(
            LL_DEBUG, ri,
            "follower=%x hb=%d reply-ni=%ld my-ni-for-follower=%ld",
            follower_idx, raerp->raerpm_heartbeat_msg,
            raerp->raerpm_prev_log_index, rfi->rfi_next_idx);

        return;
    }

    if (raerp->raerpm_err_non_matching_prev_term)
    {
        if (rfi->rfi_next_idx > 0)
        {
            rfi->rfi_next_idx--;
            rfi->rfi_prev_idx_term = -1; //Xxx this needs to go into a function
        }
    }

    // Heartbeats don't advance the follower index
    else if (!raerp->raerpm_heartbeat_msg)
    {
        rfi->rfi_prev_idx_term = -1;
        rfi->rfi_next_idx++;

        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "follower=%x new-next-idx=%ld",
                          follower_idx, rfi->rfi_next_idx);

        // Only called if the entry append was successful.
        raft_server_leader_try_advance_commit_idx(ri);
    }

    if ((rfi->rfi_next_idx - 1) <
        raft_server_get_current_raft_entry_index(ri))
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "follower=%x still lags next-idx=%ld",
                          follower_idx, rfi->rfi_next_idx);

        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_AE_SEND]);
    }
}

static raft_server_cb_ctx_t
raft_server_process_append_entries_reply(struct raft_instance *ri,
                                         struct ctl_svc_node *sender_csn,
                                         const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    const struct raft_append_entries_reply_msg *raerp =
        &rrm->rrm_append_entries_reply;

    DBG_RAFT_MSG((raerp->raerpm_heartbeat_msg ? LL_DEBUG : LL_NOTIFY),
                 rrm, "");

    if (!raft_instance_is_leader(ri))
        return;

    /* raerpm_err_stale_term should only be considered if it's more recent than
     * our own term, otherwise it's stale.
     */
    if (raerp->raerpm_err_stale_term &&
        raerp->raerpm_leader_term > ri->ri_log_hdr.rlh_term)
        raft_server_becomes_follower(ri, raerp->raerpm_leader_term,
                                     sender_csn->csn_uuid,
                                     RAFT_BFRSN_STALE_TERM_WHILE_LEADER);
    else
        raft_server_apply_append_entries_reply_result(ri, sender_csn->csn_uuid,
                                                      raerp);
}

/**
 * raft_server_process_received_server_msg - called following the arrival of
 *    a udp message on the server <-> server socket.  After verifying
 *    that the sender's UUID and its raft UUID are known, this function will
 *    call the appropriate function handler based on the msg type.
 */
static raft_net_cb_ctx_t
raft_server_process_received_server_msg(struct raft_instance *ri,
	                                const struct raft_rpc_msg *rrm,
                                        struct ctl_svc_node *sender_csn)
{
    NIOVA_ASSERT(ri && rrm && sender_csn);

    /* General timestamp acquisition.  Note that this does not record AE
     * [n]ack statuses of the leader's followers.  Those are updated in
     * raft_server_process_append_entries_reply().
     */
    raft_net_update_last_comm_time(ri, sender_csn->csn_uuid, false);

    switch (rrm->rrm_type)
    {
    case RAFT_RPC_MSG_TYPE_VOTE_REQUEST:
        return raft_server_process_vote_request(ri, sender_csn, rrm);

    case RAFT_RPC_MSG_TYPE_VOTE_REPLY:
        return raft_server_process_vote_reply(ri, sender_csn, rrm);

    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST:
	return raft_server_process_append_entries_request(ri, sender_csn, rrm);

    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY:
        return raft_server_process_append_entries_reply(ri, sender_csn, rrm);

    default:
        DBG_RAFT_MSG(LL_NOTIFY, rrm, "unhandled msg type %d", rrm->rrm_type);
        break;
    }
}

static raft_net_cb_ctx_t
raft_server_peer_recv_handler(struct raft_instance *ri,
                                  const char *recv_buffer,
                                  ssize_t recv_bytes,
                                  const struct sockaddr_in *from)
{
    NIOVA_ASSERT(ri && from);

    if (!recv_buffer || !recv_bytes)
        return;

    const struct raft_rpc_msg *rrm = (const struct raft_rpc_msg *)recv_buffer;

    size_t expected_msg_size = sizeof(struct raft_rpc_msg);

    if (rrm->rrm_type == RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST)
        expected_msg_size += rrm->rrm_append_entries_request.raerqm_entries_sz;

    /* Server <-> server messages do not have additional payloads.
     */
    if (recv_bytes != expected_msg_size)
    {
        DBG_RAFT_INSTANCE(
            LL_NOTIFY, ri,
            "Invalid msg size %zd (expected %zu) from peer %s:%d",
            recv_bytes, expected_msg_size, inet_ntoa(from->sin_addr),
            ntohs(from->sin_port));

        return;
    }

    DBG_RAFT_MSG(LL_DEBUG, rrm, "msg-size=(%zd) peer %s:%d",
                 recv_bytes, inet_ntoa(from->sin_addr),
                 ntohs(from->sin_port));

    /* Verify the sender's id before proceeding.
     */
    struct ctl_svc_node *sender_csn =
        raft_net_verify_sender_server_msg(ri, rrm->rrm_sender_id,
                                          rrm->rrm_raft_id, from);
    if (!sender_csn)
        return;

    raft_server_process_received_server_msg(ri, rrm, sender_csn);
}

static raft_server_cb_ctx_bool_t
raft_leader_instance_is_fresh(const struct raft_instance *ri)
{
    if (!raft_instance_is_leader(ri))
        return false;

    struct timespec now;
    niova_realtime_coarse_clock(&now);

    size_t num_acked_within_window = 1; // count "self"

    const raft_peer_t num_raft_peers = raft_num_members_validate_and_get(ri);

    for (raft_peer_t i = 0; i < num_raft_peers; i++)
    {
        if (i == raft_server_instance_self_idx(ri))
            continue;

        const struct raft_follower_info *rfi =
            raft_server_get_follower_info((struct raft_instance *)ri, i);

        // Ignore if time has moved backwards
        if (timespeccmp(&now, &rfi->rfi_last_ack, <=))
            continue;

        struct timespec diff;

        timespecsub(&now, &rfi->rfi_last_ack, &diff);

        if (timespec_2_msec(&diff) < RAFT_ELECTION_MIN_TIME_MS)
            num_acked_within_window++;
    }

    return (num_acked_within_window >= (num_raft_peers / 2 + 1)) ?
        true : false;
}

/**
 * raft_server_may_process_client_request - this function checks the state of
 *    this raft instance to determine if it's qualified to accept a client
 *    request.
 */
static raft_net_cb_ctx_int_t
raft_server_may_accept_client_request(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    /* Not the leader, then cause a redirect reply to be done.
     */
    if (raft_instance_is_booting(ri))
        return -EINPROGRESS;

    else if (raft_instance_is_candidate(ri))
        return -ENOENT;

    else if (!raft_instance_is_leader(ri)) // 1. am I the raft leader?
        return -ENOSYS;

    // 2. am I a fresh raft leader?
    else if (!raft_leader_instance_is_fresh(ri))
        return -EAGAIN;

    // 3. have I applied all of the lastApplied entries that I need -
    //    including a fake AE command (which is written to the logs)?
    else if (!raft_leader_has_applied_txn_in_my_term(ri))
        return -EBUSY;

    return 0;
}

static const char *
raft_server_may_accept_client_request_reason(const struct raft_instance *ri)
{
    int rc = raft_server_may_accept_client_request(ri);

    return raft_net_client_rpc_sys_error_2_string(rc);
}

static raft_net_cb_ctx_t
raft_server_reply_to_client(struct raft_instance *ri,
                            struct raft_net_client_request *rncr)
{
    if (!ri || !ri->ri_csn_this_peer || !ri->ri_csn_raft || !rncr ||
        raft_net_sockaddr_is_valid(&rncr->rncr_remote_addr))
        return;

    /* Copy the reply info from the provided rncr pointer.  This reply info
     * fields have been written by the state_machine callback.
     */
    const struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    DBG_RAFT_CLIENT_RPC(LL_DEBUG, rncr->rncr_request, &rncr->rncr_remote_addr,
                        "original request");
    DBG_RAFT_CLIENT_RPC(LL_DEBUG, reply, &rncr->rncr_remote_addr,
                        "reply");

    int rc = raft_server_send_msg_to_client(ri, rncr);
    if (rc)
        DBG_RAFT_CLIENT_RPC(LL_ERROR, reply, &rncr->rncr_remote_addr,
                            "raft_server_send_msg(): %s", strerror(rc));
}

static raft_net_cb_ctx_t
raft_server_udp_client_deny_request(struct raft_instance *ri,
                                    struct raft_net_client_request *rncr,
                                    const int rc)
{
    NIOVA_ASSERT(ri && rncr && rncr->rncr_request && rncr->rncr_reply);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    reply->rcrm_sys_error = rc;

    if (rc == -ENOSYS && ri->ri_csn_leader)
    {
        reply->rcrm_type = RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT;
        uuid_copy(reply->rcrm_redirect_id, ri->ri_csn_leader->csn_uuid);
    }

    return raft_server_reply_to_client(ri, rncr);
}

static raft_net_cb_ctx_t
raft_server_udp_client_reply_init(const struct raft_instance *ri,
                                  struct raft_net_client_request *rncr)
{
    NIOVA_ASSERT(ri && rncr && rncr->rncr_request && rncr->rncr_reply);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    memset(reply, 0, sizeof(struct raft_client_rpc_msg));

    uuid_copy(reply->rcrm_raft_id, ri->ri_csn_raft->csn_uuid);
    uuid_copy(reply->rcrm_sender_id, ri->ri_csn_this_peer->csn_uuid);

    reply->rcrm_type = (rncr->rncr_request->rcrm_type ==
                        RAFT_CLIENT_RPC_MSG_TYPE_PING) ?
        RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY :
        RAFT_CLIENT_RPC_MSG_TYPE_REPLY;

    reply->rcrm_msg_id = rncr->rncr_request->rcrm_msg_id;
}

static raft_net_cb_ctx_bool_t
raft_server_udp_client_recv_ignore_request(
    struct raft_instance *ri, const struct raft_client_rpc_msg *rcm,
    const struct sockaddr_in *from, struct net_ctl *nc_out)
{
    NIOVA_ASSERT(rcm && from);

    bool ignore_request = false;
    const char *cause = NULL;

    // Ensure this client's raft instance is consistent with ours.
    int rc = raft_net_verify_sender_client_msg(ri, rcm->rcrm_raft_id);
    if (rc)
    {
        cause = "raft_net_verify_sender_client_msg()";
        ignore_request = true;
    }
    else
    {
        /* Lookup the client in the ctl-svc-node tree - existence is not
         * mandatory.
         */
        struct ctl_svc_node *client_csn = NULL;
        rc = ctl_svc_node_lookup(rcm->rcrm_sender_id, &client_csn);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_WARN, "ctl_svc_node_lookup(): %d", rc);
            return false;
        }

        if (client_csn)
        {
            if (client_csn->csn_type == CTL_SVC_NODE_TYPE_RAFT_CLIENT)
            {
                if (!net_ctl_can_recv(&client_csn->csn_peer.csnp_net_ctl))
                {
                    cause = "recv from this UUID is disabled";
                    ignore_request = true;
                }

                if (nc_out)
                    *nc_out = client_csn->csn_peer.csnp_net_ctl;
            }
            else
            {
                cause = "UUID does not belong to a client";
                ignore_request = true;

                DBG_CTL_SVC_NODE(
                    LL_NOTIFY, client_csn,
                    "recv'd RPC request from this non-client UUID");
            }

            ctl_svc_node_put(client_csn);
        }
        else if (ri->ri_net_type == RAFT_INSTANCE_NET_TCP)
        {
            cause = "CSN required for TCP mode clients";
            ignore_request = true;
        }
    }

    if (ignore_request)
        DBG_RAFT_CLIENT_RPC(LL_NOTIFY, rcm, from, "%s (rc=%d)", cause, rc);

    return ignore_request;
}

static raft_net_cb_ctx_t
raft_server_client_recv_handler(struct raft_instance *ri,
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

    struct net_ctl nc = {0};

    /* First set of request checks which are configuration based.
     */
    if (raft_server_udp_client_recv_ignore_request(ri, rcm, from, &nc))
        return;

    struct raft_net_client_request rncr = {
        .rncr_type = RAFT_NET_CLIENT_REQ_TYPE_NONE, // will be reset by cb
        .rncr_write_raft_entry = false,
        .rncr_op_error = 0,
        .rncr_nc = nc,
        .rncr_is_leader = raft_instance_is_leader(ri) ? true : false,
        .rncr_entry_term = ri->ri_log_hdr.rlh_term,
        .rncr_current_term = ri->ri_log_hdr.rlh_term,
        .rncr_request = rcm,
        .rncr_reply = (struct raft_client_rpc_msg *)reply_buf,
        .rncr_remote_addr = *from,
        .rncr_reply_data_max_size =
            (RAFT_NET_MAX_RPC_SIZE - sizeof(struct raft_client_rpc_msg)),
    };

    raft_server_udp_client_reply_init(ri, &rncr);

    /* Second set of checks which determine if this server is capable of
     * handling the request at this time.
     */
    int rc = raft_server_may_accept_client_request(ri);
    if (rc)
        return raft_server_udp_client_deny_request(ri, &rncr, rc);

    if (rcm->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_PING)
        return raft_server_reply_to_client(ri, &rncr);

    /* Call into the application state machine logic.  There are several
     * outcomes here:
     * 1. SM detects a new write, here it may store sender info for reply
     *    post-commit.
     * 2. SM detects a write which had already been committed, here we reply
     *    to the client notifying it of the completion.
     * 3. SM detects a write which is still in progress, here no reply is sent.
     * 4. SM processes a read request, returning the requested application
     *    data.
     */
    int cb_rc = ri->ri_server_sm_request_cb(&rncr);

    // rncr.rncr_type was set by the callback!
    bool write_op = rncr.rncr_type == RAFT_NET_CLIENT_REQ_TYPE_WRITE ?
        true : false;

    enum log_level log_level = cb_rc ? LL_WARN : LL_DEBUG;

    DBG_RAFT_CLIENT_RPC(log_level, rcm, from,
                        "wr_op=%d write-2-raft=%s op_error=%s, cb_rc=%s",
                        write_op, rncr.rncr_write_raft_entry ? "yes" : "no",
                        strerror(-rncr.rncr_op_error), strerror(-cb_rc));

    if (cb_rc) // Other than logging this issue, nothing can be done here
        return;

    /* cb's may run for a long time and the server may have been deposed
     * Xxx note that SM write requests left in this state may require
     *   cleanup.
     */
    rc = raft_server_may_accept_client_request(ri);
    if (rc)
        return raft_server_udp_client_deny_request(ri, &rncr, rc);

    /* Store the request as an entry in the Raft log.  Do not reply to the
     * client until the write is committed and applied!
     */
    if (rncr.rncr_write_raft_entry)
        raft_server_leader_write_new_entry(ri, rcm->rcrm_data,
                                           rcm->rcrm_data_size,
                                           RAFT_WR_ENTRY_OPT_NONE);

    /* Read operation or an already committed + applied write operation.
     */
    else
        raft_server_reply_to_client(ri, &rncr);
}

/**
 * raft_server_append_entry_should_send_to_follower - helper function which
 *    manages the rfi_ae_sends_wait_until value for the given peer_idx.
 *    It returns a true when either the peer is not detected as unresponsive
 *    or after the waiting period has passed.
 */
static bool
raft_server_append_entry_should_send_to_follower(
    struct raft_instance *ri,
    const raft_peer_t raft_peer_idx)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft &&
                 raft_member_idx_is_valid(ri, raft_peer_idx));

    struct raft_follower_info *rfi =
        raft_server_get_follower_info(ri, raft_peer_idx);

    unsigned long long now_msec = niova_unstable_coarse_clock_get_msec();
    unsigned long long since_last_unacked = 0;

    int rc = raft_net_comm_recency(ri, raft_peer_idx,
                                   RAFT_COMM_RECENCY_UNACKED_SEND,
                                   &since_last_unacked);
    if (rc == -ENOTCONN) {
        // XXX will this prevent the server from ever trying to connect?
        return false;
    }
    NIOVA_ASSERT(!rc);

    bool send_msg = true;

    if (since_last_unacked > 0) // No recv'd msgs since last send.
    {
        if (now_msec > rfi->rfi_ae_sends_wait_until)
            rfi->rfi_ae_sends_wait_until =
                (now_msec +
                 MIN(RAFT_NET_MAX_RETRY_MS,
                     (rfi->rfi_ae_sends_wait_until * 2 + 1)));
        else
            send_msg = false;
    }
    else
    {
        rfi->rfi_ae_sends_wait_until = 0;
    }

    // This is not a recency check and should be in a separate function Xxx
    if (rfi->rfi_next_idx > raft_server_get_current_raft_entry_index(ri))
    {
        // May only be ahead by '1'
        NIOVA_ASSERT(rfi->rfi_next_idx ==
                     raft_server_get_current_raft_entry_index(ri) + 1);
        send_msg = false;
    }

    return send_msg;
}

static raft_server_epoll_ae_sender_t
raft_server_append_entry_sender(struct raft_instance *ri, bool heartbeat)
{
    NIOVA_ASSERT(ri);

    if (!raft_instance_is_leader(ri) ||
        raft_server_get_current_raft_entry_index(ri) < 0)
        return;

    static char src_buf[RAFT_NET_MAX_RPC_SIZE];
    static char sink_buf[RAFT_ENTRY_SIZE];

    struct raft_rpc_msg *rrm = (struct raft_rpc_msg *)src_buf;
//    const size_t data_len =
//        RAFT_NET_MAX_RPC_SIZE - sizeof(struct raft_rpc_msg);

    const raft_peer_t num_raft_members = raft_num_members_validate_and_get(ri);

    ///Xxx this is a big mess of code which needs to be made into some
    //     subroutines.
    for (raft_peer_t i = 0; i < num_raft_members; i++)
    {
         struct ctl_svc_node *rp = ri->ri_csn_raft_peers[i];

         if (rp == ri->ri_csn_this_peer ||
             (!raft_server_append_entry_should_send_to_follower(ri, i) &&
              !heartbeat))
             continue;

         memset(src_buf, 0, RAFT_NET_MAX_RPC_SIZE);
         memset(sink_buf, 0, RAFT_ENTRY_SIZE);

         raft_server_leader_init_append_entry_msg(ri, rrm, i, heartbeat);

         struct raft_append_entries_request_msg *raerq =
             &rrm->rrm_append_entries_request;

         const int64_t peer_next_raft_idx = raerq->raerqm_prev_log_index + 1;
         const int64_t my_raft_idx =
             raft_server_get_current_raft_entry_index(ri);

         DBG_RAFT_INSTANCE_FATAL_IF((peer_next_raft_idx - 1 > my_raft_idx), ri,
                                    "follower's idx > leader's (%ld > %ld)",
                                    peer_next_raft_idx, my_raft_idx);

         if (!heartbeat && peer_next_raft_idx <= my_raft_idx)
         {
             struct raft_entry_header *reh =
                 (struct raft_entry_header *)sink_buf;

             int rc = raft_server_entry_header_read_by_store(ri, reh,
                                                             peer_next_raft_idx);
             DBG_RAFT_INSTANCE_FATAL_IF(
                 (rc), ri, "raft_server_entry_header_read_by_store(%ld): %s",
                 peer_next_raft_idx, strerror(-rc));

             raerq->raerqm_entries_sz = reh->reh_data_size;
             raerq->raerqm_leader_change_marker = reh->reh_leader_change_marker;

             NIOVA_ASSERT(reh->reh_index == peer_next_raft_idx);

             if (raerq->raerqm_entries_sz)
             {
                 rc = raft_server_entry_read(ri, peer_next_raft_idx,
                                             raerq->raerqm_entries,
                                             raerq->raerqm_entries_sz, NULL);
                 DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                                            "raft_server_entry_read(): %s",
                                            strerror(-rc));
             }
         }
         else
         {
             raerq->raerqm_entries_sz = 0;
             raerq->raerqm_leader_change_marker = 0;
             raerq->raerqm_heartbeat_msg = 1;
         }

         DBG_SIMPLE_CTL_SVC_NODE(
             (heartbeat ? LL_DEBUG : LL_NOTIFY), rp,
             "idx=%hhx pli=%ld lt=%ld", i,
             rrm->rrm_append_entries_request.raerqm_prev_log_index,
             rrm->rrm_append_entries_request.raerqm_log_term);

         // XXX what to do on failure?
         int rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, rp, rrm);

         DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "raft_server_send_msg(): %d", rc);
    }
}

static raft_server_epoll_sm_apply_bool_t
raft_server_state_machine_apply(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    NIOVA_ASSERT(ri->ri_last_applied_idx <= ri->ri_commit_idx);

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");

    if (ri->ri_last_applied_idx == ri->ri_commit_idx)
        return;

    static char sink_buf[RAFT_ENTRY_SIZE];
    static char reply_buf[RAFT_ENTRY_SIZE];

    // Replies of commit / write requests do not require payloads.
    struct raft_client_rpc_msg *reply =
        (struct raft_client_rpc_msg *)reply_buf;

    const size_t apply_idx = ri->ri_last_applied_idx + 1;

    struct raft_entry_header reh;

    int rc = raft_server_entry_header_read_by_store(ri, &reh, apply_idx);
    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                               "raft_server_entry_header_read_by_store(): %s",
                               strerror(-rc));

    bool reply_to_client = false;

    struct raft_net_client_request rncr = {
        .rncr_type = RAFT_NET_CLIENT_REQ_TYPE_COMMIT,
        .rncr_write_raft_entry = false,
        .rncr_op_error = 0,
        .rncr_is_leader = raft_instance_is_leader(ri) ? true : false,
        .rncr_entry_term = reh.reh_term,
        .rncr_current_term = ri->ri_log_hdr.rlh_term,
        .rncr_commit_data = sink_buf,
        .rncr_remote_addr = {0},
        .rncr_reply = reply,
        .rncr_reply_data_max_size = RAFT_NET_MAX_RPC_SIZE,
    };

    if (!reh.reh_leader_change_marker && reh.reh_data_size)
    {
        rc = raft_server_entry_read(ri, apply_idx, sink_buf, reh.reh_data_size,
                                    NULL);
        DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_entry_read(): %s",
                                   strerror(-rc));

        int rc = ri->ri_server_sm_request_cb(&rncr);
        if (!rc)
        {
            struct binary_hist *bh =
                &ri->ri_rihs[RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC].rihs_bh;

            if (rncr.rncr_commit_duration_msec > 0)
                binary_hist_incorporate_val(bh,
                                            rncr.rncr_commit_duration_msec);

            // Perform basic initialization on the reply buffer
            raft_server_udp_client_reply_init(ri, &rncr);
            reply_to_client = true;
        }
    }

    if (!reh.reh_leader_change_marker && !reh.reh_data_size)
        DBG_RAFT_ENTRY(LL_WARN, &reh, "application entry contains no data!");

    // Signify that the entry has been applied!
    ri->ri_last_applied_idx++;

    ri->ri_last_applied_cumulative_crc ^= reh.reh_crc;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "ri_last_applied_idx was incremented");

    DBG_RAFT_ENTRY(LL_NOTIFY, &reh, "");

    if (ri->ri_last_applied_idx < ri->ri_commit_idx)
        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_SM_APPLY]);

    if (reply_to_client)
        raft_server_reply_to_client(ri, &rncr);
}

static raft_server_epoll_ae_sender_t
raft_server_append_entry_sender_evp_cb(const struct epoll_handle *eph)
{
    NIOVA_ASSERT(eph);

    FUNC_ENTRY(LL_DEBUG);

    struct raft_instance *ri = eph->eph_arg;
    struct ev_pipe *evp = &ri->ri_evps[RAFT_SERVER_EVP_AE_SEND];

    NIOVA_ASSERT(eph->eph_fd == evp_read_fd_get(evp));

    ev_pipe_drain(evp);

    raft_server_append_entry_sender(ri, false);

    evp_increment_reader_cnt(evp); //Xxx this is a mess
    // should be inside ev_pipe.c!
}

static raft_server_epoll_sm_apply_t
raft_server_sm_apply_evp_cb(const struct epoll_handle *eph)
{
    NIOVA_ASSERT(eph);

    FUNC_ENTRY(LL_DEBUG);

    struct raft_instance *ri = eph->eph_arg;

    struct ev_pipe *evp = &ri->ri_evps[RAFT_SERVER_EVP_SM_APPLY];
    NIOVA_ASSERT(eph->eph_fd == evp_read_fd_get(evp));

    ev_pipe_drain(evp);
    evp_increment_reader_cnt(evp); //Xxx this is a mess
    // should be inside ev_pipe.c!

    raft_server_state_machine_apply(ri);
}

static epoll_mgr_cb_t
raft_server_evp_2_cb_fn(enum raft_server_event_pipes evps)
{
    switch (evps)
    {
    case RAFT_SERVER_EVP_AE_SEND:
        return raft_server_append_entry_sender_evp_cb;
    case RAFT_SERVER_EVP_SM_APPLY:
        return raft_server_sm_apply_evp_cb;
    default:
        break;
    }
    return NULL;
}

static int
raft_server_evp_setup(struct raft_instance *ri)
{
    if (!ri || raft_instance_is_client(ri))
        return -EINVAL;

    for (enum raft_server_event_pipes i = 0; i < RAFT_SERVER_EVP_ANY; i++)
    {
        int rc = raft_net_evp_add(ri, raft_server_evp_2_cb_fn(i));
        NIOVA_ASSERT(rc == i); /* rc should equal the pipe value Xxx
                                * since the code currently accesses the evp
                                * array directly.
                                */
        if (rc < 0)
            return rc;
    }

    return 0;
}

static int
raft_server_evp_cleanup(struct raft_instance *ri)
{
    if (!ri || raft_instance_is_client(ri))
        return -EINVAL;

    for (int i = 0; i < RAFT_SERVER_EVP_ANY; i++)
    {
        enum raft_epoll_handles eph_type = raft_server_evp_2_epoll_handle(i);
        NIOVA_ASSERT(eph_type < RAFT_EPOLL_NUM_HANDLES);

        struct epoll_handle *eph = &ri->ri_epoll_handles[eph_type];
        epoll_handle_del(&ri->ri_epoll_mgr, eph);

        ev_pipe_cleanup(&ri->ri_evps[i]);
    }

    return 0;
}

void
raft_server_instance_init(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_booting(ri));

    ri->ri_commit_idx = -1; //Xxx this needs to go into a more general init fn
    ri->ri_last_applied_idx = -1;

    /* Assign the timer_fd and udp_recv callbacks.
     */
    raft_net_instance_apply_callbacks(ri, raft_server_timerfd_cb,
                                    raft_server_client_recv_handler,
                                    raft_server_peer_recv_handler);

}

static util_thread_ctx_reg_t
raft_server_instance_hist_lreg_multi_facet_handler(
    enum lreg_node_cb_ops op,
    struct raft_instance_hist_stats *rihs,
    struct lreg_value *lv)
{
    if (!lv ||
	lv->lrv_value_idx_in >= binary_hist_size(&rihs->rihs_bh) ||
	op != LREG_NODE_CB_OP_READ_VAL)
        return;

    snprintf(lv->lrv_key_string, LREG_VALUE_STRING_MAX, "%lld",
             binary_hist_lower_bucket_range(&rihs->rihs_bh,
                                            lv->lrv_value_idx_in));

    LREG_VALUE_TO_OUT_SIGNED_INT(lv) =
	binary_hist_get_cnt(&rihs->rihs_bh, lv->lrv_value_idx_in);

    lv->get.lrv_value_type_out = LREG_VAL_TYPE_UNSIGNED_VAL;
}

static util_thread_ctx_reg_int_t
raft_server_instance_hist_lreg_cb(enum lreg_node_cb_ops op,
                                  struct lreg_node *lrn,
                                  struct lreg_value *lv)
{
    struct raft_instance_hist_stats *rihs = lrn->lrn_cb_arg;

    if (lv)
        lv->get.lrv_num_keys_out = binary_hist_size(&rihs->rihs_bh);

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;

        lreg_value_fill_key_and_type(
            lv, raft_instance_hist_stat_2_name(rihs->rihs_type),
            LREG_VAL_TYPE_OBJECT);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
    case LREG_NODE_CB_OP_WRITE_VAL: //fall through
        if (!lv)
            return -EINVAL;

        raft_server_instance_hist_lreg_multi_facet_handler(op, rihs, lv);
	break;

    case LREG_NODE_CB_OP_INSTALL_NODE:
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    default:
	return -ENOENT;
    }

    return 0;
}

static int
raft_server_instance_lreg_init(struct raft_instance *ri)
{
    LREG_ROOT_ENTRY_INSTALL(raft_root_entry);

    lreg_node_init(&ri->ri_lreg, LREG_USER_TYPE_RAFT,
                   raft_instance_lreg_cb, ri, LREG_INIT_OPT_NONE);

    int rc = lreg_node_install_prepare(&ri->ri_lreg,
                                       LREG_ROOT_ENTRY_PTR(raft_root_entry));
    if (rc)
        return rc;

    for (enum raft_instance_hist_types i = RAFT_INSTANCE_HIST_MIN;
         i < RAFT_INSTANCE_HIST_MAX; i++)
    {
        lreg_node_init(&ri->ri_rihs[i].rihs_lrn, i,
                       raft_server_instance_hist_lreg_cb,
                       (void *)&ri->ri_rihs[i],
                       LREG_INIT_OPT_IGNORE_NUM_VAL_ZERO);

        rc = lreg_node_install_prepare(&ri->ri_rihs[i].rihs_lrn, &ri->ri_lreg);
        if (rc)
            return rc;
    }

    return 0;
}

int
raft_server_instance_startup(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_booting(ri));

    // raft_server_instance_init() should have been run
    if (!ri->ri_timer_fd_cb)
        return -EINVAL;

    int rc = raft_server_backend_setup(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_backend_setup(): %s",
                          strerror(-rc));
        return rc;
    }

    rc = raft_server_instance_lreg_init(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_instance_lreg_init(): %s",
                          strerror(-rc));

        raft_server_instance_shutdown(ri);
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

    rc = raft_server_evp_setup(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "ev_pipe_setup(): %s",
                          strerror(-rc));

        raft_server_instance_shutdown(ri);
        return rc;
    }

    return 0;
}

static int
raft_server_backend_close(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    return ri->ri_backend->rib_backend_shutdown(ri);
}

int
raft_server_instance_shutdown(struct raft_instance *ri)
{
    raft_server_backend_close(ri);

    raft_server_evp_cleanup(ri);

    for (int i = 0; i < RAFT_SERVER_EVP_ANY; i++)
        ev_pipe_cleanup(&ri->ri_evps[i]);

    return 0;
}

int
raft_server_main_loop(struct raft_instance *ri)
{
    NIOVA_ASSERT(raft_instance_is_booting(ri));
    ri->ri_state = RAFT_STATE_FOLLOWER;
    ri->ri_follower_reason = RAFT_BFRSN_LEADER_ALREADY_PRESENT;

    raft_server_timerfd_settime(ri);

    int rc = 0;

    do
    {
        // Xxx these are just examples..
        FAULT_INJECT(disabled);
        FAULT_INJECT(any);
        rc = epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);
        if (rc == -EINTR)
            rc = 0;
    } while (rc >= 0);

    SIMPLE_LOG_MSG(LL_WARN, "epoll_mgr_wait_and_process_events(): %s",
                   strerror(-rc));

    return rc;
}

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
#include "thread.h"
#include "util_thread.h"

LREG_ROOT_ENTRY_GENERATE(raft_root_entry, LREG_USER_TYPE_RAFT);

enum raft_write_entry_opts
{
    RAFT_WR_ENTRY_OPT_NONE                 = 0,
    RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER = 1,
    RAFT_WR_ENTRY_OPT_LOG_HEADER           = 2,
    RAFT_WR_ENTRY_OPT_FOLLOWER_WRITE       = 3,
    RAFT_WR_ENTRY_OPT_ANY                  = 255,
};

REGISTRY_ENTRY_FILE_GENERATE;

#define RAFT_SERVER_SYNC_MIN_FREQ_US 100
#define RAFT_SERVER_SYNC_MAX_FREQ_US 100000000
#define RAFT_SERVER_SYNC_FREQ_US 4000

typedef void * raft_server_sync_thread_t;
typedef void raft_server_sync_thread_ctx_t;

typedef void * raft_server_chkpt_thread_t;

// A value of '0' means that all will be read
static size_t raftPersistentAppMaxScanEntries =
    RAFT_INSTANCE_PERSISTENT_APP_MAX_SCAN_ENTRIES;

static const char *
raft_server_may_accept_client_request_reason(struct raft_instance *ri);

static raft_peer_t
raft_server_instance_self_idx(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_this_peer);

    return raft_peer_2_idx(ri, ri->ri_csn_this_peer->csn_uuid);
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
        return "candidacy-stale-term";
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
    RAFT_LREG_SYNC_FREQ_US,       // uint64
    RAFT_LREG_SYNC_CNT,           // uint64
    RAFT_LREG_NEWEST_ENTRY_IDX,   // int64
    RAFT_LREG_NEWEST_ENTRY_TERM,  // int64
    RAFT_LREG_NEWEST_ENTRY_SIZE,  // uint32
    RAFT_LREG_NEWEST_ENTRY_CRC,   // uint32
    RAFT_LREG_NEWEST_UNSYNC_ENTRY_IDX,   // int64
    RAFT_LREG_NEWEST_UNSYNC_ENTRY_TERM,  // int64
    RAFT_LREG_NEWEST_UNSYNC_ENTRY_SIZE,  // uint32
    RAFT_LREG_NEWEST_UNSYNC_ENTRY_CRC,   // uint32
    RAFT_LREG_LOWEST_IDX,         // int64
    RAFT_LREG_CHKPT_IDX,          // int64
    RAFT_LREG_HIST_DEV_READ_LAT,  // hist object
    RAFT_LREG_HIST_DEV_WRITE_LAT, // hist object
    RAFT_LREG_HIST_DEV_SYNC_LAT,  // hist object
    RAFT_LREG_HIST_NENTRIES_SYNC, // hist object
    RAFT_LREG_HIST_CHKPT_LAT,     // hist object
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

static void
raft_server_set_sync_freq(struct raft_instance *ri,
                          const struct lreg_value *lv)
{
    if (!ri || !lv || LREG_VALUE_TO_REQ_TYPE_IN(lv) != LREG_VAL_TYPE_STRING)
        return;

    unsigned int sync_freq = RAFT_SERVER_SYNC_FREQ_US;
    if (strncmp(LREG_VALUE_TO_IN_STR(lv), "default", 7))
    {
        int rc = niova_string_to_unsigned_int(LREG_VALUE_TO_IN_STR(lv),
                                              &sync_freq);
        if (rc)
            return;
    }

    // Keep the sync freq in range
    if (sync_freq < RAFT_SERVER_SYNC_MIN_FREQ_US)
        sync_freq = RAFT_SERVER_SYNC_MIN_FREQ_US;

    else if (sync_freq > RAFT_SERVER_SYNC_MAX_FREQ_US)
        sync_freq = RAFT_SERVER_SYNC_MAX_FREQ_US;

    if (sync_freq != ri->ri_sync_freq_us)
        ri->ri_sync_freq_us = sync_freq;
}


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
        case RAFT_LREG_LOWEST_IDX:
            lreg_value_fill_signed(lv, "lowest-idx", ri->ri_lowest_idx);
            break;
        case RAFT_LREG_CHKPT_IDX:
            lreg_value_fill_signed(lv, "checkpoint-idx",
                                   ri->ri_checkpoint_last_idx);
            break;
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
                lreg_value_fill_string(lv, "leader-uuid", "");
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
                lv, "sync-entry-idx",
                raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC));
            break;
        case RAFT_LREG_NEWEST_ENTRY_TERM:
            lreg_value_fill_signed(
                lv, "sync-entry-term",
                raft_server_get_current_raft_entry_term(ri, RI_NEHDR_SYNC));
            break;
        case RAFT_LREG_NEWEST_ENTRY_SIZE:
            lreg_value_fill_unsigned(
                lv, "sync-entry-data-size",
                raft_server_get_current_raft_entry_data_size(ri,
                                                             RI_NEHDR_SYNC));
            break;
        case RAFT_LREG_NEWEST_ENTRY_CRC:
            lreg_value_fill_unsigned(
                lv, "sync-entry-crc",
                raft_server_get_current_raft_entry_crc(ri, RI_NEHDR_SYNC));
            break;
        case RAFT_LREG_NEWEST_UNSYNC_ENTRY_IDX:
            lreg_value_fill_signed(
                lv, "unsync-entry-idx",
                raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC));
            break;
        case RAFT_LREG_NEWEST_UNSYNC_ENTRY_TERM:
            lreg_value_fill_signed(
                lv, "unsync-entry-term",
                raft_server_get_current_raft_entry_term(ri, RI_NEHDR_UNSYNC));
            break;
        case RAFT_LREG_NEWEST_UNSYNC_ENTRY_SIZE:
            lreg_value_fill_unsigned(
                lv, "unsync-entry-data-size",
                raft_server_get_current_raft_entry_data_size(ri,
                                                             RI_NEHDR_UNSYNC));
            break;
        case RAFT_LREG_NEWEST_UNSYNC_ENTRY_CRC:
            lreg_value_fill_unsigned(
                lv, "unsync-entry-crc",
                raft_server_get_current_raft_entry_crc(ri, RI_NEHDR_UNSYNC));
            break;
        case RAFT_LREG_SYNC_FREQ_US:
            lreg_value_fill_unsigned(lv, "sync-freq-us", ri->ri_sync_freq_us);
            break;
        case RAFT_LREG_SYNC_CNT:
            lreg_value_fill_unsigned(lv, "sync-cnt", ri->ri_sync_cnt);
            break;
        case RAFT_LREG_HIST_COMMIT_LAT:
            lreg_value_fill_histogram(
                lv, raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC),
                RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC);
            break;
        case RAFT_LREG_HIST_READ_LAT:
            lreg_value_fill_histogram(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_READ_LAT_MSEC),
                RAFT_INSTANCE_HIST_READ_LAT_MSEC);
            break;
        case RAFT_LREG_HIST_DEV_READ_LAT:
            lreg_value_fill_histogram(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC),
                RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC);
            break;
        case RAFT_LREG_HIST_DEV_WRITE_LAT:
            lreg_value_fill_histogram(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC),
                RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC);
            break;
        case RAFT_LREG_HIST_DEV_SYNC_LAT:
            lreg_value_fill_histogram(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_DEV_SYNC_LAT_USEC),
                RAFT_INSTANCE_HIST_DEV_SYNC_LAT_USEC);
            break;
        case RAFT_LREG_HIST_NENTRIES_SYNC:
            lreg_value_fill_histogram(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_NENTRIES_SYNC),
                RAFT_INSTANCE_HIST_NENTRIES_SYNC);
            break;
        case RAFT_LREG_HIST_CHKPT_LAT:
            lreg_value_fill_histogram(
                lv,
                raft_instance_hist_stat_2_name(
                    RAFT_INSTANCE_HIST_CHKPT_LAT_USEC),
                RAFT_INSTANCE_HIST_CHKPT_LAT_USEC);
            break;
        case RAFT_LREG_FOLLOWER_VSTATS:
            lreg_value_fill_varray(lv, "follower-stats",
                                   LREG_USER_TYPE_RAFT_PEER_STATS,
                                   raft_num_members_validate_and_get(ri) - 1,
                                   raft_instance_lreg_peer_vstats_cb);
            break;
        default:
            break;
        }
        break;

// Write VAL
    case LREG_NODE_CB_OP_WRITE_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case RAFT_LREG_SYNC_FREQ_US:
            raft_server_set_sync_freq(ri, lv);
            break;
        case RAFT_LREG_CHKPT_IDX:
            ri->ri_user_requested_checkpoint = true;
            break;
        case RAFT_LREG_LOWEST_IDX:
            ri->ri_user_requested_reap = true;
            break;
        default:
            rc = -EPERM;
            break;
        }

    default:
        rc = -EOPNOTSUPP;
        break;
    }

    return rc;
}

enum raft_peer_stats_items
{
    RAFT_PEER_STATS_ITEM_UUID,
//    RAFT_PEER_STATS_LAST_SEND,
    RAFT_PEER_STATS_LAST_ACK,
    RAFT_PEER_STATS_MS_SINCE_LAST_ACK,
#if 0
//    RAFT_PEER_STATS_BYTES_SENT,
//    RAFT_PEER_STATS_BYTES_RECV,
#endif
    RAFT_PEER_STATS_SYNCED_LOG_IDX,
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
    case RAFT_PEER_STATS_MS_SINCE_LAST_ACK:
        lreg_value_fill_signed(lv, "ms-since-last-ack",
                               timespec_2_msec(&rfi->rfi_last_ack) ?
                               (niova_realtime_coarse_clock_get_msec() -
                                timespec_2_msec(&rfi->rfi_last_ack)) : -1ULL);
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
    case RAFT_PEER_STATS_SYNCED_LOG_IDX:
        lreg_value_fill_signed(lv, "sync-idx", rfi->rfi_synced_idx);
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

    raft_peer_t peer;

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

        // This peer is not listed in the follower output.
        peer = lrn->lrn_lvd.lvd_index +
            (lrn->lrn_lvd.lvd_index >= raft_server_instance_self_idx(ri) ?
             1 : 0);

        NIOVA_ASSERT(raft_member_idx_is_valid(ri, peer));

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
        NIOVA_ASSERT(re_idx < 0);
    else
        NIOVA_ASSERT(re_idx >= 0);

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

    // Capture the approx time this entry will be stored
    niova_realtime_coarse_clock(&reh->reh_store_time);

    memset(reh->reh_pad, 0, RAFT_ENTRY_PAD_SIZE);

    memcpy(re->re_data, data, len);

    // Checksum the entire entry - including the 'data' section
    reh->reh_crc = raft_server_entry_calc_crc(re);
}

static bool
raft_server_entry_next_entry_is_valid(struct raft_instance *ri,
                                      const struct raft_entry_header *reh);

/**
 * raft_instance_update_newest_entry_hdr - the raft instance stores a copy of
 *    newest entry's header.  This function updates the raft instance with the
 *    contents of the provided entry_header.
 */
static void
raft_instance_update_newest_entry_hdr(
    struct raft_instance *ri,
    const struct raft_entry_header *reh,
    enum raft_instance_newest_entry_hdr_types type, const bool force)
{
    NIOVA_ASSERT(ri && reh);
    if (reh->reh_index < 0)
        return;  // ignore log blocks

    pthread_mutex_lock(&ri->ri_newest_entry_mutex);

    for (enum raft_instance_newest_entry_hdr_types i = RI_NEHDR__START;
         i < RI_NEHDR__END; i++)
    {
        if (type != RI_NEHDR_ALL && i != type)
            continue;

        struct raft_entry_header *tgt = &ri->ri_newest_entry_hdr[i];
        bool updated = false;

        if (force || reh->reh_index > tgt->reh_index)
        {
            updated = true;
            *tgt = *reh;
        }

        DBG_RAFT_ENTRY(LL_DEBUG, tgt, "dst (who=%s)", thread_name_get());
        DBG_RAFT_ENTRY(LL_DEBUG, reh, "src (updated=%s)",
                       updated ? "true" : "false");

        if (raft_server_does_synchronous_writes(ri))
            NIOVA_ASSERT(updated);
    }

    pthread_mutex_unlock(&ri->ri_newest_entry_mutex);

    // DBG_RAFT_INSTANCE() takes the mutex, don't deadlock
    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "");
}

static bool
raft_server_has_unsynced_entries(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    bool valid = true;

    pthread_mutex_lock(&ri->ri_newest_entry_mutex);

    const bool diff = memcmp(&ri->ri_newest_entry_hdr[RI_NEHDR_SYNC],
                             &ri->ri_newest_entry_hdr[RI_NEHDR_UNSYNC],
                             sizeof(struct raft_entry_header)) ? true : false;
    // Sanity check on the hdr state
    if (diff && ((ri->ri_newest_entry_hdr[RI_NEHDR_SYNC].reh_index >
                  ri->ri_newest_entry_hdr[RI_NEHDR_UNSYNC].reh_index) ||
                 (ri->ri_newest_entry_hdr[RI_NEHDR_SYNC].reh_term >
                  ri->ri_newest_entry_hdr[RI_NEHDR_UNSYNC].reh_term) ||
                 raft_server_does_synchronous_writes(ri)))
        valid = false;

    pthread_mutex_unlock(&ri->ri_newest_entry_mutex);

    DBG_RAFT_INSTANCE_FATAL_IF((!valid), ri, "invalid ri newest entries");

    return diff;
}

static void
raft_server_entry_write_by_store(
    struct raft_instance *ri, const struct raft_entry *re,
    const struct raft_net_sm_write_supplements *ws)
{
    struct timespec io_op[2];
    niova_unstable_clock(&io_op[0]);

    ri->ri_backend->rib_entry_write(ri, re, ws);

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
                        enum raft_write_entry_opts opts,
                        const struct raft_net_sm_write_supplements *ws)
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

    raft_server_entry_write_by_store(ri, re, ws);

    /* Following the successful writing and sync of the entry, copy the
     * header contents into the raft instance.   Note, this is a noop if the
     * entry is for a log header.
     */
    enum raft_instance_newest_entry_hdr_types type =
        raft_server_does_synchronous_writes(ri) ?
        RI_NEHDR_ALL : RI_NEHDR_UNSYNC;

    raft_instance_update_newest_entry_hdr(ri, &re->re_header, type, false);

    niova_free(re);

    if (raft_server_does_synchronous_writes(ri))
        DBG_RAFT_INSTANCE_FATAL_IF((raft_server_has_unsynced_entries(ri)), ri,
                                   "raft_server_has_unsynced_entries() fails");

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

    // Unsynced entries are available for reading from the backend
    else if (!raft_instance_is_booting(ri) &&
             raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC) <
             reh_index)
        return -ERANGE;

    reh->reh_index = reh_index;

    struct timespec io_op[2];
    niova_unstable_clock(&io_op[0]);

    int rc = ri->ri_backend->rib_entry_header_read(ri, reh);

    FATAL_IF((rc), "rib_entry_header_read(): %s", strerror(-rc));

    niova_unstable_clock(&io_op[1]);

    struct binary_hist *bh =
        &ri->ri_rihs[RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC].rihs_bh;

    const long long elapsed_usec =
        (long long)(timespec_2_usec(&io_op[1]) - timespec_2_usec(&io_op[0]));

    if (elapsed_usec > 0)
        binary_hist_incorporate_val(bh, elapsed_usec);

    DBG_RAFT_ENTRY(LL_DEBUG, reh, "elapsed_usec=%lld", elapsed_usec);

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

static void
raft_server_incorporate_latency_measurement(struct raft_instance *ri,
                                            struct timespec start,
                                            struct timespec end,
                                            enum raft_instance_hist_types ht)
{
    struct binary_hist *bh =
        &ri->ri_rihs[ht].rihs_bh;

    const long long elapsed_usec =
        (long long)(timespec_2_usec(&end) - timespec_2_usec(&start));

    if (elapsed_usec > 0)
        binary_hist_incorporate_val(bh, elapsed_usec);
}

/**
 * raft_server_backend_sync - reentrant function which can be called from the
 *    main raft thread and the sync-thread.
 */
static int
raft_server_backend_sync(struct raft_instance *ri, const char *caller)
{
    if (!ri)
        return -EINVAL;

    if (raft_server_does_synchronous_writes(ri))
    {
        DBG_RAFT_INSTANCE_FATAL_IF((raft_server_has_unsynced_entries(ri)), ri,
                                   "raft_server_has_unsynced_entries() true");
        return 0;
    }
    else
    {
        NIOVA_ASSERT(ri->ri_backend->rib_backend_sync);
    }

    // Grab the unsync'd header contents
    struct raft_entry_header unsync_reh = {0};
    raft_instance_get_newest_header(ri, &unsync_reh, RI_NEHDR_UNSYNC);

    const raft_entry_idx_t sync_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC);

    NIOVA_ASSERT(sync_idx <= unsync_reh.reh_index);

    binary_hist_incorporate_val(
        &ri->ri_rihs[RAFT_INSTANCE_HIST_NENTRIES_SYNC].rihs_bh,
        unsync_reh.reh_index - sync_idx);

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "caller=%s nentries-this-sync=%ld",
                      caller, unsync_reh.reh_index - sync_idx);

    struct timespec io_op[2];
    niova_unstable_clock(&io_op[0]);

    int rc = ri->ri_backend->rib_backend_sync(ri);

    niova_unstable_clock(&io_op[1]);

    raft_server_incorporate_latency_measurement(
        ri, io_op[0], io_op[1], RAFT_INSTANCE_HIST_DEV_SYNC_LAT_USEC);

    DBG_RAFT_INSTANCE(LL_DEBUG, ri, "caller=%s rib_backend_sync(): %s",
                      caller, strerror(-rc));

    if (!rc) // Copy the contents of the current unsynced header to the synced
        raft_instance_update_newest_entry_hdr(ri, &unsync_reh, RI_NEHDR_SYNC,
                                              false);

    return rc;
}

static void
raft_server_backend_sync_pending(struct raft_instance *ri, const char *caller)
{
    const bool unsynced_entries = raft_server_has_unsynced_entries(ri);

    int rc = unsynced_entries ? raft_server_backend_sync(ri, caller) : 0;

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_backend_sync(): %s",
                               strerror(-rc));
#if 0
    // Schedule the main thread to issue AE requests to followers
    if (unsynced_entries && !rc)
        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_REMOTE_SEND]);
#endif
}

static int
raft_server_log_header_write(struct raft_instance *ri,
                             const uuid_t candidate, int64_t candidate_term)
{
    if (!ri || !ri->ri_csn_raft)
        return -EINVAL;

    raft_server_log_header_write_prep(ri, candidate, candidate_term);

    // Calls to rib_header_write() must be followed by a sync
    int rc = ri->ri_backend->rib_header_write(ri);

    return rc ? rc : raft_server_backend_sync(ri, __func__);
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

    case RAFT_INSTANCE_STORE_ROCKSDB: // fall through
    case RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP:
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

    pthread_mutex_lock(&ri->ri_newest_entry_mutex);

    for (enum raft_instance_newest_entry_hdr_types i = RI_NEHDR__START;
         i < RI_NEHDR__END; i++)
    {
        memset(&ri->ri_newest_entry_hdr[i], 0,
               sizeof(struct raft_entry_header));

        ri->ri_newest_entry_hdr[i].reh_index = -1ULL;
    }

    pthread_mutex_unlock(&ri->ri_newest_entry_mutex);
}

/**
 * raft_server_entry_next_entry_is_valid - this function is used when a caller
 *    wants to verify that an entry header correctly falls into the raft log
 *    sequence.  The function compares the prospective header with the known
 *    newest log header, ri->ri_newest_entry_hdr_unsynced.
 * @ri:  raft instance
 * @next_reh:  the raft entry header being validated
 * NOTES:  function considers the UNSYNCED newest value.
 */
static bool
raft_server_entry_next_entry_is_valid(struct raft_instance *ri,
                                      const struct raft_entry_header *next_reh)
{
    NIOVA_ASSERT(ri && next_reh);

    if (next_reh->reh_index < 0)
        return true;

    struct raft_entry_header unsync_hdr = {0};
    raft_instance_get_newest_header(ri, &unsync_hdr, RI_NEHDR_UNSYNC);

    /* A null UUID means ri_newest_entry_hdr is uninitialized, otherwise,
     * the expected index is the 'newest' + 1.
     */
    const raft_entry_idx_t expected_raft_unsync_index =
        unsync_hdr.reh_index + 1;

    if (raft_server_does_synchronous_writes(ri))
    {
        /* Sync write case - there is no sync thread operating on the ri so
         * no lock is needed to when comparing the newest sync and unsync
         * headers.
         */
        const raft_entry_idx_t expected_sync_idx =
            raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC) + 1;

        DBG_RAFT_INSTANCE_FATAL_IF(
            (expected_raft_unsync_index != expected_sync_idx), ri,
            "sync and unsync next indices are not equal");
    }

    // The index must increase by '1' and the term must never decrease.
    if (next_reh->reh_index != expected_raft_unsync_index ||
        (next_reh->reh_term < unsync_hdr.reh_term))
    {
        DBG_RAFT_ENTRY(LL_ERROR, &unsync_hdr, "invalid entry");
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "");

        return false;
    }

    return true;
}

static int
raft_server_entries_scan_internal(struct raft_instance *ri,
                                  const raft_entry_idx_t start,
                                  const raft_entry_idx_t max)
{
    int rc = 0;

    struct raft_entry_header reh;

    for (raft_entry_idx_t i = start; i < max; i++)
    {
        rc = raft_server_entry_header_read_by_store(ri, &reh, i);

        DBG_RAFT_ENTRY(LL_WARN, &reh, "i=%lx rc=%d", i, rc);
        if (rc)
        {
            DBG_RAFT_ENTRY(LL_DEBUG, &reh,
                           "raft_server_entry_header_read_by_store():  %s",
                           strerror(-rc));
            break;
        }

        /* Skip the validity check on the first iteration when starting_entry
         * is set.
         */
        else if (start && i > start)
        {
            if (!raft_server_entry_next_entry_is_valid(ri, &reh))
            {
                rc = -EINVAL;
                DBG_RAFT_ENTRY(
                    LL_WARN, &reh,
                    "raft_server_entry_next_entry_is_valid() false");
                break;
            }
        }

        /* During startup, sync and unsynced should be equivalent since all
         * found entries are considered to be synced and 'synced' status for
         * a raft instance occurs when the unsynced-idx == synced-idx.
         */
        raft_instance_update_newest_entry_hdr(ri, &reh, RI_NEHDR_ALL, false);
    }

    return rc;
}

/**
 * raft_server_entries_scan - reads through the non-header log entries to the
 *    log's end with the purpose of finding the latest valid entry.
 */
static int
raft_server_entries_scan(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    const raft_entry_idx_t entry_max_idx = ri->ri_entries_detected_at_startup;
    if (!entry_max_idx)
        return 0;

    else if (entry_max_idx < 0)
        return (int)entry_max_idx;

    /* Ensure the start of the key space is intact since it may not be
     * otherwise checked due to raftPersistentAppMaxScanEntries.
     */
#define LOG_INITIAL_SCAN_SZ 1000UL
    raft_entry_idx_t lowest_idx = niova_atomic_read(&ri->ri_lowest_idx);

    int rc = raft_server_entries_scan_internal(
        ri, lowest_idx, lowest_idx + LOG_INITIAL_SCAN_SZ);
    if (rc)
        return rc;

    // Reinit
    raft_instance_initialize_newest_entry_hdr(ri);

    raft_entry_idx_t starting_entry =
        (ri->ri_store_type == RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP &&
         raftPersistentAppMaxScanEntries > 0 &&
         entry_max_idx > raftPersistentAppMaxScanEntries)
        ? starting_entry = entry_max_idx - raftPersistentAppMaxScanEntries
        : lowest_idx;

    return raft_server_entries_scan_internal(ri, starting_entry,
                                             entry_max_idx);
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
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC) + 1;

    NIOVA_ASSERT(trunc_entry_idx >= 0);

    ri->ri_backend->rib_log_truncate(ri, trunc_entry_idx);

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "new-max-raft-idx=%ld", trunc_entry_idx);
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

static unsigned int
raft_election_timeout_lower_bound(const struct raft_instance *ri)
{
    return (unsigned int)(ri->ri_election_timeout_max_ms /
                          RAFT_ELECTION_RANGE_DIVISOR);
}

static unsigned int
raft_election_timeout_calc(const struct raft_instance *ri)
{
    unsigned int halved_timeout = raft_election_timeout_lower_bound(ri);

    return (halved_timeout + (random_get() % halved_timeout));
}

static void
raft_election_timeout_set(const struct raft_instance *ri, struct timespec *ts)
{
    if (!ts)
        return;

    unsigned long long msec = raft_election_timeout_calc(ri);

    msec_2_timespec(ts, msec);
}

static void
raft_heartbeat_timeout_sec(const struct raft_instance *ri, struct timespec *ts)
{
    unsigned long long msec = (ri->ri_election_timeout_max_ms /
                               ri->ri_heartbeat_freq_per_election_min);

    NIOVA_ASSERT(msec >= RAFT_HEARTBEAT__MIN_TIME_MS);

    msec_2_timespec(ts, msec);
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
        raft_heartbeat_timeout_sec(ri, &its.it_value);
        its.it_interval = its.it_value;
    }
    else
    {
        raft_election_timeout_set(ri, &its.it_value);
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
raft_server_send_msg_to_client(struct raft_instance *ri,
                               struct raft_net_client_request_handle *rncr,
                               struct ctl_svc_node *csn)
{
    NIOVA_ASSERT(ri && rncr);

    if (!ri || !rncr || !rncr->rncr_reply)
        return -EINVAL;

    const ssize_t msg_size = (sizeof(struct raft_client_rpc_msg) +
                              rncr->rncr_reply->rcrm_data_size);
    struct iovec iov[1] = {
        [0].iov_len = msg_size,
        [0].iov_base = rncr->rncr_reply,
    };

    if (csn)
        return raft_net_send_msg(ri, csn, iov, 1, RAFT_UDP_LISTEN_CLIENT);
    else
        return raft_net_send_msg_to_uuid(ri, rncr->rncr_client_uuid, iov, 1,
                                         RAFT_UDP_LISTEN_CLIENT);
}

static int
raft_server_send_msg(struct raft_instance *ri,
                     const enum raft_udp_listen_sockets sock_src,
                     struct ctl_svc_node *rp, const struct raft_rpc_msg *rrm)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (rp->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER)
        NIOVA_ASSERT(sock_src == RAFT_UDP_LISTEN_SERVER);
    else
        NIOVA_ASSERT(sock_src == RAFT_UDP_LISTEN_CLIENT);

    size_t msg_size = sizeof(struct raft_rpc_msg);
    if (rrm->rrm_type == RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST)
        msg_size += rrm->rrm_append_entries_request.raerqm_entries_sz;

    struct iovec iov = {
        .iov_len = msg_size,
        .iov_base = (void *)rrm
    };

    return raft_net_send_msg(ri, rp, &iov, 1, sock_src);
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
raft_server_candidate_is_viable(struct raft_instance *ri)
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

static void
raft_server_set_uuids_in_rpc_msg(const struct raft_instance *ri,
                                 struct raft_rpc_msg *rrm)
{
    if (ri && rrm)
    {
        uuid_copy(rrm->rrm_sender_id, RAFT_INSTANCE_2_SELF_UUID(ri));
        uuid_copy(rrm->rrm_raft_id, RAFT_INSTANCE_2_RAFT_UUID(ri));
        uuid_copy(rrm->rrm_db_id, ri->ri_db_uuid);
    }
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

    if (FAULT_INJECT(raft_candidate_state_disabled))
        return;

    // Reset vote counters
    raft_server_init_candidate_state(ri);

    // raft_server_vote_for_self() must sync ALL raft contents
    int rc = raft_server_vote_for_self(ri);

    if (rc) // Failed to sync our own log header!
        DBG_RAFT_INSTANCE(LL_FATAL, ri, "raft_server_log_header_write(): %s",
                          strerror(-rc));

    // Get the latest entry header following the self-vote
    struct raft_entry_header sync_hdr = {0};
    raft_instance_get_newest_header(ri, &sync_hdr, RI_NEHDR_SYNC);

    struct raft_rpc_msg rrm = {
        //.rrm_rrm_sender_id = ri->ri_csn_this_peer.csn_uuid,
        .rrm_type = RAFT_RPC_MSG_TYPE_VOTE_REQUEST,
        .rrm_version = 0,
        .rrm_vote_request.rvrqm_proposed_term = ri->ri_log_hdr.rlh_term,
        .rrm_vote_request.rvrqm_last_log_term = sync_hdr.reh_term,
        .rrm_vote_request.rvrqm_last_log_index = sync_hdr.reh_index,
    };

    raft_server_set_uuids_in_rpc_msg(ri, &rrm);

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
        NIOVA_ASSERT(new_term >= ri->ri_log_hdr.rlh_term);
    else
        NIOVA_ASSERT(new_term > ri->ri_log_hdr.rlh_term);

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
raft_leader_has_applied_txn_in_my_term(struct raft_instance *ri)
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
static raft_server_net_cb_ctx_t
raft_server_leader_init_state(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    // Grab the current sync header
    struct raft_entry_header sync_hdr = {0};
    raft_instance_get_newest_header(ri, &sync_hdr, RI_NEHDR_SYNC);

    // The server should have synced its state prior and not accepted new AE
    DBG_RAFT_INSTANCE_FATAL_IF((raft_server_has_unsynced_entries(ri)), ri,
                               "raft_server_has_unsynced_entries() is true");

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
    rls->rls_initial_term_idx = sync_hdr.reh_index;

    for (raft_peer_t i = 0; i < num_raft_peers; i++)
    {
        struct raft_follower_info *rfi = raft_server_get_follower_info(ri, i);

        rfi->rfi_next_idx = sync_hdr.reh_index + 1;
        rfi->rfi_prev_idx_term = sync_hdr.reh_term;
        rfi->rfi_prev_idx_crc = sync_hdr.reh_crc;
        rfi->rfi_current_idx_term = -1ULL;
        rfi->rfi_current_idx_crc = 0;
#if SYNC_IDX_BUG
        rfi->rfi_synced_idx = sync_hdr.reh_index;
#else
        rfi->rfi_synced_idx = -1ULL;
#endif
    }
}

/**
 * raft_server_write_next_entry - called from leader and follower context.
 *    Leader writes differ from follower writes in that they always are current
 *    and they may provide a write-supplement set.
 * @ri:  raft-instance pointer
 * @term:  term in which the entry was originally written - which may not be
 *    the current term.
 * @data:  raft-entry data
 * @len:  length of the raft-entry data
 * @opts:  options flags
 * @ws:  write supplements - currently, only used on the leader in entry_write
 *    context, however, both leaders and followers may make use of write-supp
 *    when performing SM applies.
 */
static raft_net_cb_ctx_t
raft_server_write_next_entry(struct raft_instance *ri, const int64_t term,
                             const char *data, const size_t len,
                             enum raft_write_entry_opts opts,
                             const struct raft_net_sm_write_supplements *ws)
{
    struct raft_entry_header unsync_hdr = {0};
    raft_instance_get_newest_header(ri, &unsync_hdr, RI_NEHDR_UNSYNC);

    NIOVA_ASSERT(term >= unsync_hdr.reh_term);

    const raft_entry_idx_t next_entry_idx = unsync_hdr.reh_index + 1;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri,
                      "next-entry-idx=%ld term=%ld len=%zd opts=%d",
                      next_entry_idx, term, len, opts);

    DBG_RAFT_INSTANCE_FATAL_IF((next_entry_idx < 0), ri,
                               "negative next-entry-idx=%ld", next_entry_idx);

    int rc = raft_server_entry_write(ri, next_entry_idx, term, data, len, opts,
                                     ws);
    if (rc)
        DBG_RAFT_INSTANCE(LL_FATAL, ri, "raft_server_entry_write(): %s",
                          strerror(-rc));
}

static raft_net_cb_ctx_t
raft_server_leader_write_new_entry(
    struct raft_instance *ri, const char *data, const size_t len,
    enum raft_write_entry_opts opts,
    const struct raft_net_sm_write_supplements *ws)
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
    raft_server_write_next_entry(ri, ri->ri_log_hdr.rlh_term, data, len, opts,
                                 ws);

    // Schedule ourselves to send this entry to the other members
    ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_REMOTE_SEND]);
}

static raft_server_net_cb_leader_t
raft_server_write_leader_change_marker(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_leader(ri));

    raft_server_leader_write_new_entry(ri, NULL, 0,
                                       RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER,
                                       NULL);
}

static void
raft_server_set_leader_csn(struct raft_instance *ri,
                           struct ctl_svc_node *leader_csn);

static raft_server_net_cb_ctx_t
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
static raft_server_net_cb_ctx_t
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

    // next_idx of '0' means no blocks have ever been written.
    if (rfi->rfi_next_idx == 0)
    {
        rfi->rfi_prev_idx_term = 0;
        rfi->rfi_current_idx_term = -1;
    }

    /* Grab the current idx info if the follower is behind.
     */
    const int64_t my_raft_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC);

    const bool refresh_prev = rfi->rfi_prev_idx_term < 0 ? true : false;
#if 0
    const bool refresh_current =
        (my_raft_idx >= rfi->rfi_next_idx &&
         (refresh_prev || rfi->rfi_current_idx_term < 0)) ? true : false;
#else
    const bool refresh_current = my_raft_idx >=
        rfi->rfi_next_idx ? true : false;
#endif

    struct raft_entry_header reh = {0};

    if (refresh_prev)
    {
        const int64_t follower_prev_entry_idx = rfi->rfi_next_idx - 1;

        NIOVA_ASSERT(follower_prev_entry_idx >= -1);

        // Test that the follower's prev-idx is not ahead of this leader's idx
        NIOVA_ASSERT(follower_prev_entry_idx <= my_raft_idx);

        int rc =
            raft_server_entry_header_read_by_store(ri, &reh,
                                                   follower_prev_entry_idx);

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

        int rc = raft_server_entry_header_read_by_store(ri, &reh,
                                                        rfi->rfi_next_idx);
        DBG_RAFT_INSTANCE_FATAL_IF(
            (rc), ri, "raft_server_entry_header_read_by_store(%ld): %s",
            rfi->rfi_next_idx, strerror(-rc));

        rfi->rfi_current_idx_term = reh.reh_term;
        rfi->rfi_current_idx_crc = reh.reh_crc;
    }

    DBG_RAFT_INSTANCE(
        ((refresh_prev || refresh_current) ? LL_NOTIFY : LL_DEBUG), ri,
        "peer=%hhx refresh=%d:%d pti=%ld:%ld si=%ld ct=%ld ccrc=%lu",
        follower, refresh_prev, refresh_current, rfi->rfi_prev_idx_term,
        rfi->rfi_next_idx, rfi->rfi_synced_idx, rfi->rfi_current_idx_term,
        rfi->rfi_current_idx_crc);

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

    raft_server_set_uuids_in_rpc_msg(ri, rrm);

    struct raft_append_entries_request_msg *raerq =
        &rrm->rrm_append_entries_request;

    int rc = raft_server_refresh_follower_prev_log_term(ri, follower);

    DBG_RAFT_INSTANCE_FATAL_IF(
        (rc), ri, "raft_server_refresh_follower_prev_log_term() %s",
        strerror(-rc));
    raerq->raerqm_heartbeat_msg = heartbeat ? 1 : 0;

    raerq->raerqm_leader_term = raft_server_leader_get_current_term(ri);
    raerq->raerqm_commit_index = ri->ri_commit_idx;
    raerq->raerqm_log_term = rfi->rfi_current_idx_term;
    raerq->raerqm_this_idx_crc = rfi->rfi_current_idx_crc;
    raerq->raerqm_entries_sz = 0;
    raerq->raerqm_leader_change_marker = 0;
    raerq->raerqm_prev_idx_crc = rfi->rfi_prev_idx_crc;
    raerq->raerqm_lowest_index = ri->ri_lowest_idx;
    raerq->raerqm_chkpt_index = ri->ri_checkpoint_last_idx;

    // Previous log index is the address of the follower's last write.
    raerq->raerqm_prev_log_index = rfi->rfi_next_idx - 1;

    // OK to copy the rls_prev_idx_term[] since it was refreshed above.
    raerq->raerqm_prev_log_term = rfi->rfi_prev_idx_term;
}

static raft_server_epoll_remote_sender_t
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

//XXX if leader and followers still have sync ops pending, wake up at a higher
//    frequency
    raft_server_timerfd_settime(ri);
}

/**
 * raft_server_process_vote_request_decide - determine if this peer should
 *    vote for the candidate.
 */
static bool
raft_server_process_vote_request_decide(
    const struct raft_instance *ri, const struct raft_vote_request_msg *vreq,
    const struct raft_entry_header *cmp_hdr)
{
    NIOVA_ASSERT(ri && vreq && cmp_hdr);

    // "allow at most one winner per term"
    return (vreq->rvrqm_proposed_term <= ri->ri_log_hdr.rlh_term ||
            vreq->rvrqm_last_log_term < cmp_hdr->reh_term ||
            vreq->rvrqm_last_log_index < cmp_hdr->reh_index) ? false : true;
}

/**
 * raft_server_process_vote_request - peer has requested that we vote for
 *    them.
 */
static raft_server_net_cb_ctx_t
raft_server_process_vote_request(struct raft_instance *ri,
                                 struct ctl_svc_node *sender_csn,
                                 const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);

    // The caller *should* have already checked this.
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    const struct raft_vote_request_msg *vreq = &rrm->rrm_vote_request;

    struct raft_rpc_msg rreply_msg = {0};

    // Make a decision based on the synced status of the log
    raft_server_backend_sync_pending(ri, __func__);

    struct raft_entry_header sync_hdr = {0};
    raft_instance_get_newest_header(ri, &sync_hdr, RI_NEHDR_SYNC);

    /* Do some initialization on the reply message.
     */
    raft_server_set_uuids_in_rpc_msg(ri, &rreply_msg);

    rreply_msg.rrm_type = RAFT_RPC_MSG_TYPE_VOTE_REPLY;
    rreply_msg.rrm_vote_reply.rvrpm_term = ri->ri_log_hdr.rlh_term;

    /* Check the vote criteria - do we vote 'yes' or 'no'?
     */
    rreply_msg.rrm_vote_reply.rvrpm_voted_granted =
        raft_server_process_vote_request_decide(ri, vreq, &sync_hdr) ? 1 : 0;

    DBG_RAFT_MSG(LL_NOTIFY, rrm, "vote=%s my term=%ld last=%ld:%ld",
                 rreply_msg.rrm_vote_reply.rvrpm_voted_granted ? "yes" : "no",
                 ri->ri_log_hdr.rlh_term, sync_hdr.reh_term,
                 sync_hdr.reh_index);

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
static raft_server_net_cb_follower_ctx_bool_t
raft_server_append_entry_check_already_stored(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);
    NIOVA_ASSERT(raft_instance_is_follower(ri));

    // raerqm_prev_log_index can be -1 if no writes have ever been done.
    NIOVA_ASSERT(raerq->raerqm_prev_log_index >= RAFT_MIN_APPEND_ENTRY_IDX);

    const raft_entry_idx_t raft_current_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC);

    const raft_entry_idx_t leaders_next_idx_for_me =
        raerq->raerqm_prev_log_index + 1;

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
        int rc = raft_server_entry_header_read_by_store(
            ri, &reh, leaders_next_idx_for_me);

        FATAL_IF((rc), "raft_server_header_entry_read(): %s", strerror(-rc));

        if (reh.reh_term != raerq->raerqm_log_term)
            return false;

        FATAL_IF((raerq->raerqm_this_idx_crc != reh.reh_crc),
                 "crc (%u) does not match leader (%u) for idx=%ld",
                 reh.reh_crc, raerq->raerqm_this_idx_crc,
                 leaders_next_idx_for_me);
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
        int rc = raft_server_entry_header_read_by_store(
            ri, &reh, raerq->raerqm_prev_log_index);

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
static raft_server_net_cb_follower_ctx_t
raft_server_append_entry_log_prune_if_needed(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);
    NIOVA_ASSERT(raft_instance_is_follower(ri));
    // This value must have already been checked by the caller.
    NIOVA_ASSERT(raerq->raerqm_prev_log_index >= RAFT_MIN_APPEND_ENTRY_IDX);

    const int64_t raft_entry_idx_prune = raerq->raerqm_prev_log_index + 1;

    /* Forcing a sync here will prevent the sync-thread from persisting new
     * log entries which may be beyond raerqm_prev_log_index (which is where
     * we're truncating to..).
     */
    int rc = raft_server_backend_sync(ri, __func__);
    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_backend_sync(): %s",
                               strerror(-rc));

    // We must not prune already committed transactions.
    DBG_RAFT_INSTANCE_FATAL_IF(
        (ri->ri_commit_idx >= raft_entry_idx_prune ||
         ri->ri_last_applied_idx >= raft_entry_idx_prune),
        ri, "cannot prune committed entry raerq-nli=%ld",
        raft_entry_idx_prune);

    if (raerq->raerqm_prev_log_index >= 0)
    {
        struct raft_entry_header reh;

        /* Read the block at the leader's index and apply it to our header.
         * We don't call raft_server_entry_next_entry_is_valid() since the log
         * sequence had been verified already at startup.
         */
        int rc = raft_server_entry_header_read_by_store(
            ri, &reh, raerq->raerqm_prev_log_index);

        FATAL_IF((rc), "raft_server_entry_header_read_by_store(): %s",
                 strerror(-rc));

        raft_instance_update_newest_entry_hdr(ri, &reh, RI_NEHDR_ALL, true);
    }

    // truncate the log.
    raft_server_log_truncate(ri);
}

static raft_server_net_cb_follower_ctx_int_t
raft_server_follower_index_ahead_of_leader(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq,
    struct raft_entry_header *reh)
{
    NIOVA_ASSERT(ri && raerq && reh);

    int rc = 0;

    /* If this follower's index is ahead of the leader's then we must check
     * for a retried AE which has already been stored in our log.
     * Note that this AE may have been delayed in the network or may have
     * retried due to a dropped reply.  It's important that we try to ACK
     * this request and not proceed with modifying our log.
     */
    if (raft_server_append_entry_check_already_stored(ri, raerq))
    {
        rc = -EALREADY;
    }
    else // Otherwise, the log needs to be pruned.
    {
        raft_server_append_entry_log_prune_if_needed(ri, raerq);
        NIOVA_ASSERT(!raft_server_has_unsynced_entries(ri));
    }
    /* The log may have been synced and/or pruned - re-obtain the
     * current_idx.
     * Note:  if a sync occurred then the synced idx will be equivalent to
     *        the unsynced idx.
     */
    raft_instance_get_newest_header(ri, reh, RI_NEHDR_SYNC);

    return rc;
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
static raft_server_net_cb_follower_ctx_int_t
raft_server_append_entry_log_prepare_and_check(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);

    struct raft_entry_header reh = {0};
    raft_instance_get_newest_header(ri, &reh, RI_NEHDR_UNSYNC);

    // raft_server_follower_index_ahead_of_leader() may update reh
    int rc = (reh.reh_index > raerq->raerqm_prev_log_index) ?
        raft_server_follower_index_ahead_of_leader(ri, raerq, &reh) : 0;

    if (rc)
        return rc;

    // At this point, current_idx should not exceed the one from the leader.
    NIOVA_ASSERT(reh.reh_index <= raerq->raerqm_prev_log_index);

    /* In this case, the leader's and follower's indexes have yet to converge
     * which implies a "non_matching_prev_term" since the term isn't testable
     * until the indexes match.
     */
    if (reh.reh_index < raerq->raerqm_prev_log_index)
        rc = -ERANGE;

    /* Equivalent log indexes but the terms do not match.  Note that this cond
     * will likely lead to more pruning as the leader continues to decrement
     * its raerqm_prev_log_index value for this follower.
     */
    else if (reh.reh_term != raerq->raerqm_prev_log_term)
        rc = -EEXIST;

    DBG_RAFT_INSTANCE((raerq->raerqm_heartbeat_msg ? LL_DEBUG : LL_NOTIFY), ri,
                      "rci=%ld leader-prev-[idx:term]=%ld:%ld rc=%d",
                      reh.reh_index, raerq->raerqm_prev_log_index,
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
static raft_server_net_cb_ctx_int_t
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

    // Demote myself if candidate
    if (ri->ri_log_hdr.rlh_term <= raerq->raerqm_leader_term &&
        ri->ri_state == RAFT_STATE_CANDIDATE)
        raft_server_becomes_follower(ri, raerq->raerqm_leader_term,
                                     sender_csn->csn_uuid,
                                     RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE);

    // Demote myself if stale leader
    else if (ri->ri_log_hdr.rlh_term < raerq->raerqm_leader_term &&
             ri->ri_state == RAFT_STATE_LEADER)
        raft_server_becomes_follower(ri, raerq->raerqm_leader_term,
                                     sender_csn->csn_uuid,
                                     RAFT_BFRSN_STALE_TERM_WHILE_LEADER);

    // Apply leader csn pointer.
    raft_server_set_leader_csn(ri, sender_csn);

    return 0;
}

/**
 * raft_server_write_new_entry_from_leader - the log write portion of the
 *    AE operation.  The log index is derived from the raft-instance which
 *    must match the index provided by the leader in raerq,
 */
static raft_server_net_cb_follower_ctx_t
raft_server_write_new_entry_from_leader(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq)
{
    NIOVA_ASSERT(ri && raerq);
    NIOVA_ASSERT(raft_instance_is_follower(ri));

    if (raerq->raerqm_heartbeat_msg) // heartbeats don't enter the log
        return;

    struct raft_entry_header unsync_hdr = {0};
    raft_instance_get_newest_header(ri, &unsync_hdr, RI_NEHDR_UNSYNC);

    NIOVA_ASSERT(raerq->raerqm_log_term > 0);
    NIOVA_ASSERT(raerq->raerqm_log_term >= raerq->raerqm_prev_log_term);
    NIOVA_ASSERT(raerq->raerqm_log_term >= unsync_hdr.reh_term);

    const size_t entry_size = raerq->raerqm_entries_sz;

    // Msg size of '0' is OK.
    NIOVA_ASSERT(entry_size <= RAFT_ENTRY_MAX_DATA_SIZE);

    // Sanity check on the 'next' idx to be written.
    NIOVA_ASSERT(unsync_hdr.reh_index == raerq->raerqm_prev_log_index);

    enum raft_write_entry_opts opts = raerq->raerqm_leader_change_marker ?
        RAFT_WR_ENTRY_OPT_LEADER_CHANGE_MARKER : RAFT_WR_ENTRY_OPT_NONE;

    raft_server_write_next_entry(ri, raerq->raerqm_log_term,
                                 raerq->raerqm_entries, entry_size, opts,
                                 NULL);
}

/**
 * raft_server_process_append_entries_request_prep_reply - helper function for
 *    raft_server_process_append_entries_request() which does some general
 *    AE reply setup.
 */
static raft_server_net_cb_ctx_t
raft_server_process_append_entries_request_prep_reply(
    struct raft_instance *ri, struct raft_rpc_msg *reply,
    const struct raft_append_entries_request_msg *raerq,
    bool stale_term, bool non_matching_prev_term, const int rc)
{
    reply->rrm_type = RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY;

    reply->rrm_append_entries_reply.raerpm_leader_term =
        ri->ri_log_hdr.rlh_term;

    reply->rrm_append_entries_reply.raerpm_prev_log_index =
        raerq->raerqm_prev_log_index;

    reply->rrm_append_entries_reply.raerpm_heartbeat_msg =
        raerq->raerqm_heartbeat_msg;

    const raft_entry_idx_t current_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC);

    struct raft_append_entries_reply_msg *rae_reply =
        &reply->rrm_append_entries_reply;

    /* Issue #27 - explicitly tell the leader if we're newly initialized
     * to remove any ambiguity about the use of the raerpm_synced_log_index
     * value.  `raerpm_newly_initialized_peer == 0` will allow the leader to
     * set our next-idx to '0'.
     */
    rae_reply->raerpm_newly_initialized_peer =
        current_idx == ID_ANY_64bit ? 1 : 0;

    // Issue #27 - send synced-log-index in non_matching_prev_term case too
    rae_reply->raerpm_synced_log_index =
        (!rc || non_matching_prev_term) ? current_idx : ID_ANY_64bit;

    raft_server_set_uuids_in_rpc_msg(ri, reply);

    rae_reply->raerpm_err_stale_term = stale_term;
    rae_reply->raerpm_err_non_matching_prev_term = non_matching_prev_term;
}

static raft_server_net_cb_ctx_int_t
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

static raft_server_net_cb_ctx_t
raft_server_advance_commit_idx(struct raft_instance *ri,
                               const int64_t new_commit_idx)
{
    NIOVA_ASSERT(ri);

    /* This peer may be behind, don't advance the commit index past our
     * current raft index.
     */
    if (ri->ri_commit_idx < new_commit_idx &&
        (raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC) >=
         new_commit_idx))
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "new_commit_idx=%ld", new_commit_idx);

        ri->ri_commit_idx = new_commit_idx;

        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_SM_APPLY]);
    }
}

static raft_server_net_cb_ctx_t
raft_server_append_entry_reply_send(
    struct raft_instance *ri,
    const struct raft_append_entries_request_msg *raerq,
    struct ctl_svc_node *sender_csn, bool stale_term,
    bool non_matching_prev_term, const int ae_rc)
{
    struct raft_rpc_msg rreply_msg = {0};

    raft_server_process_append_entries_request_prep_reply(
        ri, &rreply_msg, raerq, stale_term, non_matching_prev_term, ae_rc);

    int rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, sender_csn,
                                  &rreply_msg);

    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_send_msg(): %s",
                               strerror(rc));
}

static raft_server_net_cb_ctx_t
raft_server_process_append_entries_request(struct raft_instance *ri,
                                           struct ctl_svc_node *sender_csn,
                                           const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    DBG_RAFT_MSG(LL_DEBUG, rrm, "");

    const struct raft_append_entries_request_msg *raerq =
        &rrm->rrm_append_entries_request;

    if (raft_server_process_append_entries_request_validity_check(raerq))
    {
        DBG_RAFT_MSG(
            LL_WARN, rrm,
            "raft_server_process_append_entries_request_validity_check() err");
        return;
    }

    // Try to update the term if the leader has a higher one.
    const int64_t leader_term = raerq->raerqm_leader_term;
    raft_server_try_update_log_header_null_voted_for_peer(ri, leader_term);

    // Candidate timer - reset if this operation is valid.
    bool reset_timerfd = true;
    bool fault_inject_ignore_ae = false;
    bool stale_term = false;
    bool non_matching_prev_term = false;

    int rc =
        raft_server_process_append_entries_term_check_ops(ri, sender_csn,
                                                          raerq);
    if (rc)
    {
        NIOVA_ASSERT(rc == -ESTALE);
        reset_timerfd = false;
        stale_term = true;
    }
    else
    {
        bool advance_commit_idx = false;
        raft_entry_idx_t new_commit_idx = raerq->raerqm_commit_index;

        rc = raft_server_append_entry_log_prepare_and_check(ri, raerq);
        if (rc)
        {
            if (rc == -EALREADY)
            {
                advance_commit_idx = true;
            }
            else
            {
                non_matching_prev_term = true;
                if (rc == -ERANGE)
                    advance_commit_idx = true;
            }

            // Issue #28 - advance the index of recently restarted server
            if (advance_commit_idx)
                new_commit_idx = MIN(new_commit_idx,
                                     raft_server_get_current_raft_entry_index(
                                         ri, RI_NEHDR_SYNC));
        }
        else
        {
            advance_commit_idx = true;
            if (!raerq->raerqm_heartbeat_msg &&
                !(fault_inject_ignore_ae =
                      FAULT_INJECT(raft_follower_ignores_AE)))
                raft_server_write_new_entry_from_leader(ri, raerq);
        }

        /* Update our commit-idx based on the value sent from the leader.
         * NOTE:  if synchronous mode is set then this will account for the
         * the write performed above, otherwise, only the sync'd writes to
         * this point are considered.
         */
        if (advance_commit_idx)
            raft_server_advance_commit_idx(ri, new_commit_idx);
    }

    // Issue reply
    if (!fault_inject_ignore_ae)
        raft_server_append_entry_reply_send(ri, raerq, sender_csn, stale_term,
                                            non_matching_prev_term, rc);

    if (reset_timerfd)
        raft_server_timerfd_settime(ri);
}

static raft_server_net_cb_leader_ctx_int64_t
raft_server_leader_calculate_committed_idx(struct raft_instance *ri,
                                           const bool sync_thread)
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

    struct raft_entry_header sync_hdr = {0};
    raft_instance_get_newest_header(ri, &sync_hdr, RI_NEHDR_SYNC);

    self->rfi_next_idx = sync_hdr.reh_index + 1;
    self->rfi_synced_idx = sync_hdr.reh_index;
    self->rfi_prev_idx_term = sync_hdr.reh_term;

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
                 rfi->rfi_synced_idx < sorted_indexes[i]))
            {
                sorted_indexes[i] = rfi->rfi_synced_idx + 1;
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

//XXX the bug here is that we may get valid values but not from the committing
//    majority and therefore we assert..
    /* The leader still has not obtained the sync_idx values from a majority
     * of its followers.  Also, ensure the ri_commit_idx is not moving
     * backwards but exempt the sync thread since it may have a stale view.
     */
    if (committed_raft_idx && !sync_thread &&
        (committed_raft_idx < ri->ri_commit_idx))
        DBG_RAFT_INSTANCE(LL_WARN, ri,
                          "committed_raft_idx (%ld) < ri_commit_idx",
                          committed_raft_idx);

    return committed_raft_idx;
}

static int64_t
raft_server_leader_can_advance_commit_idx(struct raft_instance *ri,
                                          const bool sync_thread)
{
    NIOVA_ASSERT(ri);
    if (!raft_instance_is_leader(ri))
        return ID_ANY_64bit;

    const struct raft_leader_state *rls = &ri->ri_leader;

    const int64_t committed_raft_idx =
        raft_server_leader_calculate_committed_idx(ri, sync_thread);

    DBG_RAFT_INSTANCE_FATAL_IF( // Note:  sync_thread view may be stale
        (!sync_thread && committed_raft_idx < ri->ri_commit_idx &&
         committed_raft_idx >= rls->rls_initial_term_idx), ri,
        "committed_raft_idx (%ld) < ri_commit_idx after initial term commit",
        committed_raft_idx);

    /* Only increase the commit index if the majority has ACKd this leader's
     * "leader_change_marker" AE.
     */
    return (committed_raft_idx >= rls->rls_initial_term_idx &&
            committed_raft_idx > ri->ri_commit_idx) ?
        committed_raft_idx : ID_ANY_64bit;
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
static raft_server_net_cb_leader_ctx_t // or raft_server_epoll_t
raft_server_leader_try_advance_commit_idx(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    NIOVA_ASSERT(raft_instance_is_leader(ri));

    const int64_t committed_raft_idx =
        raft_server_leader_can_advance_commit_idx(ri, false);

    if (committed_raft_idx != ID_ANY_64bit)
        raft_server_advance_commit_idx(ri, committed_raft_idx);
}

static raft_server_sync_thread_ctx_t
raft_server_leader_try_advance_commit_idx_from_sync_thread(
    struct raft_instance *ri)
{
    if (ri && raft_server_leader_can_advance_commit_idx(ri, true) !=
        ID_ANY_64bit)
        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_ASYNC_COMMIT_IDX_ADV]);
}

static raft_server_net_cb_leader_ctx_t
raft_server_try_update_follower_sync_idx(struct raft_instance *ri,
                                         const raft_peer_t follower_idx,
                                         const int64_t sync_idx,
                                         const char *caller)
{
    NIOVA_ASSERT(ri && follower_idx != RAFT_PEER_ANY);

    struct raft_follower_info *rfi =
        raft_server_get_follower_info(ri, follower_idx);

    NIOVA_ASSERT(rfi && rfi->rfi_synced_idx <= rfi->rfi_next_idx);

    if (rfi->rfi_synced_idx < sync_idx)
    {
        rfi->rfi_synced_idx = sync_idx;

         DBG_RAFT_INSTANCE(LL_NOTIFY, ri,
                           "follower=%x new-sync-idx=%ld caller=%s",
                           follower_idx, rfi->rfi_synced_idx, caller);

        // if this request increases the remote's rfi_synced_idx..
        raft_server_leader_try_advance_commit_idx(ri);
    }
}

static raft_server_net_cb_leader_ctx_t
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

    struct timespec la = rfi->rfi_last_ack;

    // Update the last ack value for this follower.
    niova_realtime_coarse_clock(&rfi->rfi_last_ack);

    DBG_RAFT_INSTANCE(
        (raerp->raerpm_heartbeat_msg ? LL_DEBUG : LL_NOTIFY), ri,
        "flwr=%x next-idx=%ld si=%ld err=%hhx rp-pli=%ld rp-si=%ld la-ms=%lld",
        follower_idx, rfi->rfi_next_idx, rfi->rfi_synced_idx,
        raerp->raerpm_err_non_matching_prev_term,
        raerp->raerpm_prev_log_index, raerp->raerpm_synced_log_index,
        (timespec_2_msec(&rfi->rfi_last_ack) - timespec_2_msec(&la)));

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
            if (raerp->raerpm_newly_initialized_peer)
            {
                rfi->rfi_next_idx = 0;
            }
            else
            {
                rfi->rfi_next_idx =
                    (raerp->raerpm_synced_log_index >= 0 &&
                     raerp->raerpm_synced_log_index < rfi->rfi_next_idx)
                    ? raerp->raerpm_synced_log_index
                    : rfi->rfi_next_idx - 1;
            }

            rfi->rfi_prev_idx_term = -1; //Xxx this needs to go into a function
        }
    }
    else
    {
        // Heartbeats don't advance the follower index
        if (!raerp->raerpm_heartbeat_msg)
        {
            rfi->rfi_prev_idx_term = -1;
            rfi->rfi_next_idx++;

            DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "follower=%x new-next-idx=%ld",
                              follower_idx, rfi->rfi_next_idx);
        }

        raft_server_try_update_follower_sync_idx(
            ri, follower_idx, raerp->raerpm_synced_log_index, __func__);
    }

    if ((rfi->rfi_next_idx - 1) <
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC))
    {
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "follower=%x still lags next-idx=%ld",
                          follower_idx, rfi->rfi_next_idx);

        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_REMOTE_SEND]);
    }
}

static raft_server_net_cb_ctx_t
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

static raft_server_net_cb_ctx_t
raft_server_process_sync_idx_update(struct raft_instance *ri,
                                    struct ctl_svc_node *sender_csn,
                                    const struct raft_rpc_msg *rrm)
{
    NIOVA_ASSERT(ri && sender_csn && rrm);
    NIOVA_ASSERT(!ctl_svc_node_compare_uuid(sender_csn, rrm->rrm_sender_id));

    if (!raft_instance_is_leader(ri))
        return;

    /* Ignore the message if the term does not match.  Note that this term is
     * not for the sync'd index - it's the remote's current term value which
     * should match this leader's term.
     */
    else if (rrm->rrm_sync_index_update.rsium_term != ri->ri_log_hdr.rlh_term)
        return;

    raft_server_try_update_follower_sync_idx(
        ri, raft_peer_2_idx(ri, sender_csn->csn_uuid),
        rrm->rrm_sync_index_update.rsium_synced_log_index, __func__);
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

    case RAFT_RPC_MSG_TYPE_SYNC_IDX_UPDATE:
        return raft_server_process_sync_idx_update(ri, sender_csn, rrm);

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
    SIMPLE_FUNC_ENTRY(LL_TRACE);
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

static raft_server_net_cb_ctx_bool_t
raft_leader_instance_is_fresh(const struct raft_instance *ri)
{
    if (!raft_instance_is_leader(ri) ||
        FAULT_INJECT(raft_leader_may_be_deposed))
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
        if (timespeccmp(&now, &rfi->rfi_last_ack, <))
            continue;

        struct timespec diff;

        timespecsub(&now, &rfi->rfi_last_ack, &diff);

        if (timespec_2_msec(&diff) <= raft_election_timeout_lower_bound(ri))
            num_acked_within_window++;
    }

    SIMPLE_LOG_MSG(LL_DEBUG,
                   "num_acked_within_window: %lu required: %d (%d peers)",
                   num_acked_within_window, num_raft_peers / 2 + 1,
                   num_raft_peers);

    return (num_acked_within_window >= (num_raft_peers / 2 + 1)) ?
        true : false;
}

/**
 * raft_server_may_process_client_request - this function checks the state of
 *    this raft instance to determine if it's qualified to accept a client
 *    request.
 */
static raft_net_cb_ctx_int_t
raft_server_may_accept_client_request(struct raft_instance *ri)
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
raft_server_may_accept_client_request_reason(struct raft_instance *ri)
{
    int rc = raft_server_may_accept_client_request(ri);

    return raft_net_client_rpc_sys_error_2_string(rc);
}

static raft_net_cb_ctx_t
raft_server_reply_to_client(struct raft_instance *ri,
                            struct raft_net_client_request_handle *rncr,
                            struct ctl_svc_node *csn)
{
    if (!ri || !ri->ri_csn_this_peer || !ri->ri_csn_raft || !rncr ||
        !raft_net_client_request_handle_has_reply_info(rncr))
        return;

    /* Copy the reply info from the provided rncr pointer.  This reply info
     * fields have been written by the state_machine callback.
     */
    const struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    if (rncr->rncr_request)
        DBG_RAFT_CLIENT_RPC(LL_DEBUG, rncr->rncr_request, "original request");
    DBG_RAFT_CLIENT_RPC(LL_DEBUG, reply, "reply");

    int rc = raft_server_send_msg_to_client(ri, rncr, csn);
    if (rc)
        DBG_RAFT_CLIENT_RPC(LL_ERROR, reply,
                            "raft_server_send_msg(): %s", strerror(rc));
}

static raft_net_cb_ctx_t
raft_server_udp_client_deny_request(struct raft_instance *ri,
                                    struct raft_net_client_request_handle *rncr,
                                    struct ctl_svc_node *csn,
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

    return raft_server_reply_to_client(ri, rncr, csn);
}

/**
 * raft_server_client_reply_init - prepares a reply RPC by copying the
 *    relevant items from the original request.
 */
static raft_net_cb_ctx_t
raft_server_client_reply_init(const struct raft_instance *ri,
                              struct raft_net_client_request_handle *rncr,
                              enum raft_client_rpc_msg_type msg_type)
{
    NIOVA_ASSERT(ri && rncr && rncr->rncr_reply &&
                 (msg_type == RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY ||
                  msg_type == RAFT_CLIENT_RPC_MSG_TYPE_REPLY) &&
                 raft_net_client_request_handle_has_reply_info(rncr) &&
                 rncr->rncr_reply_data_size < rncr->rncr_reply_data_max_size);

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;
    memset(reply, 0, sizeof(struct raft_client_rpc_msg));

    uuid_copy(reply->rcrm_raft_id, ri->ri_csn_raft->csn_uuid);
    uuid_copy(reply->rcrm_sender_id, ri->ri_csn_this_peer->csn_uuid);
    uuid_copy(reply->rcrm_dest_id, rncr->rncr_client_uuid);

    reply->rcrm_type = msg_type;
    reply->rcrm_msg_id = rncr->rncr_msg_id;
    reply->rcrm_data_size = rncr->rncr_reply_data_size;
}

static raft_net_cb_ctx_bool_t
raft_server_client_recv_ignore_request(
    struct raft_instance *ri, const struct raft_client_rpc_msg *rcm,
    const struct sockaddr_in *from, struct ctl_svc_node **csn_out)
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
            DECLARE_AND_INIT_UUID_STR(sender_uuid,
                                      rcm->rcrm_sender_id);
            SIMPLE_LOG_MSG(LL_WARN, "ctl_svc_node_lookup(): %d uuid %s",
                           rc, sender_uuid);
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
            }
            else
            {
                cause = "UUID does not belong to a client";
                ignore_request = true;

                DBG_CTL_SVC_NODE(
                    LL_NOTIFY, client_csn,
                    "recv'd RPC request from this non-client UUID");
            }

            if (ignore_request)
                ctl_svc_node_put(client_csn);
            else
                *csn_out = client_csn;
        }
        else
        {
            cause = "CSN required";
            ignore_request = true;
        }
    }

    if (ignore_request)
        DBG_RAFT_CLIENT_RPC_SOCK(LL_NOTIFY, rcm, from, "%s (rc=%d)", cause, rc);

    return ignore_request;
}

static void // raft_net_cb_ctx_t or raft_server_epoll_sm_apply_bool_t
raft_server_net_client_request_init(
    const struct raft_instance *ri,
    struct raft_net_client_request_handle *rncr,
    enum raft_net_client_request_type type,
    const struct raft_client_rpc_msg *rpc_request,  const char *commit_data,
    const size_t commit_data_size, const struct sockaddr_in *from,
    char *reply_buf, const size_t reply_buf_size)
{
    NIOVA_ASSERT(ri && rncr && reply_buf &&
                 reply_buf_size >= sizeof(struct raft_client_rpc_msg));

    if (type == RAFT_NET_CLIENT_REQ_TYPE_NONE)
        FATAL_IF((!rpc_request || commit_data),
                 "invalid argument:  rpc_request may only be specified");
    else if (type == RAFT_NET_CLIENT_REQ_TYPE_COMMIT)
        FATAL_IF((rpc_request || !commit_data),
                 "invalid argument:  commit_data may only be specified");
    else
        FATAL_MSG("invalid request type (%d)", type);

    memset(rncr, 0, sizeof(struct raft_net_client_request_handle));

    rncr->rncr_write_raft_entry = false;
    rncr->rncr_type = type;

    rncr->rncr_is_leader = raft_instance_is_leader(ri) ? true : false;
    rncr->rncr_entry_term = ri->ri_log_hdr.rlh_term;
    rncr->rncr_current_term = ri->ri_log_hdr.rlh_term;

    rncr->rncr_reply = (struct raft_client_rpc_msg *)reply_buf;

    CONST_OVERRIDE(size_t, rncr->rncr_reply_data_max_size,
                   (reply_buf_size - sizeof(struct raft_client_rpc_msg)));

    if (rpc_request)
    {
        rncr->rncr_request = rpc_request;
        rncr->rncr_request_or_commit_data = rpc_request->rcrm_data;

        CONST_OVERRIDE(size_t, rncr->rncr_request_or_commit_data_size,
                       rpc_request->rcrm_data_size);

        /* These are reply specific items which are only provided when this
         * function is called from raft_net_udp_cb_ctx_t context.
         */
        raft_net_client_request_handle_set_reply_info(
            rncr, rpc_request->rcrm_sender_id, rpc_request->rcrm_msg_id);

        NIOVA_ASSERT(raft_net_client_request_handle_has_reply_info(rncr));
    }
    else
    {
        /* raft_net_client_request_handle_set_reply_info() must be called from
         * SM context if a post-commit reply is to be made.
         */
        rncr->rncr_request_or_commit_data = commit_data;

        CONST_OVERRIDE(size_t, rncr->rncr_request_or_commit_data_size,
                       commit_data_size);

        // Sanity check of raft_net_client_request_handle_has_reply_info()
        NIOVA_ASSERT(!raft_net_client_request_handle_has_reply_info(rncr));
    }
}

static raft_net_cb_ctx_t
raft_server_net_client_request_init_client_rpc(
    struct raft_instance *ri, struct raft_net_client_request_handle *rncr,
    const struct raft_client_rpc_msg *rpc_request,
    const struct sockaddr_in *from, char *reply_buf,
    const size_t reply_buf_size)
{
    NIOVA_ASSERT(ri && rncr && rpc_request);

    raft_server_net_client_request_init(ri, rncr,
                                        RAFT_NET_CLIENT_REQ_TYPE_NONE,
                                        rpc_request, NULL, 0, from, reply_buf,
                                        reply_buf_size);

    raft_server_client_reply_init(
        ri, rncr, (rpc_request->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_PING ?
                   RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY :
                   RAFT_CLIENT_RPC_MSG_TYPE_REPLY));
}

// warning: buffers are statically allocated, so code is not multi-thread safe
static raft_net_cb_ctx_t
raft_server_client_recv_handler(struct raft_instance *ri,
                                const char *recv_buffer,
                                ssize_t recv_bytes,
                                const struct sockaddr_in *from)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    static char reply_buf[RAFT_NET_MAX_RPC_SIZE];

    NIOVA_ASSERT(ri && from);

    if (!recv_buffer || !recv_bytes || !ri->ri_server_sm_request_cb ||
        recv_bytes < sizeof(struct raft_client_rpc_msg))
    {
        SIMPLE_LOG_MSG(LL_WARN, "sanity check fail, buf %p bytes %ld cb %p",
                       recv_buffer, recv_bytes, ri->ri_server_sm_request_cb);
        return;
    }

    const struct raft_client_rpc_msg *rcm =
        (const struct raft_client_rpc_msg *)recv_buffer;

    struct ctl_svc_node *csn = NULL;

    /* First set of request checks which are configuration based.
     */
    if (raft_server_client_recv_ignore_request(ri, rcm, from, &csn))
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "cannot verify client message");
        return;
    }

    struct raft_net_client_request_handle rncr;

    raft_server_net_client_request_init_client_rpc(ri, &rncr, rcm, from,
                                                   reply_buf,
                                                   RAFT_NET_MAX_RPC_SIZE);

    /* Second set of checks which determine if this server is capable of
     * handling the request at this time.
     */
    int rc = raft_server_may_accept_client_request(ri);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "cannot accept client message, rc=%d", rc);
        raft_server_udp_client_deny_request(ri, &rncr, csn, rc);
        goto out;
    }

    if (rcm->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_PING)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "ping reply");
        raft_server_reply_to_client(ri, &rncr, csn);
        goto out;
    }

    /* May used by state machine, note that
     * raft_net_sm_write_supplement_destroy() must be called before exiting
     * this function.
     */
    raft_net_sm_write_supplement_init(&rncr.rncr_sm_write_supp);

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

    DBG_RAFT_CLIENT_RPC(log_level, rcm,
                        "wr_op=%d write-2-raft=%s op_error=%s, cb_rc=%s",
                        write_op, rncr.rncr_write_raft_entry ? "yes" : "no",
                        strerror(-rncr.rncr_op_error), strerror(-cb_rc));

    /* Callback's with error are only logged.  There are no client replies
     * or raft operations which will occur.
     */
    if (cb_rc) // Other than logging this issue, nothing can be done here
        goto out1;

    /* cb's may run for a long time and the server may have been deposed
     * Xxx note that SM write requests left in this state may require
     *   cleanup.
     */
    rc = raft_server_may_accept_client_request(ri);
    if (rc)
    {
        raft_server_udp_client_deny_request(ri, &rncr, csn, rc);
        goto out1;
    }

    /* Store the request as an entry in the Raft log.  Do not reply to
     * the client until the write is committed and applied!
     */
    if (rncr.rncr_write_raft_entry)
        raft_server_leader_write_new_entry(ri, rcm->rcrm_data,
                                           rcm->rcrm_data_size,
                                           RAFT_WR_ENTRY_OPT_NONE,
                                           &rncr.rncr_sm_write_supp);

    /* Read operation or an already committed + applied write
     * operation.
     */
    else
        raft_server_reply_to_client(ri, &rncr, csn);

out1:
    raft_net_sm_write_supplement_destroy(&rncr.rncr_sm_write_supp);
out:
    if (csn)
        ctl_svc_node_put(csn);
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
    if (rc == -EALREADY)
        return false;
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

    const raft_entry_idx_t unsync_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC);

    // This is not a recency check and should be in a separate function Xxx
    if (rfi->rfi_next_idx > unsync_idx)
    {
        // May only be ahead by '1'
        NIOVA_ASSERT(rfi->rfi_next_idx == unsync_idx + 1);
        send_msg = false;
    }

    return send_msg;
}

static raft_server_epoll_remote_sender_t
raft_server_append_entry_sender(struct raft_instance *ri, bool heartbeat)
{
    NIOVA_ASSERT(ri);

    const int64_t my_raft_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC);

    if (!raft_instance_is_leader(ri) || my_raft_idx < 0)
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

        DBG_RAFT_INSTANCE_FATAL_IF((peer_next_raft_idx - 1 > my_raft_idx), ri,
                                   "follower's idx > leader's (%ld > %ld)",
                                   peer_next_raft_idx, my_raft_idx);

        if (!heartbeat && peer_next_raft_idx <= my_raft_idx)
        {
            struct raft_entry_header *reh =
                (struct raft_entry_header *)sink_buf;

            int rc = raft_server_entry_header_read_by_store(
                ri, reh, peer_next_raft_idx);

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

        int rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, rp, rrm);

        /* log errors, but raft will retry if needed */
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "raft_server_send_msg(): %d", rc);
    }
}

static raft_server_epoll_sm_apply_t
raft_server_sm_apply_opt(struct raft_instance *ri,
                         struct raft_net_client_request_handle *rncr)
{
    NIOVA_ASSERT(ri && rncr);

    if (ri->ri_backend->rib_sm_apply_opt)
        ri->ri_backend->rib_sm_apply_opt(ri, &rncr->rncr_sm_write_supp);
}

static raft_server_epoll_sm_apply_bool_t
raft_server_net_client_request_init_sm_apply(
    struct raft_instance *ri, struct raft_net_client_request_handle *rncr,
    char *commit_data, const size_t commit_data_size, char *reply_buf,
    const size_t reply_buf_size)
{
    NIOVA_ASSERT(ri && rncr && commit_data);

    raft_server_net_client_request_init(ri, rncr,
                                        RAFT_NET_CLIENT_REQ_TYPE_COMMIT,
                                        NULL, commit_data, commit_data_size,
                                        NULL, reply_buf, reply_buf_size);
}

/**
 * raft_server_backend_setup_last_applied - called in setup context to provide
 *    the last-applied info from a stateful backend, such as RocksDB.
 */
void
raft_server_backend_setup_last_applied(struct raft_instance *ri,
                                       raft_entry_idx_t last_applied_idx,
                                       crc32_t last_applied_cumulative_crc)
{
    // Assert some setup / bootup context items.
    NIOVA_ASSERT(ri && ri->ri_last_applied_idx == -1 &&
                 ri->ri_commit_idx == -1 &&
                 ri->ri_last_applied_cumulative_crc == 0 &&
                 ri->ri_state == RAFT_STATE_BOOTING);

    ri->ri_last_applied_idx = last_applied_idx;
    ri->ri_last_applied_cumulative_crc = last_applied_cumulative_crc;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "");
}

static raft_server_epoll_sm_apply_t
raft_server_last_applied_increment(struct raft_instance *ri,
                                   const struct raft_entry_header *reh)
{
    NIOVA_ASSERT(ri && reh &&
                 (reh->reh_index == (ri->ri_last_applied_idx + 1)));

    ri->ri_last_applied_idx++;
    ri->ri_last_applied_cumulative_crc ^= reh->reh_crc;

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "idx=%ld crc=%x",
                      ri->ri_last_applied_idx, reh->reh_crc);
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

    const raft_entry_idx_t apply_idx = ri->ri_last_applied_idx + 1;

    struct raft_entry_header reh;

    int rc = raft_server_entry_header_read_by_store(ri, &reh, apply_idx);
    DBG_RAFT_INSTANCE_FATAL_IF((rc), ri,
                               "raft_server_entry_header_read_by_store(): %s",
                               strerror(-rc));

    /* Signify that the entry will be applied.  Prepare the last-applied values
     * prior to entering raft_server_sm_apply_opt().
     */
    raft_server_last_applied_increment(ri, &reh);

    struct raft_net_client_request_handle rncr;
    raft_server_net_client_request_init_sm_apply(ri, &rncr, sink_buf,
                                                 reh.reh_data_size,
                                                 reply_buf,
                                                 RAFT_NET_MAX_RPC_SIZE);

    if (!reh.reh_leader_change_marker && reh.reh_data_size)
    {
        rc = raft_server_entry_read(ri, apply_idx, sink_buf, reh.reh_data_size,
                                    NULL);
        DBG_RAFT_INSTANCE_FATAL_IF((rc), ri, "raft_server_entry_read(): %s",
                                   strerror(-rc));

        // Initialize supplement handle for possible use by SM callback
        raft_net_sm_write_supplement_init(&rncr.rncr_sm_write_supp);

        int rc = ri->ri_server_sm_request_cb(&rncr);

        // Called regardless of ri_server_sm_request_cb() error
        raft_server_sm_apply_opt(ri, &rncr);

        if (!rc && raft_instance_is_leader(ri))
        {
            if (reh.reh_term == ri->ri_log_hdr.rlh_term)
            {
                struct timespec ts;
                niova_realtime_coarse_clock(&ts);

                timespecsub(&ts, &reh.reh_store_time, &ts);

                struct binary_hist *bh =
                    &ri->ri_rihs[RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC].rihs_bh;

                if (timespec_2_msec(&ts) > 0)
                    binary_hist_incorporate_val(bh, timespec_2_msec(&ts));
            }
            /* Perform basic initialization on the reply buffer if the SM has
             * provided the necessary info for completing the reply.  The SM
             * would have called
             * raft_net_client_request_handle_set_reply_info() if the necessary
             * info was provided.  Note that the SM may not check for leader
             * status, so the reply info may be provided even when this node
             * is a follower.  Therefore, udp init should be bypassed if this
             * node is not the leader.
             */
            if (raft_net_client_request_handle_has_reply_info(&rncr))
                raft_server_client_reply_init(
                    ri, &rncr, RAFT_CLIENT_RPC_MSG_TYPE_REPLY);
        }

        DBG_RAFT_ENTRY(LL_NOTIFY, &reh, "rc=%s", strerror(-rc));

        // The destructor may issue a callback into the SM.
        raft_net_sm_write_supplement_destroy(&rncr.rncr_sm_write_supp);
    }

    if (!reh.reh_leader_change_marker && !reh.reh_data_size)
        DBG_RAFT_ENTRY(LL_WARN, &reh, "application entry contains no data!");

    DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "ri_last_applied_idx was incremented");
    DBG_RAFT_ENTRY(LL_NOTIFY, &reh, "");

    if (ri->ri_last_applied_idx < ri->ri_commit_idx)
        ev_pipe_notify(&ri->ri_evps[RAFT_SERVER_EVP_SM_APPLY]);

    if (raft_instance_is_leader(ri) && // Only issue if we're the leader!
        raft_net_client_request_handle_has_reply_info(&rncr))
        raft_server_reply_to_client(ri, &rncr, NULL);
}

static raft_server_epoll_remote_sender_t
raft_server_follower_send_sync_idx(struct raft_instance *ri)
{
    if (!ri || !raft_instance_is_follower(ri) ||
        !raft_server_does_synchronous_writes(ri))
        return;

    // Extra check to ensure we don't send to ourselves
    struct ctl_svc_node *leader = ri->ri_csn_leader;
    if (leader == ri->ri_csn_this_peer)
        return;

    struct raft_entry_header reh = {0};
    raft_instance_get_newest_header(ri, &reh, RI_NEHDR_SYNC);

    struct raft_rpc_msg rrm = {
        .rrm_type = RAFT_RPC_MSG_TYPE_SYNC_IDX_UPDATE,
        .rrm_version = 0,
        .rrm_sync_index_update.rsium_synced_log_index = reh.reh_index,
        .rrm_sync_index_update.rsium_term = reh.reh_term,
    };

    raft_server_set_uuids_in_rpc_msg(ri, &rrm);

    int rc = raft_server_send_msg(ri, RAFT_UDP_LISTEN_SERVER, leader, &rrm);
    if (rc)
        DBG_RAFT_INSTANCE(LL_NOTIFY, ri, "raft_server_send_msg(): %s",
                          strerror(-rc));
}

static raft_server_epoll_remote_sender_t
raft_server_remote_send_evp_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph);

    FUNC_ENTRY(LL_DEBUG);

    struct raft_instance *ri = eph->eph_arg;
    struct ev_pipe *evp = &ri->ri_evps[RAFT_SERVER_EVP_REMOTE_SEND];

    NIOVA_ASSERT(eph->eph_fd == evp_read_fd_get(evp));

    EV_PIPE_RESET(evp); // reset prior to dequeuing work

    raft_instance_is_leader(ri) ?
        raft_server_append_entry_sender(ri, false) :
        raft_server_follower_send_sync_idx(ri);
}

static raft_server_epoll_sm_apply_t
raft_server_sm_apply_evp_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph);

    FUNC_ENTRY(LL_DEBUG);

    struct raft_instance *ri = eph->eph_arg;

    struct ev_pipe *evp = &ri->ri_evps[RAFT_SERVER_EVP_SM_APPLY];
    NIOVA_ASSERT(eph->eph_fd == evp_read_fd_get(evp));

    EV_PIPE_RESET(evp);

    raft_server_state_machine_apply(ri);
}

static raft_server_epoll_t
raft_server_commit_idx_adv_evp_cb(const struct epoll_handle *eph,
                                  uint32_t events)
{
    NIOVA_ASSERT(eph);
    FUNC_ENTRY(LL_DEBUG);

    struct raft_instance *ri = eph->eph_arg;

    struct ev_pipe *evp = &ri->ri_evps[RAFT_SERVER_EVP_ASYNC_COMMIT_IDX_ADV];
    NIOVA_ASSERT(eph->eph_fd == evp_read_fd_get(evp));

    EV_PIPE_RESET(evp);

    if (raft_instance_is_leader(ri))
        raft_server_leader_try_advance_commit_idx(ri);
}

static epoll_mgr_cb_t
raft_server_evp_2_cb_fn(enum raft_server_event_pipes evps)
{
    switch (evps)
    {
    case RAFT_SERVER_EVP_REMOTE_SEND:
        return raft_server_remote_send_evp_cb;
    case RAFT_SERVER_EVP_SM_APPLY:
        return raft_server_sm_apply_evp_cb;
    case RAFT_SERVER_EVP_ASYNC_COMMIT_IDX_ADV:
        return raft_server_commit_idx_adv_evp_cb;
    default:
        break;
    }
    return NULL;
}

//xxx port to raft_net_evp_add()
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

static int
raft_server_instance_startup(struct raft_instance *ri);

static int
raft_server_instance_shutdown(struct raft_instance *ri);

static void
raft_server_instance_init(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_booting(ri));

    if (!ri->ri_election_timeout_max_ms)
        ri->ri_election_timeout_max_ms = RAFT_ELECTION_UPPER_TIME_MS;

    if (!ri->ri_heartbeat_freq_per_election_min)
        ri->ri_heartbeat_freq_per_election_min =
            RAFT_HEARTBEAT_FREQ_PER_ELECTION;

    ri->ri_commit_idx = -1;
    ri->ri_last_applied_idx = -1;
    ri->ri_lowest_idx = -1;
    ri->ri_checkpoint_last_idx = -1;

    ri->ri_startup_pre_net_bind_cb = raft_server_instance_startup;
    ri->ri_shutdown_cb = raft_server_instance_shutdown;

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

static raft_server_sync_thread_t
raft_server_sync_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct raft_instance *ri = (struct raft_instance *)thread_ctl_get_arg(tc);

    if (!ri->ri_sync_freq_us)
        ri->ri_sync_freq_us = RAFT_SERVER_SYNC_FREQ_US;

//    thread_ctl_set_user_pause_usec(tc, ri->ri_sync_freq_us);

    NIOVA_ASSERT(ri);

    THREAD_LOOP_WITH_CTL(tc)
    {
        usleep(ri->ri_sync_freq_us);
        DBG_THREAD_CTL(LL_TRACE, tc, "here");
        const bool has_unsynced_entries =
            raft_server_has_unsynced_entries(ri);

        DBG_RAFT_INSTANCE((has_unsynced_entries ? LL_DEBUG : LL_TRACE), ri,
                          "raft_server_has_unsynced_entries(): %d",
                          has_unsynced_entries);

        if (has_unsynced_entries)
        {
            raft_server_backend_sync_pending(ri, __func__);
            ri->ri_sync_cnt++;

            raft_server_leader_try_advance_commit_idx_from_sync_thread(ri);
        }
    }

    return (void *)0;
}

static int
raft_server_sync_thread_start(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_booting(ri));
    NIOVA_ASSERT(!raft_server_does_synchronous_writes(ri));

    int rc = thread_create_watched(raft_server_sync_thread,
                                   &ri->ri_sync_thread_ctl,
                                   "sync_thread", (void *)ri, NULL);
     if (rc)
	return rc;

    thread_ctl_run(&ri->ri_sync_thread_ctl);

    return 0;
}

static void
raft_server_take_chkpt(struct raft_instance *ri)
{
    raft_entry_idx_t sync_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC);

    if (sync_idx < 0)
        return;

    struct timespec io_op[2];
    niova_unstable_clock(&io_op[0]);

    /* rib_backend_checkpoint() returns the idx used for the checkpoint which
     * must be >= to the one read here.
     */
    int64_t rc = ri->ri_backend->rib_backend_checkpoint(ri);

    niova_unstable_clock(&io_op[1]);

    raft_server_incorporate_latency_measurement(
        ri, io_op[0], io_op[1], RAFT_INSTANCE_HIST_CHKPT_LAT_USEC);

    if (rc >= 0)
    {
        FATAL_IF(
            (rc < sync_idx ||
             rc < niova_atomic_read(&ri->ri_checkpoint_last_idx)),
            "invalid checkpoint-idx=%ld (sync-idx=%ld, chkpt_last_idx=%lld)",
            rc, sync_idx, ri->ri_checkpoint_last_idx);

        // Atomic here since this runs in a separate thread context.
        niova_atomic_init(&ri->ri_checkpoint_last_idx, rc);
    }

    DBG_RAFT_INSTANCE((rc < 0 ? LL_ERROR : /*LL_INFO*/ LL_WARN), ri,
                      "rib_backend_checkpoint(%zd): %s",
                      rc, rc < 0 ? strerror(-rc) : "Success");
}


// XXX change my name to "reap" or "cull" log
static void
raft_server_reap_log(struct raft_instance *ri)
{
    if (!ri)
        return;

    const raft_entry_idx_t sync_idx =
        raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC);

    if (sync_idx < 0)
        return;

    const raft_entry_idx_t num_entries =
        sync_idx - MAX(0, niova_atomic_read(&ri->ri_lowest_idx));

    raft_entry_idx_t reap_idx = -1ULL;

    int rc = 0;
    if (num_entries > RAFT_INSTANCE_PERSISTENT_APP_MAX_SCAN_ENTRIES)
    {
        reap_idx = sync_idx - RAFT_INSTANCE_PERSISTENT_APP_MAX_SCAN_ENTRIES;
        NIOVA_ASSERT(reap_idx >
                     RAFT_INSTANCE_PERSISTENT_APP_MAX_SCAN_ENTRIES);

        ri->ri_backend->rib_log_reap(ri, reap_idx);

        niova_atomic_init(&ri->ri_lowest_idx, reap_idx);
    }

    DBG_RAFT_INSTANCE((rc ? LL_ERROR : LL_WARN), ri,
                      "num-entries=%ld, reap-idx=%ld",
                      num_entries, reap_idx);
}

static raft_server_chkpt_thread_t
raft_server_chkpt_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct raft_instance *ri = (struct raft_instance *)thread_ctl_get_arg(tc);

//    thread_ctl_set_user_pause_usec(tc, ri->ri_sync_freq_us);

    NIOVA_ASSERT(ri);

    THREAD_LOOP_WITH_CTL(tc)
    {
        sleep(1);
        DBG_THREAD_CTL(LL_TRACE, tc, "here");

        const bool user_requested_chkpt = ri->ri_user_requested_checkpoint;
        if (user_requested_chkpt)
            ri->ri_user_requested_checkpoint = false;

        const bool user_requested_reap = ri->ri_user_requested_reap;
        if (user_requested_reap)
            ri->ri_user_requested_reap = false;

        const raft_entry_idx_t sync_idx =
            raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC);

        if (sync_idx < 0)
            continue;

        const raft_entry_idx_t num_entries_since_last_chkpt =
            sync_idx - ri->ri_checkpoint_last_idx;

        NIOVA_ASSERT(num_entries_since_last_chkpt >= 0);

        DBG_RAFT_INSTANCE((num_entries_since_last_chkpt ? LL_DEBUG : LL_TRACE),
                          ri, "num_entries_since_last_chkpt=%zd user-req=%s",
                          num_entries_since_last_chkpt,
                          user_requested_chkpt ? "yes" : "no");

        if (user_requested_chkpt ||
            (num_entries_since_last_chkpt >=
             RAFT_INSTANCE_PERSISTENT_APP_MAX_SCAN_ENTRIES))
            raft_server_take_chkpt(ri);

        // Test for pruning
        if (user_requested_reap ||
            ((sync_idx - MAX(niova_atomic_read(&ri->ri_lowest_idx), 0)) >
             RAFT_INSTANCE_PERSISTENT_APP_MAX_SCAN_ENTRIES *
             RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR))
            raft_server_reap_log(ri);
    }

    return (void *)0;
}

static int
raft_server_chkpt_thread_start(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_booting(ri));
    NIOVA_ASSERT(ri->ri_backend->rib_backend_checkpoint);

    int rc = thread_create_watched(raft_server_chkpt_thread,
                                   &ri->ri_chkpt_thread_ctl,
                                   "chkpt_thread", (void *)ri, NULL);
     if (rc)
	return rc;

    thread_ctl_run(&ri->ri_chkpt_thread_ctl);

    return 0;
}

static int
raft_server_instance_startup(struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && raft_instance_is_booting(ri));

    FATAL_IF((pthread_mutex_init(&ri->ri_newest_entry_mutex, NULL)),
             "pthread_mutex_init(): %s", strerror(errno));

    // raft_server_instance_init() should have been run
    if (!ri->ri_timer_fd_cb)
        return -EINVAL;

    // Init the in-memory sync/unsync entry headers
    raft_instance_initialize_newest_entry_hdr(ri);

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
        goto out;
    }

    rc = raft_server_log_load(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "raft_server_log_load(): %s",
                          strerror(-rc));
        goto out;
    }

    rc = raft_server_evp_setup(ri);
    if (rc)
    {
        DBG_RAFT_INSTANCE(LL_ERROR, ri, "ev_pipe_setup(): %s",
                          strerror(-rc));
        goto out;
    }

    if (!raft_server_does_synchronous_writes(ri))
    {
        rc = raft_server_sync_thread_start(ri);
        if (rc)
            goto out;
    }

    if (ri->ri_backend->rib_backend_checkpoint)
    {
        rc = raft_server_chkpt_thread_start(ri);
        if (rc)
            goto out;
    }

out:
    if (rc)
        raft_net_instance_shutdown(ri);

    return rc;
}

static int
raft_server_backend_close(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    return ri->ri_backend->rib_backend_shutdown(ri);
}

static int
raft_server_instance_shutdown(struct raft_instance *ri)
{
    raft_server_backend_close(ri);

    raft_server_evp_cleanup(ri);

    for (int i = 0; i < RAFT_SERVER_EVP_ANY; i++)
        ev_pipe_cleanup(&ri->ri_evps[i]);

    pthread_mutex_destroy(&ri->ri_newest_entry_mutex);

    return 0;
}

static int
raft_server_main_loop(struct raft_instance *ri)
{
    NIOVA_ASSERT(raft_instance_is_booting(ri));
    ri->ri_state = RAFT_STATE_FOLLOWER;
    ri->ri_follower_reason = RAFT_BFRSN_LEADER_ALREADY_PRESENT;

    raft_server_timerfd_settime(ri);

    int rc = 0;

    do
    {
        rc = epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);
        if (rc == -EINTR)
            rc = 0;
    } while (rc >= 0);

    SIMPLE_LOG_MSG(LL_WARN, "epoll_mgr_wait_and_process_events(): %s",
                   strerror(-rc));

    return rc;
}



int
raft_server_instance_run(const char *raft_uuid_str,
                         const char *this_peer_uuid_str,
                         raft_sm_request_handler_t sm_request_handler,
                         enum raft_instance_store_type type,
                         bool sync_writes, void *arg)
{
    if (!raft_uuid_str || !this_peer_uuid_str || !sm_request_handler)
        return -EINVAL;

    struct raft_instance *ri = raft_net_get_instance();

    raft_server_instance_init(ri);

    ri->ri_raft_uuid_str = raft_uuid_str;
    ri->ri_this_peer_uuid_str = this_peer_uuid_str;
    ri->ri_server_sm_request_cb = sm_request_handler;
    ri->ri_backend_init_arg = arg;
    ri->ri_synchronous_writes = sync_writes;

    raft_instance_backend_type_specify(ri, type);

    int rc = raft_net_instance_startup(ri, false);
    if (rc)
        return rc;

    rc = raft_server_main_loop(ri);

    raft_net_instance_shutdown(ri);

    return rc;
}

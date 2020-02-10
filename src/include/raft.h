/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_H_
#define __NIOVA_RAFT_H_ 1

#include "common.h"
#include "util.h"

#define NUM_RAFT_LOG_HEADERS 2
#define RAFT_ENTRY_PAD_SIZE 63
#define RAFT_ENTRY_MAGIC  0x1a2b3c4dd4c3b2a1
#define RAFT_HEADER_MAGIC 0xafaeadacabaaa9a8

#define RAFT_ENTRY_HEADER_RESERVE 128

#define RAFT_ENTRY_SIZE           65536
#define RAFT_ENTRY_MAX_DATA_SIZE  (RAFT_ENTRY_SIZE - RAFT_ENTRY_HEADER_RESERVE)

#define RAFT_ELECTION_MAX_TIME_MS   3000 // XXX increased by 10x for now
#define RAFT_ELECTION_MIN_TIME_MS   1500
#define RAFT_ELECTION_RANGE_MS                                  \
    (RAFT_ELECTION_MAX_TIME_MS - RAFT_ELECTION_MIN_TIME_MS)

#define RAFT_HEARTBEAT_TIME_MS      50

#define RAFT_INSTANCE_2_SELF_UUID(ri)          \
    (ri)->ri_csn_this_peer->csn_uuid

#define RAFT_INSTANCE_2_RAFT_UUID(ri)           \
    (ri)->ri_csn_raft->csn_uuid

typedef void raft_server_udp_cb_ctx_t;
typedef void raft_net_udp_cb_ctx_t;
typedef void raft_server_timerfd_cb_ctx_t;
typedef int  raft_server_timerfd_cb_ctx_int_t;
typedef void raft_server_leader_mode_t;
typedef int  raft_server_leader_mode_int_t;

enum raft_rpc_msg_type
{
    RAFT_RPC_MSG_TYPE_INVALID                = 0,
    RAFT_RPC_MSG_TYPE_VOTE_REQUEST           = 1,
    RAFT_RPC_MSG_TYPE_VOTE_REPLY             = 2,
    RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST = 3,
    RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY   = 4,
    RAFT_RPC_MSG_TYPE_ANY                    = 5,
};

struct raft_vote_request_msg
{
    int64_t rvrqm_proposed_term;
    int64_t rvrqm_last_log_term;
    int64_t rvrqm_last_log_index;
};

struct raft_vote_reply_msg
{
    uint8_t rvrpm_voted_granted;
    uint8_t rvrpm__pad[7];
    int64_t rvrpm_term;
};

struct raft_append_entries_request_msg
{
    int64_t  raerqm_term;
    uint64_t raerqm_commit_index;
    int64_t  raerqm_prev_log_term;
    int64_t  raerqm_prev_log_index;
    uint16_t raerqm_entries_sz; // if '0' then "heartbeat" msg
    uint16_t raerqm__pad[3];
    char     raerqm_entries[]; // Must be last
};

struct raft_append_entries_reply_msg
{
    int64_t raerpm_term;
    uint8_t raerpm_err_stale_term;
    uint8_t raerpm_err_non_matching_prev_term;
    uint8_t raerpm__pad[6];
};

//#define RAFT_RPC_MSG_TYPE_Version0_SIZE 120

struct raft_rpc_msg
{
    uint32_t rrm_type;
    uint16_t rrm_version;
    uint16_t rrm__pad;
    uuid_t   rrm_sender_id;
    uuid_t   rrm_raft_id;
    union
    {   // This union must be at the end of the structure
        struct raft_vote_request_msg           rrm_vote_request;
        struct raft_vote_reply_msg             rrm_vote_reply;
        struct raft_append_entries_request_msg rrm_append_entries_request;
        struct raft_append_entries_reply_msg   rrm_append_entries_reply;
    };
};

struct raft_entry_header
{
    uint64_t reh_magic;     // Magic is not included in the crc
    crc32_t  reh_crc;       // Crc is after the magic
    uint32_t reh_data_size; // The size of the log entry data
    int64_t  reh_index;    // Must match physical offset + NUM_RAFT_LOG_HEADERS
#if 0 //XXx
    int64_t  reh_phys_index; // physical offset
    int64_t  reh_raft_index; // the raft index number
#endif
    int64_t  reh_term;
    uint32_t reh_log_hdr_blk:1;
    uuid_t   reh_self_uuid; // UUID of this peer
    uuid_t   reh_raft_uuid; // UUID of raft instance
    char     reh_pad[RAFT_ENTRY_PAD_SIZE];
};

struct raft_entry
{
    struct raft_entry_header re_header;
    char                     re_data[];
};

struct raft_log_header
{
    uint64_t rlh_magic;
    int64_t  rlh_term;
    uint64_t rlh_seqno;
    uuid_t   rlh_voted_for;
};

#define RAFT_LOG_HEADER_DATA_SIZE sizeof(struct raft_log_header)

enum raft_state
{
    RAFT_STATE_LEADER,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_CLIENT,
};

#define RAFT_LOG_SUFFIX_MAX_LEN 8

enum raft_epoll_handles
{
    RAFT_EPOLL_HANDLE_PEER_UDP,
    RAFT_EPOLL_HANDLE_CLIENT_UDP,
    RAFT_EPOLL_HANDLE_TIMERFD,
    RAFT_EPOLL_NUM_HANDLES,
};

enum raft_udp_listen_sockets
{
    RAFT_UDP_LISTEN_MIN    = 0,
    RAFT_UDP_LISTEN_SERVER = RAFT_UDP_LISTEN_MIN,
    RAFT_UDP_LISTEN_CLIENT = 1,
    RAFT_UDP_LISTEN_MAX    = 2,
    RAFT_UDP_LISTEN_ANY    = RAFT_UDP_LISTEN_MAX,
};

enum raft_vote_result
{
    RATE_VOTE_RESULT_UNKNOWN,
    RATE_VOTE_RESULT_YES,
    RATE_VOTE_RESULT_NO,
};

struct raft_candidate_state
{
    int64_t               rcs_term;
    enum raft_vote_result rcs_results[CTL_SVC_MAX_RAFT_PEERS];
};

struct raft_leader_state
{
    uint64_t rls_commit_idx;
    int64_t  rls_leader_term;
//    uint64_t rls_match_idx[CTL_SVC_MAX_RAFT_PEERS];
    uint64_t rls_next_idx[CTL_SVC_MAX_RAFT_PEERS];
    int64_t  rls_prev_idx_term[CTL_SVC_MAX_RAFT_PEERS];
};

struct raft_instance
{
    struct udp_socket_handle    ri_ush[RAFT_UDP_LISTEN_MAX];
    struct ctl_svc_node        *ri_csn_raft;
    struct ctl_svc_node        *ri_csn_raft_peers[CTL_SVC_MAX_RAFT_PEERS];
    struct ctl_svc_node        *ri_csn_this_peer;
    struct ctl_svc_node        *ri_csn_leader;
    const char                 *ri_raft_uuid_str;
    const char                 *ri_this_peer_uuid_str;
    struct raft_candidate_state ri_candidate;
    struct raft_leader_state    ri_leader;
    enum raft_state             ri_state;
    int                         ri_timer_fd;
    int                         ri_log_fd;
    char                        ri_log[PATH_MAX + 1];
    struct stat                 ri_log_stb;
    struct raft_log_header      ri_log_hdr;
    uint64_t                    ri_commit_idx;
    uint64_t                    ri_last_applied_idx;
    struct raft_entry_header    ri_newest_entry_hdr;
    struct epoll_mgr            ri_epoll_mgr;
    struct epoll_handle         ri_epoll_handles[RAFT_EPOLL_NUM_HANDLES];
};

static inline void
raft_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(RAFT_ELECTION_RANGE_MS > 0);
    COMPILE_TIME_ASSERT(sizeof(struct raft_entry_header) ==
                        RAFT_ENTRY_HEADER_RESERVE);
}

#define DBG_RAFT_MSG(log_level, rm, fmt, ...)                           \
{                                                                       \
    char __uuid_str[UUID_STR_LEN];                                      \
    uuid_unparse((rm)->rrm_sender_id, __uuid_str);                      \
    switch ((rm)->rrm_type)                                             \
    {                                                                   \
    case RAFT_RPC_MSG_TYPE_VOTE_REQUEST:                                \
        SIMPLE_LOG_MSG(log_level,                                       \
                       "VREQ nterm=%lx last=%lx:%lx %s "fmt,            \
                       (rm)->rrm_vote_request.rvrqm_proposed_term,      \
                       (rm)->rrm_vote_request.rvrqm_last_log_term,      \
                       (rm)->rrm_vote_request.rvrqm_last_log_index,     \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
    case RAFT_RPC_MSG_TYPE_VOTE_REPLY:                                  \
        SIMPLE_LOG_MSG(log_level,                                       \
                       "VREPLY term=%lx granted=%s %s "fmt,             \
                       (rm)->rrm_vote_reply.rvrpm_term,                 \
                       ((rm)->rrm_vote_reply.rvrpm_voted_granted ?      \
                        "yes" : "no"),                                  \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST:                      \
        SIMPLE_LOG_MSG(log_level,                                       \
                       "APPREQ t=%lx ci=%lx pl=%lx:%lx sz=%hx %s "fmt,  \
                       (rm)->rrm_append_entries_request.raerqm_term,    \
                       (rm)->rrm_append_entries_request.raerqm_commit_index, \
                       (rm)->rrm_append_entries_request.raerqm_prev_log_term, \
                       (rm)->rrm_append_entries_request.raerqm_prev_log_index, \
                       (rm)->rrm_append_entries_request.raerqm_entries_sz, \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
    default:                                                            \
        break;                                                          \
    }                                                                   \
}

#define DBG_RAFT_ENTRY(log_level, re, fmt, ...)                         \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "re@%p crc=%x size=%u idx=%ld term=%ld lb=%x "fmt,   \
                   (re), (re)->reh_crc, (re)->reh_data_size, (re)->reh_index, \
                   (re)->reh_term, (re)->reh_log_hdr_blk, ##__VA_ARGS__)

#define DBG_RAFT_INSTANCE(log_level, ri, fmt, ...)                      \
{                                                                       \
    char __uuid_str[UUID_STR_LEN];                                      \
    uuid_unparse((ri)->ri_log_hdr.rlh_voted_for, __uuid_str);           \
                                                                        \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "%c et=%lx ei=%lx ht=%lx hs=%lx v=%s "fmt,           \
                   raft_server_state_to_char((ri)->ri_state),           \
                   raft_server_get_current_raft_entry_term((ri)),       \
                   raft_server_get_current_raft_entry_index((ri)),      \
                   (ri)->ri_log_hdr.rlh_term,                           \
                   (ri)->ri_log_hdr.rlh_seqno,                          \
                   __uuid_str, ##__VA_ARGS__);                          \
}

#define DBG_RAFT_INSTANCE_FATAL_IF(cond, ri, message, ...)              \
{                                                                       \
    if ((cond))                                                         \
    {                                                                   \
        DBG_RAFT_INSTANCE(LL_FATAL, ri, message, ##__VA_ARGS__);        \
    }                                                                   \
}

static inline char
raft_server_state_to_char(enum raft_state state)
{
    switch (state)
    {
    case RAFT_STATE_LEADER:
        return 'L';
    case RAFT_STATE_FOLLOWER:
        return 'F';
    case RAFT_STATE_CANDIDATE:
        return 'C';
    case RAFT_STATE_CLIENT:
        return 'c';
    default:
        break;
    }

    return '?';
}

/**
 * raft_server_entry_header_is_null - strict check which asserts that if the
 *   magic value in the header is not equal to RAFT_HEADER_MAGIC that the
 *   entry header is completely null.
 */
static inline bool
raft_server_entry_header_is_null(const struct raft_entry_header *reh)
{
    if (reh->reh_magic == RAFT_HEADER_MAGIC)
        return false;

    const struct raft_entry_header null_reh = {0};
    NIOVA_ASSERT(!memcmp(reh, &null_reh, sizeof(struct raft_entry_header)));

    return true;
}

/**
 * raft_server_get_current_raft_entry_term - returns the term value from the
 *    most recent raft entry to have been written to the log.  Note that
 *    ri_newest_entry_hdr never refers to a header block.
 */
static inline int64_t
raft_server_get_current_raft_entry_term(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    return ri->ri_newest_entry_hdr.reh_term;
}

/**
 * raft_server_get_current_raft_entry_index - given a raft instance, returns
 *    the position of the last written application log entry.  The results
 *    are based on the ri_newest_entry_hdr data, which stores a copy of the
 *    most recently written log entry.  If ri_newest_entry_hdr is null, this
 *    means the log has no entries.  Otherwise, a non-negative value is
 *    adjusted to subtract NUM_RAFT_LOG_HEADERS and returned.
 */
static inline int64_t
raft_server_get_current_raft_entry_index(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    int64_t current_reh_index = -1;

    if (!raft_server_entry_header_is_null(&ri->ri_newest_entry_hdr))
    {
        current_reh_index =
            ri->ri_newest_entry_hdr.reh_index - NUM_RAFT_LOG_HEADERS;

        NIOVA_ASSERT(current_reh_index >= 0);
    }

    return current_reh_index;
}


raft_net_udp_cb_ctx_t
raft_server_process_received_server_msg(struct raft_instance *ri,
	                                const struct raft_rpc_msg *rrm);

int
raft_server_epoll_setup_timerfd(struct raft_instance *ri);

int
raft_server_instance_startup(struct raft_instance *ri);

int
raft_server_instance_shutdown(struct raft_instance *ri);

int
raft_server_main_loop(struct raft_instance *ri);

#endif

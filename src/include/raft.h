/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_H_
#define __NIOVA_RAFT_H_ 1

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common.h"
#include "util.h"
#include "ctl_svc.h"
#include "raft_net.h"
#include "ev_pipe.h"

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

#define RAFT_MIN_APPEND_ENTRY_IDX -1

#define RAFT_INSTANCE_2_SELF_UUID(ri)          \
    (ri)->ri_csn_this_peer->csn_uuid

#define RAFT_INSTANCE_2_RAFT_UUID(ri)           \
    (ri)->ri_csn_raft->csn_uuid

typedef void    raft_server_udp_cb_ctx_t;
typedef int     raft_server_udp_cb_ctx_int_t;
typedef bool    raft_server_udp_cb_ctx_bool_t;
typedef bool    raft_server_udp_cb_follower_ctx_bool_t;
typedef int     raft_server_udp_cb_follower_ctx_int_t;
typedef void    raft_server_udp_cb_follower_ctx_t;
typedef void    raft_server_udp_cb_leader_t;
typedef void    raft_server_udp_cb_leader_ctx_t;
typedef int64_t raft_server_udp_cb_leader_ctx_int64_t;
typedef void    raft_server_timerfd_cb_ctx_t;
typedef int     raft_server_timerfd_cb_ctx_int_t;
typedef void    raft_server_leader_mode_t;
typedef int     raft_server_leader_mode_int_t;
typedef int64_t raft_server_leader_mode_int64_t;
typedef void    raft_server_epoll_ae_sender_t;
typedef void    raft_server_epoll_sm_apply_t;
typedef void    raft_server_epoll_sm_apply_bool_t;

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
    int64_t  raerqm_leader_term; // current term of the leader
    int64_t  raerqm_log_term; // term of the log entry (prev_log_index + 1)
                              // .. used for replays of old msgs ?? XXX needed?
    uint64_t raerqm_term_seqno; // used for read-window'ing
    int64_t  raerqm_commit_index;
    int64_t  raerqm_prev_log_term;
    int64_t  raerqm_prev_log_index;
    uint32_t raerqm_prev_idx_crc;
    uint32_t raerqm_this_idx_crc;
    uint16_t raerqm_entries_sz;
    uint8_t  raerqm_heartbeat_msg;
    uint8_t  raerqm_leader_change_marker;
    uint8_t  raerqm__pad[4];
    char     raerqm_entries[]; // Must be last
};

struct raft_append_entries_reply_msg
{
    int64_t  raerpm_leader_term;
    int64_t  raerpm_prev_log_index;
    uint64_t raerpm_term_seqno; // used for read-window'ing
    uint8_t  raerpm_heartbeat_msg;
    uint8_t  raerpm_err_stale_term;
    uint8_t  raerpm_err_non_matching_prev_term;
    uint8_t  raerpm__pad[5];
};

//#define RAFT_RPC_MSG_TYPE_Version0_SIZE 120

struct raft_rpc_msg
{
    uint32_t rrm_type;
    uint16_t rrm_version;
    uint16_t rrm__pad;
//    uuid_t   rrm_dest_id; // XXX should match the recv'r
    uuid_t   rrm_sender_id; // should match the sender
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
    uuid_t   reh_self_uuid; // UUID of this peer
    crc32_t  reh_crc;       // Crc is after the magic and uuid's (all are constant).
    uint32_t reh_data_size; // The size of the log entry data - must be below reh_crc!
                            //    see raft_server_entry_calc_crc() before changing.
    int64_t  reh_index;     // Add NUM_RAFT_LOG_HEADERS to get phys offset
    int64_t  reh_term;      // Term in which entry was originally written
    uuid_t   reh_raft_uuid; // UUID of raft instance
    uint8_t  reh_leader_change_marker; // noop
    uint8_t  reh_pad[RAFT_ENTRY_PAD_SIZE];
};

static inline bool
raft_entry_is_header_block(const struct raft_entry_header *reh)
{
    NIOVA_ASSERT(reh);

    return reh->reh_index < 0 ? true : false;
}

struct raft_entry
{
    struct raft_entry_header re_header;
    char                     re_data[];
};

struct raft_log_header
{
    uint64_t rlh_version;
    uint64_t rlh_magic;
    int64_t  rlh_term;
    uint64_t rlh_seqno;
    uuid_t   rlh_voted_for;
};

#define RAFT_LOG_HEADER_DATA_SIZE sizeof(struct raft_log_header)

enum raft_state
{
    RAFT_STATE_BOOTING,
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
    RAFT_EPOLL_HANDLE_EVP_AE_SEND,
    RAFT_EPOLL_HANDLE_EVP_SM_APPLY,
    RAFT_EPOLL_NUM_HANDLES,
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

struct raft_follower_info
{
    int64_t            rfi_next_idx;
    int64_t            rfi_current_idx_term;
    int64_t            rfi_current_idx_crc;
    int64_t            rfi_prev_idx_term;
    int64_t            rfi_prev_idx_crc;
    struct timespec    rfi_last_ack;
    unsigned long long rfi_ae_sends_wait_until;
};

struct raft_leader_state
{
    int64_t                   rls_initial_term_idx; // idx @start of ldr's term
    int64_t                   rls_leader_term;
    struct raft_follower_info rls_rfi[CTL_SVC_MAX_RAFT_PEERS];
};

struct epoll_handle;
struct raft_instance;

enum raft_server_event_pipes
{
    RAFT_SERVER_EVP_AE_SEND  = 0,
    RAFT_SERVER_EVP_SM_APPLY = 1,
    RAFT_SERVER_EVP_ANY      = 2,
};

enum raft_follower_reasons
{
    RAFT_BFRSN_NONE,
    RAFT_BFRSN_VOTED_FOR_PEER,
    RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE,
    RAFT_BFRSN_STALE_TERM_WHILE_LEADER,
    RAFT_BFRSN_LEADER_ALREADY_PRESENT,
};

struct raft_instance
{
    struct udp_socket_handle    ri_ush[RAFT_UDP_LISTEN_MAX];
    struct ctl_svc_node        *ri_csn_raft;
    struct ctl_svc_node        *ri_csn_raft_peers[CTL_SVC_MAX_RAFT_PEERS];
    struct ctl_svc_node        *ri_csn_this_peer;
    struct ctl_svc_node        *ri_csn_leader;
    struct timespec             ri_last_send[CTL_SVC_MAX_RAFT_PEERS];
    struct timespec             ri_last_recv[CTL_SVC_MAX_RAFT_PEERS];
    const char                 *ri_raft_uuid_str;
    const char                 *ri_this_peer_uuid_str;
    struct raft_candidate_state ri_candidate;
    struct raft_leader_state    ri_leader;
    enum raft_state             ri_state;
    enum raft_follower_reasons  ri_follower_reason;
    int                         ri_timer_fd;
    int                         ri_log_fd;
    char                        ri_log[PATH_MAX + 1];
    struct stat                 ri_log_stb;
    struct raft_log_header      ri_log_hdr;
    int64_t                     ri_commit_idx;
    int64_t                     ri_last_applied_idx;
    crc32_t                     ri_last_applied_cumulative_crc;
    struct raft_entry_header    ri_newest_entry_hdr;
    struct epoll_mgr            ri_epoll_mgr;
    struct epoll_handle         ri_epoll_handles[RAFT_EPOLL_NUM_HANDLES];
    raft_net_timer_cb_t         ri_timer_fd_cb;
    raft_net_udp_cb_t           ri_udp_client_recv_cb;
    raft_net_udp_cb_t           ri_udp_server_recv_cb;
    raft_sm_request_handler_t   ri_server_sm_request_cb;
    raft_sm_commit_handler_t    ri_server_sm_commit_cb;
    struct ev_pipe              ri_evps[RAFT_SERVER_EVP_ANY];
    struct lreg_node            ri_lreg;
    struct lreg_node            ri_lreg_peer_stats[CTL_SVC_MAX_RAFT_PEERS];
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
                       "VREQ nterm=%ld last=%ld:%ld %s "fmt,            \
                       (rm)->rrm_vote_request.rvrqm_proposed_term,      \
                       (rm)->rrm_vote_request.rvrqm_last_log_term,      \
                       (rm)->rrm_vote_request.rvrqm_last_log_index,     \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
    case RAFT_RPC_MSG_TYPE_VOTE_REPLY:                                  \
        SIMPLE_LOG_MSG(log_level,                                       \
                       "VREPLY term=%ld granted=%s %s "fmt,             \
                       (rm)->rrm_vote_reply.rvrpm_term,                 \
                       ((rm)->rrm_vote_reply.rvrpm_voted_granted ?      \
                        "yes" : "no"),                                  \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST:                      \
        SIMPLE_LOG_MSG(log_level,                                       \
                       "AE_REQ t=%ld lt=%ld ci=%ld pl=%ld:%ld sz=%hx hb=%hhx lcm=%hhx %s "fmt, \
                       (rm)->rrm_append_entries_request.raerqm_leader_term, \
                       (rm)->rrm_append_entries_request.raerqm_log_term, \
                       (rm)->rrm_append_entries_request.raerqm_commit_index, \
                       (rm)->rrm_append_entries_request.raerqm_prev_log_term, \
                       (rm)->rrm_append_entries_request.raerqm_prev_log_index, \
                       (rm)->rrm_append_entries_request.raerqm_entries_sz, \
                       (rm)->rrm_append_entries_request.raerqm_heartbeat_msg, \
                       (rm)->rrm_append_entries_request.raerqm_leader_change_marker, \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
    case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY:                      \
        SIMPLE_LOG_MSG(log_level,                                       \
                       "AE_REPLY t=%ld pli=%ld hb=%hhx err=%hhx:%hhx %s "fmt, \
                       (rm)->rrm_append_entries_reply.raerpm_leader_term, \
                       (rm)->rrm_append_entries_reply.raerpm_prev_log_index, \
                       (rm)->rrm_append_entries_reply.raerpm_heartbeat_msg, \
                       (rm)->rrm_append_entries_reply.raerpm_err_stale_term, \
                       (rm)->rrm_append_entries_reply.raerpm_err_non_matching_prev_term, \
                       __uuid_str,                                      \
                       ##__VA_ARGS__);                                  \
        break;                                                          \
default:                                                                \
        break;                                                          \
    }                                                                   \
}

#define DBG_RAFT_ENTRY(log_level, re, fmt, ...)                         \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "re@%p crc=%u size=%u idx=%ld term=%ld lcm=%hhx "fmt, \
                   (re), (re)->reh_crc, (re)->reh_data_size,            \
                   (re)->reh_index, (re)->reh_term,                     \
                   (re)->reh_leader_change_marker , ##__VA_ARGS__)

#define DBG_RAFT_ENTRY_FATAL_IF(cond, re, message, ...)                 \
{                                                                       \
    if ((cond))                                                         \
    {                                                                   \
        DBG_RAFT_ENTRY(LL_FATAL, re, message, ##__VA_ARGS__);           \
    }                                                                   \
}

#define DBG_RAFT_INSTANCE(log_level, ri, fmt, ...)                      \
{                                                                       \
    char __uuid_str[UUID_STR_LEN];                                      \
    uuid_unparse((ri)->ri_log_hdr.rlh_voted_for, __uuid_str);           \
    char __leader_uuid_str[UUID_STR_LEN] = {0};                         \
    if (ri->ri_csn_leader && !raft_instance_is_leader((ri)))            \
        uuid_unparse((ri)->ri_csn_leader->csn_uuid,                     \
                     __leader_uuid_str);                                \
                                                                        \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "%c et=%ld ei=%ld ht=%ld hs=%ld ci=%ld:%ld v=%s l=%s " \
                   fmt,                                                 \
                   raft_server_state_to_char((ri)->ri_state),           \
                   raft_server_get_current_raft_entry_term((ri)),       \
                   raft_server_get_current_raft_entry_index((ri)),      \
                   (ri)->ri_log_hdr.rlh_term,                           \
                   (ri)->ri_log_hdr.rlh_seqno,                          \
                   (ri)->ri_commit_idx, (ri)->ri_last_applied_idx,      \
                   __uuid_str, __leader_uuid_str, ##__VA_ARGS__);       \
}

#define DBG_RAFT_INSTANCE_FATAL_IF(cond, ri, message, ...)              \
{                                                                       \
    if ((cond))                                                         \
    {                                                                   \
        DBG_RAFT_INSTANCE(LL_FATAL, ri, message, ##__VA_ARGS__);        \
    }                                                                   \
}

static inline enum raft_epoll_handles
raft_server_evp_2_epoll_handle(enum raft_server_event_pipes evps)
{
    switch (evps)
    {
    case RAFT_SERVER_EVP_AE_SEND:
        return RAFT_EPOLL_HANDLE_EVP_AE_SEND;
    case RAFT_SERVER_EVP_SM_APPLY:
        return RAFT_EPOLL_HANDLE_EVP_SM_APPLY;
    default:
        break;
    }
    return RAFT_EPOLL_NUM_HANDLES;
}

static inline char
raft_server_state_to_char(enum raft_state state)
{
    switch (state)
    {
    case RAFT_STATE_LEADER:
        return 'L';
    case RAFT_STATE_BOOTING:
        return 'B';
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

static inline char *
raft_server_state_to_string(enum raft_state state)
{
    switch (state)
    {
    case RAFT_STATE_LEADER:
        return "leader";
    case RAFT_STATE_BOOTING:
        return "booting";
    case RAFT_STATE_FOLLOWER:
        return "follower";
    case RAFT_STATE_CANDIDATE:
        return "candidate";
    case RAFT_STATE_CLIENT:
        return "client";
    default:
        break;
    }

    return "unknown";
}

static inline bool
raft_instance_is_client(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_state == RAFT_STATE_CLIENT ? true : false;
}

static inline bool
raft_instance_is_leader(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_state == RAFT_STATE_LEADER ? true : false;
}

static inline bool
raft_instance_is_candidate(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_state == RAFT_STATE_CANDIDATE ? true : false;
}

static inline bool
raft_instance_is_follower(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_state == RAFT_STATE_FOLLOWER ? true : false;
}

static inline bool
raft_instance_is_booting(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_state == RAFT_STATE_BOOTING ? true : false;
}

static inline bool
raft_num_members_is_valid(const raft_peer_t num_raft_members)
{
    return (num_raft_members > CTL_SVC_MAX_RAFT_PEERS ||
            num_raft_members < CTL_SVC_MIN_RAFT_PEERS) ? false : true;
}

static inline raft_peer_t
raft_num_members_validate_and_get(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri && ri->ri_csn_raft);

    const raft_peer_t num_peers =
	ctl_svc_node_raft_2_num_members(ri->ri_csn_raft);

    NIOVA_ASSERT(raft_num_members_is_valid(num_peers));

    return num_peers;
}

static inline bool
raft_member_idx_is_valid(const struct raft_instance *ri,
                         const raft_peer_t member)
{
    return member < raft_num_members_validate_and_get(ri) ? true : false;
}

static inline raft_peer_t
raft_majority_index_value(const raft_peer_t num_raft_members)
{
    if (!raft_num_members_is_valid(num_raft_members))
        return RAFT_PEER_ANY;

    return num_raft_members - (num_raft_members / 2) - 1;
}

/**
 * raft_server_entry_header_is_null - strict check which asserts that if the
 *   magic value in the header is not equal to RAFT_HEADER_MAGIC that the
 *   entry header is completely null.
 */
static inline bool
raft_server_entry_header_is_null(const struct raft_entry_header *reh)
{
    if (reh->reh_magic == RAFT_ENTRY_MAGIC)
        return false;

    const struct raft_entry_header null_reh = {0};
    NIOVA_ASSERT(!memcmp(reh, &null_reh, sizeof(struct raft_entry_header)));

    return true;
}

static inline size_t
raft_server_instance_get_num_log_headers(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri->ri_log_hdr.rlh_version == 0);

    return NUM_RAFT_LOG_HEADERS;
}

static inline size_t
raft_server_entry_idx_to_phys_idx(const struct raft_instance *ri,
                                  int64_t entry_idx)
{
    NIOVA_ASSERT(ri);

    entry_idx += raft_server_instance_get_num_log_headers(ri);

    NIOVA_ASSERT(entry_idx > 0);

    return entry_idx;
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

static inline crc32_t
raft_server_get_current_raft_entry_crc(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    return ri->ri_newest_entry_hdr.reh_crc;
}

/**
 * raft_server_get_current_phys_entry_index - obtains the physical position
 *   of the most recent entry.  This physical value is typically used for
 *   I/O.
 */
static inline int64_t
raft_server_get_current_phys_entry_index(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    int64_t current_phys_index = -1;

    if (!raft_server_entry_header_is_null(&ri->ri_newest_entry_hdr))
    {
        current_phys_index = ri->ri_newest_entry_hdr.reh_index;

        // ri_newest_entry_hdr should never have a log hdr block idx value.
        NIOVA_ASSERT(current_phys_index >= 0);

        current_phys_index += raft_server_instance_get_num_log_headers(ri);
    }

    return current_phys_index;
}

/**
 * raft_server_get_current_raft_entry_index - given a raft instance, returns
 *    the logical position of the last written application log entry.  The
 *    logical position is the physical index minus the number of
 *    NUM_RAFT_LOG_HEADERS blocks.  The values processed by this function
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
        current_reh_index = ri->ri_newest_entry_hdr.reh_index;
        NIOVA_ASSERT(current_reh_index >= 0);
    }

    return current_reh_index;
}

static inline struct raft_follower_info *
raft_server_get_follower_info(struct raft_instance *ri,
                              const raft_peer_t member)
{
    NIOVA_ASSERT(ri && raft_member_idx_is_valid(ri, member));

    return &ri->ri_leader.rls_rfi[member];
}

void
raft_server_instance_init(struct raft_instance *ri);

int
raft_server_instance_startup(struct raft_instance *ri);

int
raft_server_instance_shutdown(struct raft_instance *ri);

int
raft_server_main_loop(struct raft_instance *ri);

#endif

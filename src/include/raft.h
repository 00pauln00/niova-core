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

#include "binary_hist.h"
#include "common.h"
#include "crc32.h"
#include "ctl_svc.h"
#include "epoll_mgr.h"
#include "ev_pipe.h"
#include "raft_net.h"
#include "tcp.h"
#include "tcp_mgr.h"
#include "udp.h"
#include "util.h"

#define RAFT_ENTRY_PAD_SIZE 47
#define RAFT_ENTRY_MAGIC  0x1a2b3c4dd4c3b2a1
#define RAFT_HEADER_MAGIC 0xafaeadacabaaa9a8

#define RAFT_ENTRY_HEADER_RESERVE 128

#define RAFT_ENTRY_SIZE_MIN        65536

// Raft election timeout upper and lower bounds
#define	RAFT_ELECTION__MAX_TIME_MS 100000
#define	RAFT_ELECTION__MIN_TIME_MS 100

#define RAFT_HEARTBEAT__MIN_FREQ    2
#define RAFT_HEARTBEAT__MIN_TIME_MS 10

#define RAFT_ELECTION_UPPER_TIME_MS 300
#define RAFT_ELECTION_RANGE_DIVISOR 2.0

// Leader steps down after this many cycles following quorum loss
#define RAFT_ELECTION_CHECK_QUORUM_FACTOR 10

#define RAFT_HEARTBEAT_FREQ_PER_ELECTION 10

#define RAFT_MIN_APPEND_ENTRY_IDX -1

#define RAFT_INSTANCE_2_SELF_UUID(ri) \
    (ri)->ri_csn_this_peer->csn_uuid

#define RAFT_INSTANCE_2_RAFT_UUID(ri) \
    (ri)->ri_csn_raft->csn_uuid

typedef void                             raft_server_net_cb_ctx_t;
typedef int                              raft_server_net_cb_ctx_int_t;
typedef bool                             raft_server_net_cb_ctx_bool_t;
typedef bool                             raft_server_net_cb_follower_ctx_bool_t;
typedef int                              raft_server_net_cb_follower_ctx_int_t;
typedef void                             raft_server_net_cb_follower_ctx_t;
typedef void                             raft_server_net_cb_leader_t;
typedef void                             raft_server_net_cb_leader_ctx_t;
typedef int64_t                          raft_server_net_cb_leader_ctx_int64_t;
typedef void                             raft_server_timerfd_cb_ctx_t;
typedef int                              raft_server_timerfd_cb_ctx_int_t;
typedef void                             raft_server_leader_mode_t;
typedef int                              raft_server_leader_mode_int_t;
typedef int64_t                          raft_server_leader_mode_int64_t;
typedef void                             raft_server_epoll_remote_sender_t;
typedef void                             raft_server_epoll_sm_apply_t;
typedef void                             raft_server_epoll_sm_apply_bool_t;
typedef int                              raft_server_epoll_sm_apply_int_t;
typedef void                             raft_server_epoll_t;

typedef raft_server_epoll_sm_apply_t     raft_server_sm_apply_cb_t;
typedef raft_server_epoll_sm_apply_int_t raft_server_sm_apply_cb_int_t;

enum raft_rpc_msg_type
{
    RAFT_RPC_MSG_TYPE_INVALID                = 0,
    RAFT_RPC_MSG_TYPE_PRE_VOTE_REQUEST       = 1,
    RAFT_RPC_MSG_TYPE_PRE_VOTE_REPLY         = 2,
    RAFT_RPC_MSG_TYPE_VOTE_REQUEST           = 3,
    RAFT_RPC_MSG_TYPE_VOTE_REPLY             = 4,
    RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST = 5,
    RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY   = 6,
    RAFT_RPC_MSG_TYPE_ANY                    = 7,
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
    uuid_t  rvrpm_current_leader;
};

struct raft_append_entries_request_msg
{
    int64_t  raerqm_leader_term; // current term of the leader
    int64_t  raerqm_log_term; // term of the log entry (prev_log_index + 1)
                              // .. used for replays of old msgs ?? XXX needed?
    int64_t  raerqm_commit_index;
    int64_t  raerqm_lowest_index;  // oldest index available through raft
    int64_t  raerqm_chkpt_index;  // index of last checkpoint
    int64_t  raerqm_prev_log_term;
    int64_t  raerqm_prev_log_index;
    uint32_t raerqm_prev_idx_crc;
    uint32_t raerqm_this_idx_crc;
    uint32_t raerqm_entries_sz;
    uint8_t  raerqm_heartbeat_msg;
    uint8_t  raerqm_leader_change_marker;
    uint8_t  raerqm_entry_out_of_range;
    uint8_t  raerqm__pad[1];
    char     WORD_ALIGN_MEMBER(raerqm_entries[]); // Must be last
};

struct raft_append_entries_reply_msg
{
    int64_t raerpm_leader_term;
    int64_t raerpm_prev_log_index;
    int64_t raerpm_synced_log_index; // highest synchronized index
    uint8_t raerpm_heartbeat_msg;
    uint8_t raerpm_err_stale_term;
    uint8_t raerpm_err_non_matching_prev_term;
    uint8_t raerpm_newly_initialized_peer;
    uint8_t raerpm__pad[4];
};

struct raft_sync_idx_update_msg
{
    int64_t rsium_synced_log_index;
    int64_t rsium_term;
};

//#define RAFT_RPC_MSG_TYPE_Version0_SIZE 120

struct raft_rpc_msg
{
    uint32_t rrm_type;
    uint16_t rrm_version;
    uint16_t rrm__pad;
    uuid_t   rrm_sender_id; // should match the sender
    uuid_t   rrm_raft_id;
    uuid_t   rrm_db_id;     // Id assigned to the backend instance
    union
    {   // This union must be at the end of the structure
        struct raft_vote_request_msg           rrm_vote_request;
        struct raft_vote_reply_msg             rrm_vote_reply;
        struct raft_append_entries_request_msg rrm_append_entries_request;
        struct raft_append_entries_reply_msg   rrm_append_entries_reply;
        struct raft_sync_idx_update_msg        rrm_sync_index_update;
    };
/*  char rrm_payload[]; // future use if more msg types (other than
 *      rrm_append_entries_request require payload
 */
};

#define BW_RATE_LEN 31

struct raft_recovery_handle
{
    uuid_t          rrh_peer_uuid;
    uuid_t          rrh_peer_db_uuid;
    int64_t         rrh_peer_chkpt_idx;
    ssize_t         rrh_chkpt_size;
    ssize_t         rrh_remaining;
    ssize_t         rrh_completed;
    char            rrh_rate_bytes_per_sec[BW_RATE_LEN + 1];
    struct timespec rrh_start;
    bool            rrh_from_recovery_marker;
};

struct raft_entry_header
{
    uint64_t         reh_magic; // Magic is not included in the crc
    uuid_t           reh_self_uuid; // UUID of this peer
    struct timespec  reh_store_time; // Don't include in CRC - may differ across peers
    crc32_t          reh_crc; // Crc is after the magic and uuid's (all are constant).
    uint32_t         reh_data_size; // The size of the log entry data - must be below reh_crc!
                                    //    see raft_server_entry_calc_crc() before changing.
    raft_entry_idx_t reh_index;
    int64_t          reh_term; // Term in which entry was originally written
    uuid_t           reh_raft_uuid; // UUID of raft instance
    uint8_t          reh_leader_change_marker; // noop
    uint8_t          reh_pad[RAFT_ENTRY_PAD_SIZE];
};

struct raft_entry
{
    struct raft_entry_header re_header; // Must directly precede re_data
    char                     re_data[];
};

static inline size_t
raft_server_entry_to_total_size(const struct raft_entry *re)
{
    return (size_t)(sizeof(struct raft_entry_header) +
                    re->re_header.reh_data_size);
}

struct raft_log_header
{
    uint64_t rlh_version;
    uint64_t rlh_magic;
    int64_t  rlh_term;
    uint64_t rlh_seqno;
    uuid_t   rlh_voted_for;
};

#define RAFT_LOG_HEADER_DATA_SIZE sizeof(struct raft_log_header)

enum raft_process_state
{
    RAFT_PROC_STATE_BOOTING,
    RAFT_PROC_STATE_RUNNING,
    RAFT_PROC_STATE_RECOVERING,
    RAFT_PROC_STATE_SHUTDOWN,
} PACKED;

enum raft_state
{
    RAFT_STATE__NONE = 0,
    RAFT_STATE_LEADER,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE_PREVOTE,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_CLIENT,
} PACKED;

#define RAFT_LOG_SUFFIX_MAX_LEN 8

#define RAFT_EPOLL_HANDLES_MAX 8
#define RAFT_EVP_HANDLES_MAX 4

enum raft_epoll_handles
{
    RAFT_EPOLL_HANDLE_PEER_UDP,
    RAFT_EPOLL_HANDLE_CLIENT_UDP,
    RAFT_EPOLL_HANDLE_TIMERFD,
    RAFT_EPOLL_HANDLE_EVP_REMOTE_SEND,
    RAFT_EPOLL_HANDLE_EVP_ASYNC_COMMIT_IDX_ADV,
    RAFT_EPOLL_HANDLE_EVP_SM_APPLY,
    RAFT_EPOLL_NUM_HANDLES,
    RAFT_EPOLL_HANDLE_EVP_ANY, // purposely out of range
    RAFT_EPOLL_HANDLE_NONE,
} PACKED;

enum raft_vote_result
{
    RAFT_VOTE_RESULT_UNKNOWN,
    RAFT_PRE_VOTE_RESULT_YES,
    RAFT_PRE_VOTE_RESULT_NO,
    RAFT_VOTE_RESULT_YES,
    RAFT_VOTE_RESULT_NO,
} PACKED;

struct raft_candidate_state
{
    int64_t               rcs_term;
    bool                  rcs_prevote;
    enum raft_vote_result rcs_results[CTL_SVC_MAX_RAFT_PEERS];
};

struct raft_follower_info
{
    int64_t            rfi_next_idx;
    int64_t            rfi_ackd_idx;
    int64_t            rfi_synced_idx;
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

enum raft_follower_reasons
{
    RAFT_BFRSN_NONE,
    RAFT_BFRSN_VOTED_FOR_PEER,
    RAFT_BFRSN_STALE_TERM_WHILE_CANDIDATE,
    RAFT_BFRSN_STALE_TERM_WHILE_LEADER,
    RAFT_BFRSN_PRE_VOTE_REJECTED,
    RAFT_BFRSN_LEADER_ALREADY_PRESENT,
};

enum raft_instance_hist_types
{
    RAFT_INSTANCE_HIST_MIN                = 0,
    RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC    = 0,
    RAFT_INSTANCE_HIST_READ_LAT_MSEC      = 1,
    RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC  = 2,
    RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC = 3,
    RAFT_INSTANCE_HIST_DEV_SYNC_LAT_USEC  = 4,
    RAFT_INSTANCE_HIST_NENTRIES_SYNC      = 5,
    RAFT_INSTANCE_HIST_CHKPT_LAT_USEC     = 6,
    RAFT_INSTANCE_HIST_MAX                = 7,
    RAFT_INSTANCE_HIST_CLIENT_MAX = RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC,
};

struct raft_instance_hist_stats
{
    enum raft_instance_hist_types rihs_type;
    struct binary_hist            rihs_bh;
    struct lreg_node              rihs_lrn;
};

/**
 * raft_instance_backend API - this structure contains the set of functions
 *    required to implement a niova-raft backend.  Note that function types
 *    which take struct raft_entry or raft_entry_header are to use the
 *    raft-entry contents to address and size the respective requests.
 * @rib_entry_write:  synchronously writes the supplied raft-entry at the
 *    index set in the raft-entry header.
 * @rib_entry_header_read:  reads only the header section at the index
 *    specified in the provided raft_entry_header.
 * @rib_entry_read:  reads the entire raft entry at the supplied index.  In
 *    general, niova-raft will have already requested the header in preparation
 *    for the read and will have specified the read size accordingly.
 * @rib_log_truncate:  causes all raft entries at and beyond the
 *    raft_entry_idx_t to be nullified from the raft log such that read
 *    operations of those addresses do not produce seemingly valid raft
 *    entries.
 * @rib_log_reap:  Reap is opposite of truncate - it causes the removal of
 *    entries older than and including the provided raft_entry_idx_t.  This
 *    API call is optional and is intended for backends whose application data
 *    are guaranteed to be persistent
 *    (ie RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP).
 * @rib_header_load:  read-in the raft header.  The header stores frequently
 *    adjusted raft metadata such as the last known term value and the peer
 *    whom was last voted for.
 * @rib_header_write:  synchronously writes changes to the raft header.
 * @rib_backend_setup:  perform procedures necessary for preparing the backend
 *    for use.
 * @rib_backend_shutdown:  stop / close the backend.
 * @rib_backend_sync:  force a sync of all pending items to the backing store
 * @rib_backend_checkpoint:  optional API call for backends which support
 *    some form of checkpointing, such as rocksDB.
 * @rib_sm_apply_opt:  optional callback used for niova-raft implementations
 *    which require conjoined, atomic, persistent updates of raft metadata and
 *    state machine data.

 */
struct raft_instance_backend
{
    void    (*rib_entry_write)(struct raft_instance *,
                               const struct raft_entry *,
                               const struct raft_net_sm_write_supplements *);
    int     (*rib_entry_header_read)(struct raft_instance *,
                                     struct raft_entry_header *);
    ssize_t (*rib_entry_read)(struct raft_instance *, struct raft_entry *);
    void    (*rib_log_truncate)(struct raft_instance *,
                                const raft_entry_idx_t);
    void    (*rib_log_reap)(struct raft_instance *, const raft_entry_idx_t);
    int     (*rib_header_load)(struct raft_instance *);
    int     (*rib_header_write)(struct raft_instance *);
    int     (*rib_backend_setup)(struct raft_instance *);
    int     (*rib_backend_shutdown)(struct raft_instance *);
    int     (*rib_backend_sync)(struct raft_instance *);
    int64_t (*rib_backend_checkpoint)(struct raft_instance *);
    int     (*rib_backend_recover)(struct raft_instance *);
    void    (*rib_sm_apply_opt)(struct raft_instance *,
                                const struct raft_net_sm_write_supplements *);
};

enum raft_instance_newest_entry_hdr_types
{
    RI_NEHDR__START = 0,
    RI_NEHDR_SYNC = 0,
    RI_NEHDR_UNSYNC = 1,
    RI_NEHDR_ALL = 2,
    RI_NEHDR__END = RI_NEHDR_ALL,
};

typedef niova_atomic64_t raft_chkpt_thread_atomic64_t;

struct raft_evp
{
    struct ev_pipe             revp_evp;
    enum raft_event_pipe_types revp_type;
    uint8_t                    revp_installed_on_epm : 1;
};

struct raft_instance_buffer
{
    char    *ribuf_buf;
    size_t   ribuf_size;
    bool     ribuf_free;
};

#define RAFT_INSTANCE_NUM_BUFS 2UL
struct raft_instance_buf_pool
{
    size_t                      ribufp_nbufs;
    struct raft_instance_buffer ribufp_bufs[RAFT_INSTANCE_NUM_BUFS];
};

struct raft_instance
{
    struct udp_socket_handle        ri_ush[RAFT_UDP_LISTEN_MAX];
    struct tcp_mgr_instance         ri_peer_tcp_mgr;
    struct tcp_mgr_instance         ri_client_tcp_mgr;
    struct ctl_svc_node            *ri_csn_raft;
    struct ctl_svc_node            *ri_csn_raft_peers[CTL_SVC_MAX_RAFT_PEERS];
    struct ctl_svc_node            *ri_csn_this_peer;
    struct ctl_svc_node            *ri_csn_leader;
    struct timespec                 ri_last_send[CTL_SVC_MAX_RAFT_PEERS];
    struct timespec                 ri_last_recv[CTL_SVC_MAX_RAFT_PEERS];
    const char                     *ri_raft_uuid_str;
    const char                     *ri_this_peer_uuid_str;
    uuid_t                          ri_db_uuid; // set by backend
    uuid_t                          ri_db_recovery_uuid; // set by backend
    struct raft_candidate_state     ri_candidate;
    struct raft_leader_state        ri_leader;
    enum raft_state                 ri_state;
    enum raft_process_state         ri_proc_state;
    enum raft_instance_store_type   ri_store_type;
    bool                            ri_ignore_timerfd;
    bool                            ri_synchronous_writes;
    bool                            ri_user_requested_checkpoint;
    bool                            ri_user_requested_reap;
    bool                            ri_auto_checkpoints_enabled;
    bool                            ri_needs_bulk_recovery;
    bool                            ri_lreg_registered;
    bool                            ri_incomplete_recovery;
    bool                            ri_successful_recovery;
    enum raft_follower_reasons      ri_follower_reason;
    int                             ri_startup_error;
    int                             ri_timer_fd;
    char                            ri_log[PATH_MAX + 1];
    struct raft_log_header          ri_log_hdr;
    int64_t                         ri_commit_idx;
    int64_t                         ri_last_applied_idx;
    int64_t                         ri_last_applied_synced_idx;
    crc32_t                         ri_last_applied_cumulative_crc;
    raft_chkpt_thread_atomic64_t    ri_checkpoint_last_idx;
    raft_chkpt_thread_atomic64_t    ri_lowest_idx; // set by log reap
    pthread_mutex_t                 ri_compaction_mutex;
    raft_entry_idx_t                ri_pending_read_idx; // protected by mutex
    int                             ri_last_chkpt_err;
    unsigned long long              ri_sync_freq_us;
    size_t                          ri_sync_cnt;
    ssize_t                         ri_max_scan_entries;
    size_t                          ri_log_reap_factor;
    size_t                          ri_num_checkpoints;
    const size_t                    ri_max_entry_size;
    struct raft_entry_header        ri_newest_entry_hdr[RI_NEHDR_ALL];
    pthread_mutex_t                 ri_newest_entry_mutex;
    struct epoll_mgr                ri_epoll_mgr;
    struct epoll_handle             ri_epoll_handles[RAFT_EPOLL_HANDLES_MAX];
    uint32_t                        ri_election_timeout_max_ms;
    uint32_t                        ri_heartbeat_freq_per_election_min;
    uint32_t                        ri_check_quorum_timeout_factor;
    raft_net_timer_cb_t             ri_timer_fd_cb;
    raft_net_cb_t                   ri_client_recv_cb;
    raft_net_cb_t                   ri_server_recv_cb;
    raft_sm_request_handler_t       ri_server_sm_request_cb;
    raft_net_startup_pre_bind_cb_t  ri_startup_pre_net_bind_cb;
    raft_net_shutdown_cb_t          ri_shutdown_cb;
    struct raft_evp                 ri_evps[RAFT_EVP_HANDLES_MAX];
    size_t                          ri_evps_in_use;
    struct lreg_node                ri_lreg;
    struct lreg_node                ri_net_lreg;
    struct raft_instance_hist_stats ri_rihs[RAFT_INSTANCE_HIST_MAX];
    struct raft_instance_backend   *ri_backend;
    void                           *ri_backend_arg;
    void                           *ri_backend_init_arg;
    void                           *ri_client_arg;
    raft_entry_idx_t                ri_entries_detected_at_startup;
    struct thread_ctl               ri_sync_thread_ctl;
    struct thread_ctl               ri_chkpt_thread_ctl;
    struct raft_recovery_handle     ri_recovery_handle;
    struct raft_instance_buf_pool  *ri_buf_pool;
};

static inline struct raft_recovery_handle *
raft_instance_2_recovery_handle(struct raft_instance *ri)
{
    return ri ? &ri->ri_recovery_handle : NULL;
}

static inline void
raft_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(RAFT_ELECTION_UPPER_TIME_MS > 0);
    COMPILE_TIME_ASSERT(sizeof(struct raft_entry_header) ==
                        RAFT_ENTRY_HEADER_RESERVE);
    COMPILE_TIME_ASSERT((RAFT_ELECTION_UPPER_TIME_MS /
                         RAFT_HEARTBEAT_FREQ_PER_ELECTION) >
                        RAFT_HEARTBEAT__MIN_TIME_MS);
}

#define DBG_RAFT_MSG(log_level, rm, fmt, ...)                           \
do {                                                                \
    DEBUG_BLOCK(log_level) {                                            \
        char __uuid_str[UUID_STR_LEN];                                  \
        uuid_unparse((rm)->rrm_sender_id, __uuid_str);                  \
        switch ((rm)->rrm_type)                                         \
        {                                                               \
        case RAFT_RPC_MSG_TYPE_VOTE_REQUEST:   /* fall through */       \
        case RAFT_RPC_MSG_TYPE_PRE_VOTE_REQUEST:                        \
            LOG_MSG(log_level,                                          \
                    "%sVREQ nterm=%ld last=%ld:%ld %s "fmt,             \
                    (rm)->rrm_type == RAFT_RPC_MSG_TYPE_PRE_VOTE_REQUEST ? "PRE-" : "", \
                    (rm)->rrm_vote_request.rvrqm_proposed_term,         \
                    (rm)->rrm_vote_request.rvrqm_last_log_term,         \
                    (rm)->rrm_vote_request.rvrqm_last_log_index,        \
                    __uuid_str,                                         \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        case RAFT_RPC_MSG_TYPE_VOTE_REPLY:                              \
        case RAFT_RPC_MSG_TYPE_PRE_VOTE_REPLY:                          \
            LOG_MSG(log_level,                                          \
                    "%sVREPLY term=%ld granted=%s %s "fmt,              \
                    (rm)->rrm_type == RAFT_RPC_MSG_TYPE_PRE_VOTE_REQUEST ? "PRE-" :	"", \
                    (rm)->rrm_vote_reply.rvrpm_term,                    \
                    ((rm)->rrm_vote_reply.rvrpm_voted_granted ?         \
                     "yes" : "no"),                                     \
                    __uuid_str,                                         \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REQUEST:                  \
            LOG_MSG(log_level,                                          \
                    "AE_REQ t=%ld lt=%ld ci=%ld li:%ld pl=%ld:%ld sz=%u hb=%hhx lcm=%hhx oor=%hhx crc=%u:%u %s "fmt, \
                    (rm)->rrm_append_entries_request.raerqm_leader_term, \
                    (rm)->rrm_append_entries_request.raerqm_log_term,   \
                    (rm)->rrm_append_entries_request.raerqm_commit_index, \
                    (rm)->rrm_append_entries_request.raerqm_lowest_index, \
                    (rm)->rrm_append_entries_request.raerqm_prev_log_term, \
                    (rm)->rrm_append_entries_request.raerqm_prev_log_index, \
                    (rm)->rrm_append_entries_request.raerqm_entries_sz, \
                    (rm)->rrm_append_entries_request.raerqm_heartbeat_msg, \
                    (rm)->rrm_append_entries_request.raerqm_leader_change_marker, \
                    (rm)->rrm_append_entries_request.raerqm_entry_out_of_range, \
                    (rm)->rrm_append_entries_request.raerqm_prev_idx_crc, \
                    (rm)->rrm_append_entries_request.raerqm_this_idx_crc, \
                    __uuid_str,                                         \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        case RAFT_RPC_MSG_TYPE_APPEND_ENTRIES_REPLY:                    \
            LOG_MSG(log_level,                                          \
                    "AE_REPLY t=%ld pli=%ld sli=%ld hb=%hhx err=%hhx:%hhx %s "fmt, \
                    (rm)->rrm_append_entries_reply.raerpm_leader_term,  \
                    (rm)->rrm_append_entries_reply.raerpm_prev_log_index, \
                    (rm)->rrm_append_entries_reply.raerpm_synced_log_index, \
                    (rm)->rrm_append_entries_reply.raerpm_heartbeat_msg, \
                    (rm)->rrm_append_entries_reply.raerpm_err_stale_term, \
                    (rm)->rrm_append_entries_reply.raerpm_err_non_matching_prev_term, \
                    __uuid_str,                                         \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        default:                                                        \
            LOG_MSG(log_level, "UNKNOWN "fmt, ##__VA_ARGS__);           \
            break;                                                      \
        }                                                               \
    }                                                                   \
} while (0)

#define DBG_RAFT_ENTRY(log_level, re, fmt, ...)                   \
    LOG_MSG(log_level,                                            \
            "re@%p crc=%u size=%u idx=%ld term=%ld lcm=%hhx "fmt, \
            (re), (re)->reh_crc, (re)->reh_data_size,             \
            (re)->reh_index, (re)->reh_term,                      \
            (re)->reh_leader_change_marker , ##__VA_ARGS__)

#define DBG_RAFT_ENTRY_FATAL_IF(cond, re, message, ...)       \
do {                                                          \
    if ((cond))                                               \
    {                                                         \
        DBG_RAFT_ENTRY(LL_FATAL, re, message, ##__VA_ARGS__); \
    }                                                         \
} while (0)

#define _DBG_RAFT_INSTANCE(log_level, tag, ri, fmt, ...)           \
do {                                                               \
    DEBUG_BLOCK(log_level) {                                           \
        char __uuid_str[UUID_STR_LEN];                                 \
        uuid_unparse((ri)->ri_log_hdr.rlh_voted_for, __uuid_str);      \
        char __leader_uuid_str[UUID_STR_LEN] = {0};                    \
        if (ri->ri_csn_leader && !raft_instance_is_leader((ri)))       \
            uuid_unparse((ri)->ri_csn_leader->csn_uuid,                \
                         __leader_uuid_str);                           \
                                                                       \
        LOG_MSG_TAG(log_level, tag,                                     \
                    "%c:%c et[s:u]=%ld:%ld ei[s:u]=%ld:%ld ht=%ld hs=%ld c=%ld la=%ld:%ld lck=%lld v=%s l=%s " \
                    fmt,                                                \
                    raft_server_process_state_to_char((ri)->ri_proc_state), \
                    raft_server_state_to_char((ri)->ri_state),          \
                    raft_server_get_current_raft_entry_term(ri, RI_NEHDR_SYNC), \
                    raft_server_get_current_raft_entry_term(ri, RI_NEHDR_UNSYNC), \
                    raft_server_get_current_raft_entry_index(ri, RI_NEHDR_SYNC), \
                    raft_server_get_current_raft_entry_index(ri, RI_NEHDR_UNSYNC), \
                    (ri)->ri_log_hdr.rlh_term,                          \
                    (ri)->ri_log_hdr.rlh_seqno,                         \
                    (ri)->ri_commit_idx, (ri)->ri_last_applied_idx,     \
                    (ri)->ri_last_applied_synced_idx,                   \
                    niova_atomic_read(&(ri)->ri_checkpoint_last_idx),   \
                    __uuid_str, __leader_uuid_str,                      \
                    ##__VA_ARGS__);                                     \
    }                                                                   \
} while (0)

#define DBG_RAFT_INSTANCE(log_level, ri, fmt, ...)              \
    _DBG_RAFT_INSTANCE(log_level, NULL, ri, fmt, ##__VA_ARGS__)

#define DBG_RAFT_INSTANCE_TAG(log_level, tag, ri, fmt, ...)      \
    _DBG_RAFT_INSTANCE(log_level, tag, ri, fmt, ##__VA_ARGS__)

#define DBG_RAFT_INSTANCE_FATAL_IF(cond, ri, message, ...)       \
do {                                                             \
    if ((cond))                                                  \
    {                                                            \
        DBG_RAFT_INSTANCE(LL_FATAL, ri, message, ##__VA_ARGS__); \
    }                                                            \
} while (0)

static inline char
raft_server_state_to_char(enum raft_state state)
{
    switch (state)
    {
    case RAFT_STATE__NONE:
        return 'n';
    case RAFT_STATE_LEADER:
        return 'L';
    case RAFT_STATE_FOLLOWER:
        return 'F';
    case RAFT_STATE_CANDIDATE:
        return 'C';
    case RAFT_STATE_CANDIDATE_PREVOTE:
        return 'P';
    case RAFT_STATE_CLIENT:
        return 'c';
    default:
        break;
    }

    return '?';
}

static inline char
raft_server_process_state_to_char(enum raft_process_state state)
{
    switch (state)
    {
    case RAFT_PROC_STATE_BOOTING:
        return 'b';
    case RAFT_PROC_STATE_RUNNING:
        return 'r';
    case RAFT_PROC_STATE_RECOVERING:
        return 'R';
    case RAFT_PROC_STATE_SHUTDOWN:
        return 's';
    default:
        break;
    }

    return '?';
}

static inline char *
raft_server_process_state_to_string(enum raft_process_state state)
{
    switch (state)
    {
    case RAFT_PROC_STATE_BOOTING:
        return "booting";
    case RAFT_PROC_STATE_RUNNING:
        return "running";
    case RAFT_PROC_STATE_RECOVERING:
        return "recovering";
    case RAFT_PROC_STATE_SHUTDOWN:
        return "shutdown";
    default:
        break;
    }

    return "unknown";
}

static inline const char *
raft_instance_hist_stat_2_name(enum raft_instance_hist_types hist)
{
    switch (hist)
    {
    case RAFT_INSTANCE_HIST_COMMIT_LAT_MSEC:
        return "commit-latency-msec";
    case RAFT_INSTANCE_HIST_READ_LAT_MSEC:
        return "read-latency-msec";
    case RAFT_INSTANCE_HIST_DEV_READ_LAT_USEC:
        return "dev-read-latency-usec";
    case RAFT_INSTANCE_HIST_DEV_WRITE_LAT_USEC:
        return "dev-write-latency-usec";
    case RAFT_INSTANCE_HIST_DEV_SYNC_LAT_USEC:
        return "dev-sync-latency-usec";
    case RAFT_INSTANCE_HIST_NENTRIES_SYNC:
        return "nentries-per-sync";
    case RAFT_INSTANCE_HIST_CHKPT_LAT_USEC:
        return "checkpoint-latency-usec";
    default:
        break;
    }
    return "unknown";
}

static inline char *
raft_server_state_to_string(enum raft_state state)
{
    switch (state)
    {
    case RAFT_STATE_LEADER:
        return "leader";
    case RAFT_STATE_FOLLOWER:
        return "follower";
    case RAFT_STATE_CANDIDATE_PREVOTE:
        return "candidate-prevote";
    case RAFT_STATE_CANDIDATE:
        return "candidate";
    case RAFT_STATE_CLIENT:
        return "client";
    default:
        break;
    }

    return "unknown";
}

static inline void
raft_instance_backend_type_specify(struct raft_instance *ri,
                                   enum raft_instance_store_type type)
{
    if (ri)
        ri->ri_store_type = type;
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
raft_instance_is_candidate_prevote(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_state == RAFT_STATE_CANDIDATE_PREVOTE ? true : false;
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
    return ri->ri_proc_state == RAFT_PROC_STATE_BOOTING ? true : false;
}

static inline bool
raft_instance_is_shutdown(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_proc_state == RAFT_PROC_STATE_SHUTDOWN ? true : false;
}

static inline bool
raft_instance_is_running(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_proc_state == RAFT_PROC_STATE_RUNNING ? true : false;
}

static inline bool
raft_instance_is_recovering(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);
    return ri->ri_proc_state == RAFT_PROC_STATE_RECOVERING ? true : false;
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

    FATAL_IF((!raft_num_members_is_valid(num_peers)),
             "raft-instance %s num-peers(%hhu) is not >= %u and <= %u",
             ri->ri_raft_uuid_str, num_peers, CTL_SVC_MIN_RAFT_PEERS,
             CTL_SVC_MAX_RAFT_PEERS);

    return num_peers;
}

static inline struct ctl_svc_node *
raft_peer_uuid_to_csn(struct raft_instance *ri, const uuid_t peer_uuid)
{
    if (!ri || uuid_is_null(peer_uuid))
        return NULL;

    raft_peer_t npeers = raft_num_members_validate_and_get(ri);

    for (raft_peer_t i = 0; i < npeers; i++)
        if (!uuid_compare(ri->ri_csn_raft_peers[i]->csn_uuid, peer_uuid))
            return ri->ri_csn_raft_peers[i];

    return NULL;
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

static inline int
raft_server_entry_idx_qsort_compare(const void *a, const void *b)
{
    const raft_entry_idx_t *x = (const raft_entry_idx_t *)a;
    const raft_entry_idx_t *y = (const raft_entry_idx_t *)b;

    return *x < *y ? -1 :
           *x > *y ?  1 : 0;
}

static inline raft_entry_idx_t
raft_server_get_majority_entry_idx(const raft_entry_idx_t *values,
                                   const size_t nvalues,
                                   raft_entry_idx_t *ret_val)
{
    if (!values || !ret_val)
        return -EINVAL;

    else if (nvalues > CTL_SVC_MAX_RAFT_PEERS)
        return -E2BIG;

    qsort((void *)values, nvalues, sizeof(raft_entry_idx_t),
          raft_server_entry_idx_qsort_compare);

    *ret_val = values[raft_majority_index_value((raft_peer_t)nvalues)];

    return 0;
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

    struct raft_entry_header null_reh = {0};
    null_reh.reh_index = -1ULL;

    NIOVA_ASSERT(!memcmp(reh, &null_reh, sizeof(struct raft_entry_header)));

    return true;
}

static inline void
raft_instance_get_newest_header(struct raft_instance *ri,
                                struct raft_entry_header *reh,
                                enum raft_instance_newest_entry_hdr_types type)
{
    NIOVA_ASSERT(ri && reh && (type == RI_NEHDR_SYNC ||
                               type == RI_NEHDR_UNSYNC));

    if (raft_instance_is_client(ri))
        return;

    pthread_mutex_lock(&ri->ri_newest_entry_mutex);

    *reh = ri->ri_newest_entry_hdr[type];

    pthread_mutex_unlock(&ri->ri_newest_entry_mutex);
}

/**
 * raft_server_get_current_raft_entry_term - returns the term value from the
 *    most recent raft entry to have been written to the log.  Note that
 *    ri_newest_entry_hdr never refers to a header block.
 */
static inline int64_t
raft_server_get_current_raft_entry_term(
    struct raft_instance *ri,
    enum raft_instance_newest_entry_hdr_types type)
{
    NIOVA_ASSERT(ri);

    if (raft_instance_is_client(ri))
        return 0;

    struct raft_entry_header reh = {0};
    raft_instance_get_newest_header(ri, &reh, type);

    return reh.reh_term;
}

static inline uint32_t
raft_server_get_current_raft_entry_data_size(
    struct raft_instance *ri,
    enum raft_instance_newest_entry_hdr_types type)
{
    NIOVA_ASSERT(ri);

    if (raft_instance_is_client(ri))
        return 0;

    struct raft_entry_header reh = {0};
    raft_instance_get_newest_header(ri, &reh, type);

    return reh.reh_data_size;
}

static inline crc32_t
raft_server_get_current_raft_entry_crc(
    struct raft_instance *ri,
    enum raft_instance_newest_entry_hdr_types type)
{
    NIOVA_ASSERT(ri);

    if (raft_instance_is_client(ri))
        return 0;

    struct raft_entry_header reh = {0};
    raft_instance_get_newest_header(ri, &reh, type);

    return reh.reh_crc;
}

static inline bool
raft_server_does_synchronous_writes(const struct raft_instance *ri)
{
    return ri->ri_synchronous_writes;
}

/**
 * raft_server_get_current_raft_entry_index - given a raft instance, returns
 *    the logical position of the last written application log entry.
 */
static inline raft_entry_idx_t
raft_server_get_current_raft_entry_index(
    struct raft_instance *ri,
    enum raft_instance_newest_entry_hdr_types type)
{
    NIOVA_ASSERT(ri);

    if (raft_instance_is_client(ri))
        return -1;

    raft_entry_idx_t current_reh_index = -1;

    struct raft_entry_header reh = {0};
    raft_instance_get_newest_header(ri, &reh, type);

    if (!raft_server_entry_header_is_null(&reh))
    {
        current_reh_index = reh.reh_index;
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

int
raft_server_entry_check_crc(const struct raft_entry *re);

// May be used by backends to prepare a header block
void
raft_server_entry_init_for_log_header(const struct raft_instance *ri,
                                      struct raft_entry *re,
                                      const raft_entry_idx_t re_idx,
                                      const uint64_t current_term,
                                      const char *data, const size_t len);

void
raft_server_backend_use_posix(struct raft_instance *ri);

void
raft_server_backend_use_rocksdb(struct raft_instance *ri);

int
raft_server_instance_run(const char *raft_uuid_str,
                         const char *this_peer_uuid_str,
                         raft_sm_request_handler_t sm_request_handler,
                         enum raft_instance_store_type type,
                         enum raft_instance_options opts, void *arg);

void
raft_server_backend_setup_last_applied(struct raft_instance *ri,
                                       raft_entry_idx_t last_applied_idx,
                                       crc32_t last_applied_cumulative_crc);

int
raft_server_init_recovery_handle_from_marker(struct raft_instance *ri,
                                             const char *peer_uuid_str,
                                             const char *db_uuid_str);

/**
 * raft_server_instance_chkpt_compact_max_idx - return the value which
 *    representing the raft-entry-idx which will not be rolled back.
 *    Here we choosed the ri_last_applied_idx over ri_commmit_idx to remove
 *    the possibility of compaction occurring on entries which have yet to be
 *    applied.
 */
static inline raft_entry_idx_t
raft_server_instance_chkpt_compact_max_idx(const struct raft_instance *ri)
{
    NIOVA_ASSERT(ri);

    return ri->ri_last_applied_synced_idx;
}
#endif

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_NET_H_
#define __NIOVA_RAFT_NET_H_ 1

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/udp.h>

#include "common.h"
#include "epoll_mgr.h"
#include "log.h"
#include "net_ctl.h"
#include "tcp.h"

#define RAFT_NET_SEQNO_ANY -1ULL

struct raft_instance;
struct epoll_handle;
struct ctl_svc_node;
struct sockaddr_in;

#define RAFT_INSTANCE_PERSISTENT_APP_SCAN_ENTRIES     100000
#define RAFT_INSTANCE_PERSISTENT_APP_MIN_SCAN_ENTRIES 5000
#define RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_MAX  100
#define RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_MIN  2
#define RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_DEFAULT 5
#define RAFT_INSTANCE_PERSISTENT_APP_CHKPT_MAX 100
#define RAFT_INSTANCE_PERSISTENT_APP_CHKPT_MIN 2
#define RAFT_INSTANCE_PERSISTENT_APP_CHKPT_DEFAULT 5

#define RAFT_NET_BINARY_HIST_SIZE 20

#define RAFT_NET_PEER_RECENCY_NO_RECV -1ULL
#define RAFT_NET_PEER_RECENCY_NO_SEND -2ULL

typedef void     raft_net_cb_ctx_t;
typedef int      raft_net_cb_ctx_int_t;
typedef bool     raft_net_cb_ctx_bool_t;
typedef void     raft_net_timerfd_cb_ctx_t;
typedef int      raft_net_timerfd_cb_ctx_int_t;
typedef bool     raft_net_timerfd_cb_ctx_bool_t;
typedef void     raft_net_leader_prep_cb_ctx_t;

typedef uint64_t raft_net_request_tag_t;
#define RAFT_NET_TAG_NONE 0UL

struct raft_client_rpc_msg;
struct raft_net_client_request_handle;

enum raft_event_pipe_types
{
    RAFT_EVP__NONE,
    RAFT_EVP_REMOTE_SEND,
    RAFT_EVP_ASYNC_COMMIT_IDX_ADV,
    RAFT_EVP_SM_APPLY,
    RAFT_EVP_CLIENT,
    RAFT_EVP__ANY,
    RAFT_EVP_SERVER__START = RAFT_EVP_REMOTE_SEND,
    RAFT_EVP_SERVER__END = RAFT_EVP_SM_APPLY,
};

enum raft_net_client_request_type
{
    RAFT_NET_CLIENT_REQ_TYPE_NONE,
    RAFT_NET_CLIENT_REQ_TYPE_READ,   // read previously committed data
    RAFT_NET_CLIENT_REQ_TYPE_WRITE,  // stage a new write
    RAFT_NET_CLIENT_REQ_TYPE_COMMIT, // complete a pending write
    RAFT_NET_CLIENT_REQ_TYPE_ANY,
};

typedef raft_net_cb_ctx_t
(*raft_net_cb_t)(struct raft_instance *,
                 const char *, ssize_t,
                 const struct sockaddr_in *);

typedef raft_net_timerfd_cb_ctx_t
(*raft_net_timer_cb_t)(struct raft_instance *);

// State machine request handler - reads, writes, and commits
typedef raft_net_cb_ctx_int_t
(*raft_sm_request_handler_t)(struct raft_net_client_request_handle *);

// cleanup the cowr subapp tree on getting elected as leader
typedef raft_net_leader_prep_cb_ctx_t
(*raft_leader_prep_cb_t)(void);

typedef int (*raft_net_startup_pre_bind_cb_t)(struct raft_instance *);
typedef int (*raft_net_shutdown_cb_t)(struct raft_instance *);

#define RAFT_NET_ENTRY_RESERVE 512

#define RAFT_NET_ENTRY_SIZE_POSIX   (64 * 1024)
#define RAFT_NET_MAX_RPC_SIZE_POSIX                     \
    (RAFT_NET_ENTRY_SIZE_POSIX - RAFT_NET_ENTRY_RESERVE)

#define RAFT_NET_ENTRY_SIZE_ROCKSDB (4096 * 1024)
#define RAFT_NET_MAX_RPC_SIZE_ROCKSDB                     \
    (RAFT_NET_ENTRY_SIZE_ROCKSDB - RAFT_NET_ENTRY_RESERVE)

#define RAFT_NET_MAX_RETRY_MS 30000
#define RAFT_NET_MIN_RETRY_MS 100

enum raft_instance_store_type
{
    RAFT_INSTANCE_STORE_POSIX_FLAT_FILE = 0, // keep this as default
    RAFT_INSTANCE_STORE_ROCKSDB,
    RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP,
};

static inline size_t
raft_net_max_rpc_size(enum raft_instance_store_type be_type)
{
    switch (be_type)
    {
    case RAFT_INSTANCE_STORE_POSIX_FLAT_FILE:
        return RAFT_NET_MAX_RPC_SIZE_POSIX;

    case RAFT_INSTANCE_STORE_ROCKSDB: // fall through
    case RAFT_INSTANCE_STORE_ROCKSDB_PERSISTENT_APP:
        return RAFT_NET_MAX_RPC_SIZE_ROCKSDB;

    default:
        break;
    }

    return 0;
}


// Options for raft_server_instance_run()
enum raft_instance_options
{
    RAFT_INSTANCE_OPTIONS_NONE                 = 0,
    RAFT_INSTANCE_OPTIONS_SYNC_WRITES          = 1 << 0,
    RAFT_INSTANCE_OPTIONS_COALESCED_WRITES     = 1 << 1,
    RAFT_INSTANCE_OPTIONS_AUTO_CHECKPOINT      = 1 << 2,
    RAFT_INSTANCE_OPTIONS_DISABLE_UDP          = 1 << 3,
    RAFT_INSTANCE_OPTIONS_DISABLE_TCP          = 1 << 4,
};

enum raft_udp_listen_sockets
{
    RAFT_UDP_LISTEN_MIN    = 0,
    RAFT_UDP_LISTEN_SERVER = RAFT_UDP_LISTEN_MIN,
    RAFT_UDP_LISTEN_CLIENT = 1,
    RAFT_UDP_LISTEN_MAX    = 2,
    RAFT_UDP_LISTEN_ANY    = RAFT_UDP_LISTEN_MAX,
};

enum raft_client_rpc_msg_type
{
    RAFT_CLIENT_RPC_MSG_TYPE_INVALID    = 0,
    RAFT_CLIENT_RPC_MSG_TYPE_REQUEST    = 1,
    RAFT_CLIENT_RPC_MSG_TYPE_REPLY      = 2,
    RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT   = 3,
    RAFT_CLIENT_RPC_MSG_TYPE_PING       = 4,
    RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY = 5,
    RAFT_CLIENT_RPC_MSG_TYPE_ANY        = 6,
} PACKED;

enum raft_net_comm_recency_type
{
    RAFT_COMM_RECENCY_RECV,
    RAFT_COMM_RECENCY_SEND,
    RAFT_COMM_RECENCY_UNACKED_SEND,
};

#define RAFT_NET_CLIENT_USER_ID_V0_SZ 48
#define RAFT_NET_CLIENT_USER_ID_V0_NUINT64 \
    (RAFT_NET_CLIENT_USER_ID_V0_SZ / sizeof(uint64_t))
#define RAFT_NET_CLIENT_USER_ID_V0_NUUID \
    (RAFT_NET_CLIENT_USER_ID_V0_SZ / sizeof(uuid_t))

#define RAFT_NET_CLIENT_USER_ID_V0_STRLEN_SIZE 101 // includes NULL terminator
#define RAFT_NET_CLIENT_USER_ID_V0_STR_SEP ":"

struct raft_net_client_user_key_v0
{
    union
    {
        uint64_t rncui_v0_uint64[RAFT_NET_CLIENT_USER_ID_V0_NUINT64];
        uuid_t   rncui_v0_uuid[RAFT_NET_CLIENT_USER_ID_V0_NUUID];
    };
};

struct raft_net_client_user_key
{
    union
    {
        struct raft_net_client_user_key_v0 v0;
    };
};

// Used by raft client sub-"applications"
struct raft_net_client_user_id
{
    struct raft_net_client_user_key rncui_key;
    version_t                       rncui_version;
    uint32_t                        rncui__unused;
};

/**
 * @rcrm_type:  The type of RPC which is one of enum raft_client_rpc_msg_type
 * @rcrm_version:  Version number of this RPC.  This with the type composes
 *    a logical 8-byte header.  At this time, the version numbering isn't
 *    utilized.
 * @rcrm_data_size:  Size of the contents attached to rcrm_data.
 * @rcrm_msg_id:  64-bit unique ID for this msg.  Typically, this is derived
 *    from the client's UUID and a counter.
 * @rcrm_raft_id:  UUID of the raft instance.
 * @rcrm_sender_id:  UUID of the sender.  At this time, all client and server
 *    UUIDs are checked via the ctl-service.
 * @rcrm_dest_id:  UUID of the RPC's destination.
 * @rcrm_redirect_id:  Used in reply context to notify a client of the raft
 *    leader's UUID.
 * @rcrm_app_error:  Error info passed to the application.
 * @rcrm_sys_error:  System level error.
 * @rcrm_raft_client_app_seqno:  Optional 64-bit value for application use
 *    which resides here to assist raft applications whose writes are sequence
 *    based.  This removes the need to place the seqno in their own RPC layer.
 * @rcrm_data:  Application information.
 */
struct raft_client_rpc_msg
{
    uint32_t rcrm_type;
    uint32_t rcrm_version;
    uint32_t rcrm__pad0;
    uint32_t rcrm_data_size;
    uint64_t rcrm_msg_id;
    uuid_t   rcrm_raft_id;
    uuid_t   rcrm_sender_id;
    union
    {
        uuid_t rcrm_dest_id;
        uuid_t rcrm_redirect_id;
    };
    int16_t  rcrm_app_error;
    int16_t  rcrm_sys_error;
    uint16_t rcrm__pad1[2];
    uint64_t rcrm_user_tag;
    char     WORD_ALIGN_MEMBER(rcrm_data[]);
};

#define _RAFT_NET_MAP_RPC(type, rcrm)                     \
    ({                                                    \
        (sizeof(struct type) <= (rcrm)->rcrm_data_size) ? \
            (rcrm)->rcrm_data : NULL;                     \
    })

#define RAFT_NET_MAP_RPC(type, rcrm) \
    (struct type *)_RAFT_NET_MAP_RPC(type, rcrm)

#define RAFT_NET_MAP_RPC_CONST(type, rcrm) \
    (const struct type *)_RAFT_NET_MAP_RPC(type, rcrm)

static inline size_t
raft_client_rpc_msg_size(const size_t app_payload_size)
{
    return (sizeof(struct raft_client_rpc_msg) + app_payload_size);
}

static inline bool
raft_client_rpc_msg_size_is_valid(enum raft_instance_store_type store_type,
                                  const size_t app_payload_size)

{
    return raft_client_rpc_msg_size(app_payload_size) <=
        raft_net_max_rpc_size(store_type) ? true : false;
}

#define RAFT_NET_WR_SUPP_MAX 1024 // arbitrary limit..

/**
 * raft_net_sm_write_supplements - structure holding KV items which the state
 *    machine can provide to the underlying Raft API.  These extra items are
 *    then written transactionally alongside any raft write or apply operation.
 *    Note, this for use with flexible backend providers, such as RocksDB.
 */
struct raft_net_wr_supp
{
    size_t  rnws_nkv;
    void   *rnws_handle; // rocksdb cfhandle
    void  (*rnws_comp_cb)(void *);
    char  **rnws_keys;
    size_t *rnws_key_sizes;
    char  **rnws_values;
    size_t *rnws_value_sizes;
};

struct raft_net_sm_write_supplements
{
    size_t                   rnsws_nitems;
    struct raft_net_wr_supp *rnsws_ws;
};

struct raft_net_client_request_handle
{
    enum raft_net_client_request_type     rncr_type;  // may be set by sm cb
    bool                                  rncr_write_raft_entry;
    bool                                  rncr_is_leader;
    int                                   rncr_op_error;
    int64_t                               rncr_entry_term;
    int64_t                               rncr_current_term;
    raft_entry_idx_t                      rncr_pending_apply_idx;
    const struct raft_client_rpc_msg     *rncr_request;
    const char                           *rncr_request_or_commit_data;
    const size_t                          rncr_request_or_commit_data_size;
    struct raft_client_rpc_msg           *rncr_reply;
    const size_t                          rncr_reply_data_max_size;
    size_t                                rncr_reply_data_size;
    uint64_t                              rncr_msg_id;
    struct raft_net_sm_write_supplements  rncr_sm_write_supp;
    uuid_t                                rncr_client_uuid;
};

#define DBG_RAFT_CLIENT_RPC_SOCK(log_level, rcm, from, fmt, ...) \
    DBG_RAFT_CLIENT_RPC(log_level, rcm, "%s:%u "fmt,             \
                        inet_ntoa((from)->sin_addr),             \
                        ntohs((from)->sin_port),                 \
                        ##__VA_ARGS__)

#define DBG_RAFT_CLIENT_RPC(log_level, rcm, fmt, ...)                     \
do {                                                                    \
    DEBUG_BLOCK(log_level) {                                            \
        char __uuid_str[UUID_STR_LEN];                                  \
        char __redir_uuid_str[UUID_STR_LEN];                            \
        uuid_unparse((rcm)->rcrm_sender_id, __uuid_str);                \
        switch ((rcm)->rcrm_type)                                       \
        {                                                               \
        case RAFT_CLIENT_RPC_MSG_TYPE_REQUEST:                          \
            uuid_unparse((rcm)->rcrm_dest_id, __uuid_str);              \
            LOG_MSG(log_level,                                          \
                    "CLI-REQ   %s id=%lx sz=%u "fmt,                    \
                    __uuid_str,                                         \
                    (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,          \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        case RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT:                         \
            uuid_unparse((rcm)->rcrm_redirect_id, __redir_uuid_str);    \
            LOG_MSG(log_level,                                          \
                    "CLI-RDIR  %s id=%lx sz=%u redir-to=%s "fmt,        \
                    __uuid_str,                                         \
                    (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,          \
                    __redir_uuid_str, ##__VA_ARGS__);                   \
            break;                                                      \
        case RAFT_CLIENT_RPC_MSG_TYPE_REPLY:                            \
            LOG_MSG(log_level,                                          \
                    "CLI-REPL  %s id=%lx sz=%u err=%hd:%hd "fmt,        \
                    __uuid_str,                                         \
                    (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,          \
                    (rcm)->rcrm_sys_error, (rcm)->rcrm_app_error,       \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        case RAFT_CLIENT_RPC_MSG_TYPE_PING:        /* fall through */   \
            uuid_unparse((rcm)->rcrm_dest_id, __uuid_str);              \
        case RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY:                       \
            LOG_MSG(log_level,                                          \
                    "CLI-%s %s id=%lx err=%hd:%hd "fmt,                 \
                    (rcm)->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY ? \
                    "PREPL" : "PING ",                                  \
                    __uuid_str,                                         \
                    (rcm)->rcrm_msg_id,                                 \
                    (rcm)->rcrm_sys_error, (rcm)->rcrm_app_error,       \
                    ##__VA_ARGS__);                                     \
            break;                                                      \
        default:                                                        \
            break;                                                      \
        }                                                               \
    }                                                                   \
} while (0)

#define DBG_RAFT_CLIENT_RPC_LEADER(log_level, ri, rcm, fmt, ...) \
{                                                                \
    if ((ri)->ri_csn_leader)                                     \
    {                                                            \
        DBG_RAFT_CLIENT_RPC(log_level, rcm, fmt, ##__VA_ARGS__); \
    }                                                            \
}

static inline char *
raft_net_client_rpc_sys_error_2_string(const int rc)
{
    switch (rc)
    {
    case 0:
        return "accept";
    case -ENOENT:
        return "deny-leader-not-established";
    case -EINPROGRESS:
        return "deny-operation-in-progress";
    case -ENOSYS:
        return "redirect-to-leader";
    case -EAGAIN:
        return "deny-may-be-deposed";
    case -EBUSY:
        return "deny-determining-commit-index";
    default:
        break;
    }

    return "unknown";
}

static inline void
raft_net_compile_time_assert(void)
{
    COMPILE_TIME_ASSERT(RAFT_NET_ENTRY_RESERVE >=
                        (sizeof(struct raft_client_rpc_msg)));

    COMPILE_TIME_ASSERT(
        sizeof(raft_net_request_tag_t) <=
        sizeof((struct raft_client_rpc_msg *)0)->rcrm_user_tag);

    COMPILE_TIME_ASSERT((RAFT_INSTANCE_PERSISTENT_APP_CHKPT_DEFAULT >=
                         RAFT_INSTANCE_PERSISTENT_APP_CHKPT_MIN) &&
                        (RAFT_INSTANCE_PERSISTENT_APP_CHKPT_DEFAULT <=
                         RAFT_INSTANCE_PERSISTENT_APP_CHKPT_MAX));

    COMPILE_TIME_ASSERT((RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_DEFAULT >=
                         RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_MIN) &&
                        (RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_DEFAULT <=
                         RAFT_INSTANCE_PERSISTENT_APP_REAP_FACTOR_MAX));
}

struct raft_instance *
raft_net_get_instance(void);

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode);

int
raft_net_instance_shutdown(struct raft_instance *ri);

void
raft_net_instance_apply_callbacks(struct raft_instance *ri,
                                  raft_net_timer_cb_t timer_fd_cb,
                                  raft_net_cb_t client_recv_cb,
                                  raft_net_cb_t server_recv_cb);

raft_peer_t
raft_peer_2_idx(const struct raft_instance *ri, const uuid_t peer_uuid);

struct ctl_svc_node *
raft_net_verify_sender_server_msg(struct raft_instance *ri,
                                  const uuid_t sender_uuid,
                                  const uuid_t sender_raft_uuid,
                                  const struct sockaddr_in *sender_addr);

int
raft_net_verify_sender_client_msg(struct raft_instance *ri,
                                  const uuid_t sender_raft_uuid);

void
raft_net_update_last_comm_time(struct raft_instance *ri,
                               const uuid_t peer_uuid, bool send_or_recv);

int
raft_net_comm_get_last_recv(struct raft_instance *ri, const uuid_t peer_uuid,
                            struct timespec *ts);

raft_peer_t
raft_net_get_most_recently_responsive_server(const struct raft_instance *ri);

int
raft_net_comm_recency(const struct raft_instance *ri,
                      raft_peer_t raft_peer_idx,
                      enum raft_net_comm_recency_type type,
                      unsigned long long *ret_ms);

int
raft_net_server_target_check(const struct raft_instance *ri,
                             const uuid_t server_uuid,
                             const unsigned long long stale_timeout_ms);

int
raft_net_apply_leader_redirect(struct raft_instance *ri,
                               const uuid_t redirect_target,
                               unsigned long long stale_timeout_ms);

static inline bool
raft_net_sockaddr_is_valid(const struct sockaddr_in *sockaddr)
{
    struct sockaddr_in null_sockaddr = {0};

    return memcmp(&null_sockaddr, sockaddr, sizeof(struct sockaddr_in)) ?
        false : true;
}

int
raft_net_send_msg(struct raft_instance *ri, struct ctl_svc_node *csn,
                  struct iovec *iov, size_t niovs,
                  const enum raft_udp_listen_sockets sock_src);

int
raft_net_send_msg_to_uuid(struct raft_instance *ri, uuid_t uuid,
                          struct iovec *iov, size_t niovs,
                          const enum raft_udp_listen_sockets sock_src);

int
raft_net_send_client_msg(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm);

int
raft_net_send_client_msgv(struct raft_instance *ri,
                          struct raft_client_rpc_msg *rcrm,
                          const struct iovec *iov, size_t niovs);

void
raft_net_timerfd_settime(struct raft_instance *ri, unsigned long long msecs);


struct ev_pipe *
raft_net_evp_get(struct raft_instance *ri, enum raft_event_pipe_types type);

int
raft_net_evp_add(struct raft_instance *ri, epoll_mgr_cb_t cb,
                 enum raft_event_pipe_types type);

int
raft_net_evp_remove(struct raft_instance *ri, enum raft_event_pipe_types type);

int
raft_net_evp_notify(struct raft_instance *ri, enum raft_event_pipe_types type);

#define RAFT_NET_EVP_NOTIFY_NO_FAIL(ri, type)                           \
do {                                                                    \
    int _rc = raft_net_evp_notify(ri, type);                            \
    FATAL_IF(_rc, "raft_net_evp_notify(%d): %s", type, strerror(-_rc));  \
} while (0)

static inline void
raft_client_msg_error_set(struct raft_client_rpc_msg *rcm, int sys, int app)
{
    if (rcm)
    {
        rcm->rcrm_sys_error = sys;
        rcm->rcrm_app_error = app;
    }
}

static inline void
raft_client_net_request_handle_error_set(
    struct raft_net_client_request_handle *rncr,
    int rncr_op_err, int reply_sys, int reply_app)
{
    if (rncr)
    {
        rncr->rncr_op_error = rncr_op_err;

        if (rncr->rncr_reply)
            raft_client_msg_error_set(rncr->rncr_reply, reply_sys, reply_app);
    }
}

static inline bool
raft_client_net_request_handle_instance_is_leader(
    const struct raft_net_client_request_handle *rncr)
{
    NIOVA_ASSERT(rncr);

    return rncr->rncr_is_leader;
}

static inline struct raft_client_rpc_msg *
raft_net_data_to_rpc_msg(void *data)
{
    return OFFSET_CAST(raft_client_rpc_msg, rcrm_data, data);
}

static inline void
raft_net_client_request_handle_set_write_raft_entry(
    struct raft_net_client_request_handle *rncr)
{
    if (rncr)
        rncr->rncr_write_raft_entry = true;
}

static inline bool
raft_net_client_request_handle_writes_raft_entry(
    const struct raft_net_client_request_handle *rncr)
{
    return rncr ? rncr->rncr_write_raft_entry : false;
}

static inline void
raft_net_client_request_handle_set_reply_info(
    struct raft_net_client_request_handle *rncr,
    const uuid_t client_uuid, uint64_t msg_id)
{
    if (rncr)
    {
        // Caller may supply a null-uuid string
        uuid_copy(rncr->rncr_client_uuid, client_uuid);

        rncr->rncr_msg_id = msg_id;
    }
}

static inline bool
raft_net_client_request_handle_has_reply_info(
    const struct raft_net_client_request_handle *rncr)
{
    bool doesnt_have_info =
        (!rncr ||
         uuid_is_null(rncr->rncr_client_uuid) ||
         rncr->rncr_msg_id == ID_ANY_64bit ||
         rncr->rncr_msg_id == 0 ||
         rncr->rncr_reply_data_max_size == 0 ||
         rncr->rncr_reply == NULL);

    return !doesnt_have_info;
}

static inline char *
raft_net_client_request_handle_reply_data_map(
    struct raft_net_client_request_handle *rncr, const size_t size)
{
    if (!rncr || !size || !rncr->rncr_reply)
        return NULL;

    struct raft_client_rpc_msg *reply = rncr->rncr_reply;

    NIOVA_ASSERT(reply->rcrm_data_size <= rncr->rncr_reply_data_max_size);

    if ((reply->rcrm_data_size + size) > rncr->rncr_reply_data_max_size ||
        (rncr->rncr_reply_data_size + size) > rncr->rncr_reply_data_max_size)
        return NULL;

    char *buf = &reply->rcrm_data[rncr->rncr_reply_data_size];

    reply->rcrm_data_size += size;
    rncr->rncr_reply_data_size += size;

    return buf;
}

// Raft Net State Machine Write Supplment API
void
raft_net_sm_write_supplement_init(struct raft_net_sm_write_supplements *rnsws);

void
raft_net_sm_write_supplement_destroy(
    struct raft_net_sm_write_supplements *rnsws);

int
raft_net_sm_write_supplement_add(struct raft_net_sm_write_supplements *rnsws,
                                 void *handle, void (*rnws_comp_cb)(void *),
                                 const char *key, const size_t key_size,
                                 const char *value, const size_t value_size);

// Raft Net User ID API
static inline void
raft_net_client_user_id_init(struct raft_net_client_user_id *rncui)
{
    if (rncui)
        memset(rncui, 0, sizeof(*rncui));
}

#define RAFT_NET_CLIENT_USER_ID_2_UUID(rncui, version, index) \
    (rncui)->rncui_key.v ## version .rncui_v ## version ## _uuid[index]

#define RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, version, index) \
    (rncui)->rncui_key.v ## version .rncui_v ## version ## _uint64[index]

#define RAFT_NET_CLIENT_USER_ID_FMT "%s:%lx:%lx:%lx:%lx"
#define RAFT_NET_CLIENT_USER_ID_FMT_ARGS(rncui, uuid_str, version) \
        uuid_str,                                                  \
        RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, version, 2),       \
        RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, version, 3),       \
        RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, version, 4),       \
        RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, version, 5)

static inline int
raft_net_client_user_id_cmp(const struct raft_net_client_user_id *a,
                            const struct raft_net_client_user_id *b)
{
    NIOVA_ASSERT(a->rncui_version == 0 && b->rncui_version == 0);

    for (size_t i = 0; i < RAFT_NET_CLIENT_USER_ID_V0_NUINT64; i++)
        if (RAFT_NET_CLIENT_USER_ID_2_UINT64(a, 0, i) !=
            RAFT_NET_CLIENT_USER_ID_2_UINT64(b, 0, i))
            return RAFT_NET_CLIENT_USER_ID_2_UINT64(a, 0, i) <
                RAFT_NET_CLIENT_USER_ID_2_UINT64(b, 0, i) ?
                -1 : 1;

    return 0;
}

static inline void
raft_net_client_user_id_copy(struct raft_net_client_user_id *dest,
                             const struct raft_net_client_user_id *src)
{
    memcpy(dest, src, sizeof(struct raft_net_client_user_id));
}

static inline int
raft_net_client_user_id_2_uuid(const struct raft_net_client_user_id *rncui,
                               const version_t version, const size_t index,
                               uuid_t dst_uuid)
{
    if (!rncui)
        return -EINVAL;

    switch (version)
    {
    case 0:
        if (index > RAFT_NET_CLIENT_USER_ID_V0_NUUID)
            return -EBADSLT;

        uuid_copy(dst_uuid, RAFT_NET_CLIENT_USER_ID_2_UUID(rncui, 0, 0));
        break;

    default:
        return -ERANGE;
    }

    return 0;
}

static inline int
raft_net_client_user_id_to_string(const struct raft_net_client_user_id *rncui,
                                  char *out_string, const size_t out_string_len)
{
    if (!rncui || !out_string || !out_string_len)
        return -EINVAL;

    char uuid_str[UUID_STR_LEN];
    uuid_unparse(RAFT_NET_CLIENT_USER_ID_2_UUID(rncui, 0, 0), uuid_str);

    int rc = snprintf(out_string, out_string_len, RAFT_NET_CLIENT_USER_ID_FMT,
                      RAFT_NET_CLIENT_USER_ID_FMT_ARGS(rncui, uuid_str, 0));

    if (rc > out_string_len - 1)
        return -ENOSPC;

    return (rc > out_string_len - 1) ? -ENOSPC : 0;
}

#define raft_net_client_user_id_unparse raft_net_client_user_id_to_string

int
raft_net_client_user_id_parse(const char *in,
                              struct raft_net_client_user_id *rncui,
                              const version_t version);
void
raft_net_csn_connection_setup(struct raft_instance *ri,
                              struct ctl_svc_node *csn);

void
raft_net_set_max_scan_entries(struct raft_instance *ri,
                              ssize_t max_scan_entries);

void
raft_net_set_log_reap_factor(struct raft_instance *ri, size_t log_reap_factor);

void
raft_net_set_num_checkpoints(struct raft_instance *ri, size_t num_ckpts);

int
raft_net_sm_write_supplements_merge(struct raft_net_sm_write_supplements *dest,
                                    struct raft_net_sm_write_supplements *src);

#endif

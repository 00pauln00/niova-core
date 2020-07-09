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

#define RAFT_NET_SEQNO_ANY -1ULL

struct raft_instance;
struct epoll_handle;
struct ctl_svc_node;
struct sockaddr_in;

#define RAFT_NET_BINARY_HIST_SIZE 20

#define RAFT_NET_PEER_RECENCY_NO_RECV -1ULL
#define RAFT_NET_PEER_RECENCY_NO_SEND -2ULL

typedef void raft_net_udp_cb_ctx_t;
typedef int  raft_net_udp_cb_ctx_int_t;
typedef bool raft_net_udp_cb_ctx_bool_t;
typedef void raft_net_timerfd_cb_ctx_t;
typedef int  raft_net_timerfd_cb_ctx_int_t;

struct raft_client_rpc_msg;
struct raft_net_client_request_handle;

enum raft_net_client_request_type
{
    RAFT_NET_CLIENT_REQ_TYPE_NONE,
    RAFT_NET_CLIENT_REQ_TYPE_READ,   // read previously committed data
    RAFT_NET_CLIENT_REQ_TYPE_WRITE,  // stage a new write
    RAFT_NET_CLIENT_REQ_TYPE_COMMIT, // complete a pending write
    RAFT_NET_CLIENT_REQ_TYPE_ANY,
};

typedef raft_net_udp_cb_ctx_t
    (*raft_net_udp_cb_t)(struct raft_instance *,
                         const char *, ssize_t,
                         const struct sockaddr_in *);

typedef raft_net_timerfd_cb_ctx_t
    (*raft_net_timer_cb_t)(struct raft_instance *);

// State machine request handler - reads, writes, and commits
typedef raft_net_udp_cb_ctx_int_t
    (*raft_sm_request_handler_t)(struct raft_net_client_request_handle *);

#define RAFT_NET_MAX_RPC_SIZE 65000
#define RAFT_NET_MAX_RETRY_MS 30000
#define RAFT_NET_MIN_RETRY_MS 100

enum raft_instance_store_type
{
    RAFT_INSTANCE_STORE_POSIX_FLAT_FILE,
    RAFT_INSTANCE_STORE_ROCKSDB,
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

/* raft_client_rpc_raft_entry_data - Used by raft_client.c as the outermost
 *    encapsulation layer of data stored in a raft log entry.  This structure
 *    is presented to the raft server write and apply callbacks (via
 *    raft_client_rpc_msg::rcrm_data), whereas raft_client_rpc_msg itself is
 *    not.
 */
struct raft_client_rpc_raft_entry_data
{
    uint32_t                       rcrred_version;
    uint32_t                       rcrred_crc;
    uint32_t                       rcrred_data_size;
    uint32_t                       rcrred__pad;
    struct raft_net_client_user_id rcrred_rncui;
    char                           rcrred_data[];
};

#define RAFT_NET_CLIENT_MAX_RPC_SIZE                                    \
    (RAFT_NET_MAX_RPC_SIZE -                                            \
     (sizeof(struct raft_client_rpc_msg) +                              \
      sizeof(struct raft_client_rpc_raft_entry_data)))

struct raft_client_rpc_msg
{
    uint32_t                       rcrm_type;  // enum raft_client_rpc_msg_type
    uint32_t                       rcrm_version;
    uint32_t                       rcrm_data_size;
    uint64_t                       rcrm_msg_id;
    uuid_t                         rcrm_raft_id;
    uuid_t                         rcrm_sender_id;
    union
    {
        uuid_t                     rcrm_dest_id;
        uuid_t                     rcrm_redirect_id;
    };
    int16_t                        rcrm_app_error;
    int16_t                        rcrm_sys_error;
//int16_t                        rcrm_raft_error; for these error type: raft_net_client_rpc_sys_error_2_string()
    uint8_t                        rcrm_uses_raft_client_entry_data;
    uint8_t                        rcrm_pad[3];
    char                           rcrm_data[];
};

#define _RAFT_NET_MAP_RPC(type, rcrm)                           \
    ({                                                          \
        (sizeof(struct type) >= (rcrm)->rcrm_data_size) ?       \
            (rcrm)->rcrm_data : NULL;                           \
    })

#define RAFT_NET_MAP_RPC(type, rcrm)                    \
    (struct type *)_RAFT_NET_MAP_RPC(type, rcrm)

#define RAFT_NET_MAP_RPC_CONST(type, rcrm)              \
    (const struct type *)_RAFT_NET_MAP_RPC(type, rcrm)

static inline size_t
raft_client_rpc_msg_size(const size_t app_payload_size,
                         const bool uses_client_entry_data)
{
    return (sizeof(struct raft_client_rpc_msg) +
            (uses_client_entry_data ?
             sizeof(struct raft_client_rpc_raft_entry_data) : 0) +
            app_payload_size);
}

static inline size_t
raft_client_rpc_payload_size(const size_t app_payload_size,
                             const bool uses_client_entry_data)
{
    return ((uses_client_entry_data ?
             sizeof(struct raft_client_rpc_raft_entry_data) : 0) +
            app_payload_size);
}

static inline bool
raft_client_rpc_msg_size_is_valid(const size_t app_payload_size,
                                  const bool uses_client_entry_data)
{
    return raft_client_rpc_msg_size(app_payload_size,
                                    uses_client_entry_data) <=
        RAFT_NET_CLIENT_MAX_RPC_SIZE ? true : false;
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
    size_t   rnws_nkv;
    void    *rnws_handle; //rocksdb cfh
    void   (*rnws_comp_cb)(void *);
    char   **rnws_keys;
    size_t  *rnws_key_sizes;
    char   **rnws_values;
    size_t  *rnws_value_sizes;
};

struct raft_net_sm_write_supplements
{
    size_t                   rnsws_nitems;
    struct raft_net_wr_supp *rnsws_ws;
};

struct raft_net_client_request_handle
{
    enum raft_net_client_request_type     rncr_type; // may be set by sm callback
    bool                                  rncr_write_raft_entry;
    bool                                  rncr_is_leader;
    struct net_ctl                        rncr_nc;
    int                                   rncr_op_error;
    int64_t                               rncr_entry_term;
    int64_t                               rncr_current_term;
    raft_entry_idx_t                      rncr_pending_apply_idx;
    long long                             rncr_commit_duration_msec;
    const struct raft_client_rpc_msg     *rncr_request;
    const char                           *rncr_request_or_commit_data;
    const size_t                          rncr_request_or_commit_data_size;
    struct raft_client_rpc_msg           *rncr_reply;
    const size_t                          rncr_reply_data_max_size;
    struct sockaddr_in                    rncr_remote_addr;
    uint64_t                              rncr_msg_id;
    struct raft_net_sm_write_supplements  rncr_sm_write_supp;
    uuid_t                                rncr_client_uuid;
};

#define DBG_RAFT_CLIENT_RPC(log_level, rcm, from, fmt, ...)             \
{                                                                       \
    char __uuid_str[UUID_STR_LEN];                                      \
    char __redir_uuid_str[UUID_STR_LEN];                                \
    uuid_unparse((rcm)->rcrm_sender_id, __uuid_str);                    \
    switch ((rcm)->rcrm_type)                                           \
    {                                                                   \
    case RAFT_CLIENT_RPC_MSG_TYPE_REQUEST:                              \
        LOG_MSG(log_level,                                              \
                "CLI-REQ   %s %s:%u id=%lx sz=%u "fmt,                  \
                __uuid_str,                                             \
                inet_ntoa((from)->sin_addr), ntohs((from)->sin_port),   \
                (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,              \
                ##__VA_ARGS__);                                         \
        break;                                                          \
    case RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT:                             \
        uuid_unparse((rcm)->rcrm_redirect_id, __redir_uuid_str);        \
        LOG_MSG(log_level,                                              \
                "CLI-RDIR  %s %s:%u id=%lx sz=%u redir-to=%s "fmt,      \
                __uuid_str,                                             \
                inet_ntoa((from)->sin_addr), ntohs((from)->sin_port),   \
                (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,              \
                __redir_uuid_str, ##__VA_ARGS__);                       \
        break;                                                          \
    case RAFT_CLIENT_RPC_MSG_TYPE_REPLY:                                \
        LOG_MSG(log_level,                                              \
                "CLI-REPL  %s %s:%u id=%lx sz=%u err=%hd:%hd "fmt,      \
                __uuid_str,                                             \
                inet_ntoa((from)->sin_addr), ntohs((from)->sin_port),   \
                (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,              \
                (rcm)->rcrm_sys_error, (rcm)->rcrm_app_error,           \
                ##__VA_ARGS__);                                         \
        break;                                                          \
    case RAFT_CLIENT_RPC_MSG_TYPE_PING:        /* fall through */       \
    case RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY:                           \
        LOG_MSG(log_level,                                              \
                "CLI-%s %s %s:%u id=%lx err=%hd:%hd "fmt,               \
                (rcm)->rcrm_type == RAFT_CLIENT_RPC_MSG_TYPE_PING_REPLY ? \
                "PREPL" : "PING ",                                      \
                __uuid_str,                                             \
                inet_ntoa((from)->sin_addr), ntohs((from)->sin_port),   \
                (rcm)->rcrm_msg_id,                                     \
                (rcm)->rcrm_sys_error, (rcm)->rcrm_app_error,           \
                ##__VA_ARGS__);                                         \
        break;                                                          \
    default:                                                            \
        break;                                                          \
    }                                                                   \
}

#define DBG_RAFT_CLIENT_RPC_LEADER(log_level, ri, rcm, fmt, ...)        \
{                                                                       \
    if ((ri)->ri_csn_leader)                                            \
    {                                                                   \
        struct sockaddr_in dest;                                        \
        struct ctl_svc_node *csn = (ri)->ri_csn_leader;                 \
        udp_setup_sockaddr_in(ctl_svc_node_peer_2_ipaddr(csn),          \
                              ctl_svc_node_peer_2_client_port(csn),     \
                              &dest);                                   \
        DBG_RAFT_CLIENT_RPC(log_level, rcm, &dest, fmt, ##__VA_ARGS__); \
    }                                                                   \
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
        return "deny-boot-in-progress";
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
    COMPILE_TIME_ASSERT(RAFT_NET_MAX_RPC_SIZE >
                        (sizeof(struct raft_client_rpc_msg) +
                         sizeof(struct raft_client_rpc_raft_entry_data)));
}

struct raft_instance *
raft_net_get_instance(void);

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode);

int
raft_net_instance_shutdown(struct raft_instance *ri);

int
raft_net_server_instance_run(const char *raft_uuid_str,
                             const char *my_uuid_str,
                             raft_sm_request_handler_t sm_request_handler,
                             enum raft_instance_store_type type);

void
raft_net_instance_apply_callbacks(struct raft_instance *ri,
                                  raft_net_timer_cb_t timer_fd_cb,
                                  raft_net_udp_cb_t udp_client_recv_cb,
                                  raft_net_udp_cb_t udp_server_recv_cb);

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
raft_net_send_client_msg(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm);

void
raft_net_timerfd_settime(struct raft_instance *ri, unsigned long long msecs);


int
raft_net_evp_add(struct raft_instance *ri, epoll_mgr_cb_t cb);

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
    const struct sockaddr_in *from, const uuid_t client_uuid, uint64_t msg_id)
{
    if (rncr)
    {
        if (from)
            rncr->rncr_remote_addr = *from;

        // Caller may supply a null-uuid string
        uuid_copy(rncr->rncr_client_uuid, client_uuid);

        rncr->rncr_msg_id = msg_id;
    }
}

static inline bool
raft_net_client_request_handle_has_reply_info(
    const struct raft_net_client_request_handle *rncr)
{
    const struct sockaddr_in empty = {0};

    return (!rncr ||
            !memcmp(&rncr->rncr_remote_addr, &empty,
                    sizeof(struct sockaddr_in)) ||
            uuid_is_null(rncr->rncr_client_uuid) ||
            rncr->rncr_msg_id == ID_ANY_64bit ||
            rncr->rncr_msg_id == 0 ||
            rncr->rncr_reply_data_max_size == 0 ||
            rncr->rncr_reply == NULL) ? false : true;
}

// Raft Net State Machine Write Supplment API
void
raft_net_sm_write_supplement_init(struct raft_net_sm_write_supplements *rnsws);

void
raft_net_sm_write_supplement_destroy(
    struct raft_net_sm_write_supplements *rnsws);

int
raft_net_sm_write_supplement_add(
    struct raft_net_sm_write_supplements *rnsws, void *handle,
    void (*rnws_comp_cb)(void *),
    const char *key, const size_t key_size, const char *value,
    const size_t value_size);

// Raft Net User ID API
static inline void
raft_net_client_user_id_init(struct raft_net_client_user_id *rncui)
{
    if (rncui)
        memset(rncui, 0, sizeof(*rncui));
}

#define RAFT_NET_CLIENT_USER_ID_2_UUID(rncui, version, index)           \
    (rncui)->rncui_key.v ## version .rncui_v ## version ## _uuid[index]

#define RAFT_NET_CLIENT_USER_ID_2_UINT64(rncui, version, index)           \
    (rncui)->rncui_key.v ## version .rncui_v ## version ## _uint64[index]


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

#endif

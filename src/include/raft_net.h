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

#define RAFT_NET_BINARY_HIST_SIZE 20

#define RAFT_NET_PEER_RECENCY_NO_RECV -1ULL
#define RAFT_NET_PEER_RECENCY_NO_SEND -2ULL

typedef void raft_net_cb_ctx_t;
typedef int  raft_net_cb_ctx_int_t;
typedef bool raft_net_cb_ctx_bool_t;
typedef void raft_net_timerfd_cb_ctx_t;
typedef int  raft_net_timerfd_cb_ctx_int_t;

struct raft_client_rpc_msg;
struct raft_net_client_request;

enum raft_net_client_request_type
{
    RAFT_NET_CLIENT_REQ_TYPE_NONE,
    RAFT_NET_CLIENT_REQ_TYPE_READ,   // read previously committed data
    RAFT_NET_CLIENT_REQ_TYPE_WRITE,  // stage a new write
    RAFT_NET_CLIENT_REQ_TYPE_COMMIT, // complete a pending write
};

typedef raft_net_cb_ctx_t
    (*raft_net_cb_t)(struct raft_instance *,
                         const char *, ssize_t,
                         const struct sockaddr_in *);

typedef raft_net_timerfd_cb_ctx_t
    (*raft_net_timer_cb_t)(struct raft_instance *);

// State machine request handler - reads, writes, and commits
typedef raft_net_cb_ctx_int_t
    (*raft_sm_request_handler_t)(struct raft_net_client_request *);

#define RAFT_NET_MAX_RPC_SIZE 65000
#define RAFT_NET_MAX_TCP_RPC_SIZE 1024*1000
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
};

enum raft_net_comm_recency_type
{
    RAFT_COMM_RECENCY_RECV,
    RAFT_COMM_RECENCY_SEND,
    RAFT_COMM_RECENCY_UNACKED_SEND,
};

enum raft_net_connection_status
{
    RNCS_NEEDS_SETUP,
    RNCS_DISCONNECTED,
    RNCS_CONNECTING,
    RNCS_CONNECTED,
};

// XXX redo as tcp_connection with tc_data instead of rnc_ri?
struct raft_net_connection
{
    enum raft_net_connection_status rnc_status;
    struct raft_instance           *rnc_ri;
    struct tcp_socket_handle        rnc_tsh;
    struct epoll_handle             rnc_eph;
};

struct raft_net_tcp_handshake
{
    uuid_t  rnth_remote;
};

struct raft_client_rpc_msg
{
    uint32_t rcrm_type;  // enum raft_client_rpc_msg_type
    uint16_t rcrm_version;
    uint16_t rcrm_data_size;
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
    uint8_t  rcrm_pad[4];
    char     rcrm_data[];
};

struct raft_net_client_request
{
    enum raft_net_client_request_type rncr_type; // may be set by sm callback
    bool                              rncr_write_raft_entry;
    bool                              rncr_is_leader;
    struct net_ctl                    rncr_nc;
    int                               rncr_op_error;
    int64_t                           rncr_entry_term;
    int64_t                           rncr_current_term;
    long long                         rncr_commit_duration_msec;
    union
    {
        const struct raft_client_rpc_msg *rncr_request;
        const char                       *rncr_commit_data;
    };
    struct raft_client_rpc_msg       *rncr_reply;
    const size_t                      rncr_reply_data_max_size;
    struct sockaddr_in                rncr_remote_addr;
    uint64_t                          rncr_msg_id;
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
                "CLI-REQ   %s %s:%u id=%lx sz=%hu "fmt,                 \
                __uuid_str,                                             \
                inet_ntoa((from)->sin_addr), ntohs((from)->sin_port),   \
                (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,              \
                ##__VA_ARGS__);                                         \
        break;                                                          \
    case RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT:                             \
        uuid_unparse((rcm)->rcrm_redirect_id, __redir_uuid_str);        \
        LOG_MSG(log_level,                                              \
                "CLI-RDIR  %s %s:%u id=%lx sz=%hu redir-to=%s "fmt,     \
                __uuid_str,                                             \
                inet_ntoa((from)->sin_addr), ntohs((from)->sin_port),   \
                (rcm)->rcrm_msg_id, (rcm)->rcrm_data_size,              \
                __redir_uuid_str, ##__VA_ARGS__);                       \
        break;                                                          \
    case RAFT_CLIENT_RPC_MSG_TYPE_REPLY:                                \
        LOG_MSG(log_level,                                              \
                "CLI-REPL  %s %s:%u id=%lx sz=%hu err=%hd:%hd "fmt,     \
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

static inline bool
raft_net_sockaddr_is_valid(const struct sockaddr_in *sockaddr)
{
    struct sockaddr_in null_sockaddr = {0};

    return memcmp(&null_sockaddr, sockaddr, sizeof(struct sockaddr_in)) ?
        false : true;
}

int
raft_net_send_msg(struct raft_instance *ri, struct ctl_svc_node *csn,
                  struct iovec *iov,
                  const enum raft_udp_listen_sockets sock_src);

int
raft_net_send_client_msg(struct raft_instance *ri,
                         struct raft_client_rpc_msg *rcrm);

void
raft_net_timerfd_settime(struct raft_instance *ri, unsigned long long msecs);

int
raft_net_evp_add(struct raft_instance *ri, epoll_mgr_cb_t cb);

struct raft_net_connection *
raft_net_remote_connect(struct raft_instance *ri, struct ctl_svc_node *rp);
#endif

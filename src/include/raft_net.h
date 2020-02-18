/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_NET_H_
#define __NIOVA_RAFT_NET_H_ 1

#include "log.h"
#include "common.h"

#define RAFT_NET_SEQNO_ANY -1ULL

struct raft_instance;
struct epoll_handle;
struct ctl_svc_node;
struct sockaddr_in;

typedef void raft_net_udp_cb_ctx_t;
typedef void raft_net_timerfd_cb_ctx_t;

typedef raft_net_udp_cb_ctx_t
    (*raft_net_udp_cb_t)(struct raft_instance *,
                         const char *, ssize_t,
                         const struct sockaddr_in *);

typedef raft_net_timerfd_cb_ctx_t
    (*raft_net_timer_cb_t)(struct raft_instance *);

#define RAFT_NET_MAX_RPC_SIZE 65000

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
    RAFT_CLIENT_RPC_MSG_TYPE_INVALID  = 0,
    RAFT_CLIENT_RPC_MSG_TYPE_REQUEST  = 1,
    RAFT_CLIENT_RPC_MSG_TYPE_REPLY    = 2,
    RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT = 3,
    RAFT_CLIENT_RPC_MSG_TYPE_ANY      = 4,
};

struct raft_client_rpc_generic_msg
{
    uuid_t   rcrgm_redirect_id; // redirect ID (reply) if server not leader
    uint64_t rcrgm_msg_seqno; // generic, non-persistent RPC sequence number
    uint64_t rcrgm_msg_commit_seqno; // client app seqno committed in raft
    uint16_t rcrgm_msg_size;
    uint16_t rcrgm_msg_type;
    uint16_t rcrgm_error;
    uint16_t rcrgm__pad;
    char     rcrgm_data[];
};

struct raft_client_rpc_msg
{
    uint32_t rcrm_type;
    uint16_t rcrm_version;
    uint16_t rcrm__pad;
    uuid_t   rcrm_raft_id;
    uuid_t   rcrm_sender_id;
    union
    {   // This union must be at the end of the structure
        struct raft_client_rpc_generic_msg rcrm_gmsg;
    };
};

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode);

int
raft_net_instance_shutdown(struct raft_instance *ri);

int
raft_net_server_instance_run(const char *raft_uuid_str,
                             const char *my_uuid_str);

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

void
raft_net_update_last_comm_time(struct raft_instance *ri,
                               const uuid_t peer_uuid, bool send_or_recv);

raft_peer_t
raft_net_get_most_recently_responsive_server(struct raft_instance *ri);

#endif

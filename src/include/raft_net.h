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
    RAFT_CLIENT_RPC_MSG_TYPE_ANY      = 5,
};

/**
 * -- struct raft_client_rpc_generic_msg --
 * Raft client generic RPC message.
 * @rcrgm_redirect_id:  Used in reply context to return the raft leader UUID.
       Valid when rcrgm_error is RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT.
 * @rcrgm_msg_id:  Unique RPC identifier which must be unique across client
 *     process instances.
 * @rcrgm_msg_commit_seqno: In request context, informs the server that the
 *     client 'knows' the next value in the monotonic sequence.  The server
 *     verifies the value and returns EILSEQ if the value violates the
 *     sequence.  When EILSEQ is returned, the server will also set
 *     rcrgm_msg_commit_seqno with the last raft-committed sequence number.
 * @rcrgm_msg_size:  Size of the data appended to rcrgm_data.
 * @rcrgm_error:  Error value sent in reply context.
 * @rcrgm_msg_type:  One of enum raft_client_rpc_msg_type.  Note that
 *     RAFT_CLIENT_RPC_MSG_TYPE_REDIRECT is a special reply indicating that the
 *     server was not the raft leader.
 */
struct raft_client_rpc_generic_msg
{
    uuid_t   rcrgm_redirect_id;
    uint64_t rcrgm_msg_id;
    uint64_t rcrgm_msg_commit_seqno;
    uint16_t rcrgm_msg_size;
    uint16_t rcrgm_error;
    uint8_t  rcrgm_msg_type;
    uint8_t  rcrgm__pad[3];
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
raft_net_get_most_recently_responsive_server(const struct raft_instance *ri);

int
raft_net_get_comm_recency_value(const struct raft_instance *ri,
                                raft_peer_t raft_peer_idx,
                                unsigned long long *recency_ms);

#endif

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef __NIOVA_RAFT_NET_H_
#define __NIOVA_RAFT_NET_H_ 1

#include "common.h"

typedef void raft_net_udp_cb_ctx_t;

enum raft_client_rpc_msg_type
{
    RAFT_CLIENT_RPC_MSG_TYPE_INVALID = 0,
    RAFT_CLIENT_RPC_MSG_TYPE_REQUEST = 1,
    RAFT_CLIENT_RPC_MSG_TYPE_REPLY   = 2,
    RAFT_CLIENT_RPC_MSG_TYPE_ANY     = 3,
};

struct raft_client_rpc_generic_msg
{
    uuid_t   rcrgm_id; //sender ID (request) or redirect ID (reply)
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
    union
    {   // This union must be at the end of the structure
        struct raft_client_rpc_generic_msg rcrm_gmsg;
    };
};

struct raft_instance;

int
raft_net_instance_startup(struct raft_instance *ri, bool client_mode);

int
raft_net_instance_shutdown(struct raft_instance *ri);

int
raft_net_server_instance_run(const char *raft_uuid_str,
                             const char *my_uuid_str);

struct epoll_handle;

void
raft_net_instance_apply_callbacks(struct raft_instance *ri,
                                  void (*udp_recv_cb)(const struct
                                                      epoll_handle *),
                                  void (*timer_fd_cb)(const struct
                                                      epoll_handle *));
#endif

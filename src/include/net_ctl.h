/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef _NET_CTL_SVC_H_
#define _NET_CTL_SVC_H_ 1

#include "common.h"

enum nctl_ops
{
    NET_CTL_ENABLE_SEND,
    NET_CTL_ENABLE_RECV,
    NET_CTL_ENABLE_SEND_AND_RECV,
    NET_CTL_DISABLE_SEND,
    NET_CTL_DISABLE_RECV,
    NET_CTL_DISABLE_SEND_AND_RECV,
    NET_CTL_ENABLE_SEND_DISABLE_RECV,
    NET_CTL_ENABLE_RECV_DISABLE_SEND,
};

struct net_ctl
{
    union
    {
        uint16_t net_ctl_disable_recv:1,
                 net_ctl_disable_send:1,
                 net_ctl_recv_drop_percent:7,
                 net_ctl_send_drop_percent:7;
        uint16_t net_ctl_uint16;
    };
} PACKED;

static inline void
net_ctl_init(struct net_ctl *nc)
{
    nc->net_ctl_uint16 = 0;
}

static inline void
net_ctl_send_recv(struct net_ctl *nc, enum nctl_ops op)
{
    if (!nc)
        return;

    switch (op)
    {
    case NET_CTL_ENABLE_SEND:
        nc->net_ctl_disable_send = 0;
        break;
    case NET_CTL_ENABLE_RECV:
        nc->net_ctl_disable_recv = 0;
        break;
    case NET_CTL_ENABLE_SEND_AND_RECV:
        nc->net_ctl_disable_send = 0;
        nc->net_ctl_disable_recv = 0;
        break;
    case NET_CTL_DISABLE_SEND:
        nc->net_ctl_disable_send = 1;
        break;
    case NET_CTL_DISABLE_RECV:
        nc->net_ctl_disable_recv = 1;
        break;
    case NET_CTL_DISABLE_SEND_AND_RECV:
        nc->net_ctl_disable_send = 1;
        nc->net_ctl_disable_recv = 1;
        break;
    case NET_CTL_ENABLE_SEND_DISABLE_RECV:
        nc->net_ctl_disable_send = 0;
        nc->net_ctl_disable_recv = 1;
        break;
    case NET_CTL_ENABLE_RECV_DISABLE_SEND:
        nc->net_ctl_disable_send = 1;
        nc->net_ctl_disable_recv = 0;
        break;
    }
}

static inline bool
net_ctl_can_recv(const struct net_ctl *nc)
{
    return nc ? (nc->net_ctl_disable_recv ? false : true) : true;
}

static inline bool
net_ctl_can_send(const struct net_ctl *nc)
{
    return nc ? (nc->net_ctl_disable_send ? false : true) : true;
}

#endif

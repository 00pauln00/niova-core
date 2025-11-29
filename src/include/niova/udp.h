/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef _UDP_H
#define _UDP_H 1

#include <sys/types.h>          /* See NOTES */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <arpa/inet.h>

#include "niova/common.h"

#define NIOVA_MIN_UDP_PORT     1
#define NIOVA_MAX_UDP_PORT     65535
#define NIOVA_DEFAULT_UDP_PORT 6667
#define NIOVA_MAX_UDP_SIZE     65500

struct udp_socket_handle
{
    int  ush_socket;
    int  ush_port;
    char ush_ipaddr[IPV4_STRLEN];
};

static inline void
udp_socket_handle_init(struct udp_socket_handle *ush)
{
    if (ush)
    {
        ush->ush_socket = -1;
        ush->ush_port = -1;
        memset(ush->ush_ipaddr, 0, IPV4_STRLEN);
    }
}

static inline int
udp_socket_handle_2_sockfd(const struct udp_socket_handle *ush)
{
    return ush ? ush->ush_socket : -EINVAL;
}

struct niova_env_var;

void
udp_env_set_default_port(const struct niova_env_var *nev);

int
udp_get_default_port(void);

int
udp_setup_sockaddr_in(const char *ipaddr, int port,
                      struct sockaddr_in *addr_in);

int
udp_socket_bind(struct udp_socket_handle *ush);

int
udp_socket_setup(struct udp_socket_handle *ush);

int
udp_socket_close(struct udp_socket_handle *ush);

ssize_t
udp_socket_recv(const struct udp_socket_handle *ush, struct iovec *iov,
                size_t iovlen, struct sockaddr_in *from, bool block);

ssize_t
udp_socket_recv_fd(int fd, struct iovec *iov, size_t iovlen,
                   struct sockaddr_in *from, bool block);

ssize_t
udp_socket_send(const struct udp_socket_handle *ush, const struct iovec *iov,
                const size_t iovlen, const struct sockaddr_in *to);

size_t
udp_get_max_size();
#endif

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Kit Westneat <kit@niova.io> 2020
 */

#ifndef _TCP_H
#define _TCP_H 1

#include <sys/types.h>          /* See NOTES */
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "common.h"

#define NIOVA_MIN_TCP_PORT     1
#define NIOVA_MAX_TCP_PORT     65535
#define NIOVA_DEFAULT_TCP_PORT 6667
#define NIOVA_MAX_TCP_SIZE     1024*1024

#define NIOVA_TCP_LISTEN_DEPTH 16

struct tcp_socket_handle
{
    int  tsh_socket;
    int  tsh_port;
    char tsh_ipaddr[IPV4_STRLEN];
};

static inline void
tcp_socket_handle_init(struct tcp_socket_handle *tsh)
{
    if (tsh)
    {
        tsh->tsh_socket = -1;
        tsh->tsh_port = -1;
        memset(tsh->tsh_ipaddr, 0, IPV4_STRLEN);
    }
}

static inline int
tcp_socket_handle_2_sockfd(const struct tcp_socket_handle *tsh)
{
    return tsh ? tsh->tsh_socket : -EINVAL;
}

void
tcp_sockaddr_in_2_handle(struct sockaddr_in *addr_in, struct tcp_socket_handle *tsh);

struct niova_env_var;

void
tcp_env_set_default_port(const struct niova_env_var *nev);

int
tcp_get_default_port(void);

int
tcp_setup_sockaddr_in(const char *ipaddr, int port,
                      struct sockaddr_in *addr_in);

int
tcp_socket_bind(struct tcp_socket_handle *tsh);

int
tcp_socket_setup(struct tcp_socket_handle *tsh);

int
tcp_socket_close(struct tcp_socket_handle *tsh);

ssize_t
tcp_socket_recv(const struct tcp_socket_handle *tsh, struct iovec *iov,
                size_t iovlen, struct sockaddr_in *from, bool block);

ssize_t
tcp_socket_recv_fd(int fd, struct iovec *iov, size_t iovlen,
                   struct sockaddr_in *from, bool block);

ssize_t
tcp_socket_send(const struct tcp_socket_handle *tsh, const struct iovec *iov,
                const size_t iovlen);

int
tcp_socket_handle_accept(int fd, struct tcp_socket_handle *tsh);

int
tcp_socket_connect(struct tcp_socket_handle *tsh);

ssize_t
tcp_get_max_size();
#endif

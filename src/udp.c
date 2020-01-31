/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */
#include "log.h"
#include "env.h"
#include "udp.h"

static int udpDefaultPort = NIOVA_DEFAULT_UDP_PORT;

int
udp_get_default_port(void)
{
    return udpDefaultPort;
}

static bool
udp_iov_size_ok(const struct iovec *iovs, const size_t iovlen)
{
    if (iovs && iovlen > 0)
    {
        size_t total_sz = 0;

        for (size_t i = 0; i < iovlen; i++)
        {
            total_sz += iovs[i].iov_len;

            if (total_sz > NIOVA_MAX_UDP_SIZE)
                return false;
        }
    }

    return true;
}

int
udp_setup_sockaddr_in(const char *ipaddr, int port,
                      struct sockaddr_in *addr_in)
{
    if (port <= 0)
        port = udpDefaultPort;

    addr_in->sin_family = AF_INET;
    addr_in->sin_port = htons(port);

    int rc = inet_aton(ipaddr, &addr_in->sin_addr);

    return !rc ? -EINVAL : 0;
}

void
udp_env_set_default_port(const struct niova_env_var *nev)
{
    if (nev && nev->nev_long_value >= NIOVA_MIN_UDP_PORT &&
        nev->nev_long_value <= NIOVA_MAX_UDP_PORT)
        udpDefaultPort = nev->nev_long_value;
}

/**
 * udp_socket_close - close the socket inside the handle.
 */
int
udp_socket_close(struct udp_socket_handle *ush)
{
    if (!ush)
        return -EINVAL;

    int socket = ush->ush_socket;
    ush->ush_socket = -1;

    return (socket >= 0) ? close(socket) : 0;
}

int
udp_socket_bind(struct udp_socket_handle *ush)
{
    if (!ush || ush->ush_socket < 0)
        return -EINVAL;

    if (ush->ush_port <= 0)
        ush->ush_port = udpDefaultPort;

    struct sockaddr_in addr_in = {0};

    int rc = udp_setup_sockaddr_in(ush->ush_ipaddr, ush->ush_port, &addr_in);
    if (!rc)
    {
        rc = bind(ush->ush_socket, (struct sockaddr *)&addr_in,
                  sizeof(addr_in));
        if (rc)
        {
            rc = -errno;
            SIMPLE_LOG_MSG(LL_ERROR, "bind(): %s", strerror(-rc));
        }
    }

    if (rc)
        udp_socket_close(ush);

    return rc;
}

/**
 * udp_socket_setup - initial stage for UDP socket configuration.  This call
 *    does not bind() the socket so it's only a partial setup.
 */
int
udp_socket_setup(struct udp_socket_handle *ush)
{
    if (!ush)
        return -EINVAL;

    int rc = 0;

    ush->ush_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (ush->ush_socket < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "socket(): %s", strerror(-rc));

        udp_socket_close(ush);
    }

    return rc;
}

ssize_t
udp_socket_recv(const struct udp_socket_handle *ush, struct iovec *iov,
                const size_t iovlen, struct sockaddr_in *from, bool block)
{
    if (!ush || !iov || !iovlen)
        return -EINVAL;

    else if (!udp_iov_size_ok(iov, iovlen))
        return -EMSGSIZE;

    int socket = ush->ush_socket;

    struct sockaddr_in addr_in = {0};

    struct msghdr msg = {
        .msg_name = &addr_in,
        .msg_namelen = sizeof(addr_in),
        .msg_iov = iov,
        .msg_iovlen = iovlen,
        .msg_flags = 0,
    };

    ssize_t rc = recvmsg(socket, &msg, block ? 0 : MSG_DONTWAIT);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(((rc == -EINTR ||
                         (!block && (rc == -EAGAIN || rc == -EWOULDBLOCK))) ?
                        LL_DEBUG : LL_WARN),
                       "recvmsg(): %s", strerror(-rc));

        return rc;
    }

    if (from)
        *from = addr_in;

    SIMPLE_LOG_MSG(LL_NOTIFY, "src=%s:%u nb=%zd flags=%x",
                   inet_ntoa(addr_in.sin_addr), ntohs(addr_in.sin_port), rc,
                   msg.msg_flags);

    if (msg.msg_flags & MSG_TRUNC)
    {
        /* Partial msg recv.
         */
        SIMPLE_LOG_MSG(LL_WARN, "recvmsg(): %zd (MSG_TRUNC)", rc);

        return -EBADMSG;
    }

    return rc;
}

ssize_t
udp_socket_send(const struct udp_socket_handle *ush, struct iovec *iov,
                const size_t iovlen, struct sockaddr_in *to)
{
    if (!ush || !iov || !iovlen || !to)
        return -EINVAL;

    else if (!udp_iov_size_ok(iov, iovlen))
        return -EMSGSIZE;

    struct msghdr msg = {
        .msg_name = to,
        .msg_namelen = sizeof(*to),
        .msg_iov = iov,
        .msg_iovlen = iovlen,
    };

    ssize_t rc = sendmsg(ush->ush_socket, &msg, 0);
    if (rc < 0)
    {
        rc = -errno;

        SIMPLE_LOG_MSG(LL_WARN, "sendmsg() %s:%u: %s",
                       inet_ntoa(to->sin_addr), ntohs(to->sin_port),
                       strerror(-rc));
    }
    else
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "%s:%u nb=%zd",
                       inet_ntoa(to->sin_addr), ntohs(to->sin_port), rc);
    }

    return rc;
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <fcntl.h>

#include "log.h"
#include "env.h"
#include "udp.h"
#include "io.h"

REGISTRY_ENTRY_FILE_GENERATE;

static int udpDefaultPort = NIOVA_DEFAULT_UDP_PORT;
static ssize_t maxUdpSize = NIOVA_MAX_UDP_SIZE;

int
udp_get_default_port(void)
{
    return udpDefaultPort;
}

ssize_t
udp_get_max_size()
{
    return maxUdpSize;
}

void
udp_set_max_size(ssize_t new_size)
{
    maxUdpSize = new_size;
}

static bool
udp_iov_size_ok(const size_t total_len)
{
    return total_len > NIOVA_MAX_UDP_SIZE ? false : true;
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

/**
 * udp_socket_bind - called at some point after udp_socket_setup() to complete
 *    the setup of the UDP socket.
 */
int
udp_socket_bind(struct udp_socket_handle *ush)
{
    if (!ush || ush->ush_socket < 0)
        return -EINVAL;

    if (ush->ush_port <= 0)
        ush->ush_port = udpDefaultPort;

    struct sockaddr_in addr_in = {0};

    SIMPLE_LOG_MSG(LL_TRACE, "udp_socket_bind(): %s:%d",
                   ush->ush_ipaddr, ush->ush_port);

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
 * udp_socket_setup - initial stage for UDP socket configuration which creates
 *    a UDP socket and makes it non-blocking.  This call does not bind() the
 *    socket so it's only a partial setup.  To complete the setup,
 *    udp_socket_bind() must be called after the this function.
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
                size_t iovlen, struct sockaddr_in *from, bool block)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!ush || !iov || !iovlen)
        return -EINVAL;
    else if (iovlen > IO_MAX_IOVS)
        return -E2BIG;

    size_t total_size = niova_io_iovs_total_size_get(iov, iovlen);
    if (!total_size || !udp_iov_size_ok(total_size))
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

    SIMPLE_LOG_MSG(LL_DEBUG, "src=%s:%u nb=%zd flags=%x",
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
udp_socket_recv_fd(int fd, struct iovec *iov, size_t iovlen,
                   struct sockaddr_in *from, bool block)
{
    struct udp_socket_handle ush = {
        .ush_socket = fd,
        .ush_port = 0,
        .ush_ipaddr = "0.0.0.0",
    };

    return udp_socket_recv(&ush, iov, iovlen, from, block);
}

/**
 * udp_socket_send - send the iov contents to the location specified by the
 *    'to' parameter.  udp_socket_send() aims to deliver the entire payload
 *    specified in 'iovs' and will not perform a partial delivery unless
 *    there is some error returned by sendmsg().
 */
ssize_t
udp_socket_send(const struct udp_socket_handle *ush, const struct iovec *iov,
                const size_t iovlen, const struct sockaddr_in *to)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!ush || !iov || !iovlen || !to)
        return -EINVAL;

    else if (iovlen > IO_MAX_IOVS)
        return -E2BIG;

    const ssize_t total_size = niova_io_iovs_total_size_get(iov, iovlen);
    if (!total_size || !udp_iov_size_ok(total_size))
        return -EMSGSIZE;

    /* sendmsg() should not perform partial sends but it's better to be safe
     * than sorry.
     */
    size_t total_sent = 0;
    ssize_t rc = 0;
    for (ssize_t sendmsg_rc = 0; total_sent < total_size;)
    {
        struct iovec my_iovs[iovlen];
        const size_t my_iovlen =
            niova_io_iovs_map_consumed(iov, my_iovs, iovlen, total_sent);
        struct msghdr msg = {
            .msg_name = (void *)to,
            .msg_namelen = sizeof(*to),
            .msg_iov = my_iovs,
            .msg_iovlen = my_iovlen,
        };

        sendmsg_rc = sendmsg(ush->ush_socket, &msg, 0);
        if (sendmsg_rc < 0)
        {
            if (errno == EINTR) // retry the send.
                continue;

            rc = -errno;

            LOG_MSG(LL_NOTIFY, "sendmsg() %s:%u: %s",
                    inet_ntoa(to->sin_addr), ntohs(to->sin_port),
                    strerror(-rc));
            break;
        }

        total_sent += sendmsg_rc;

        SIMPLE_LOG_MSG(LL_DEBUG, "%s:%u rc=%zd total=(%zd:%zd)",
                       inet_ntoa(to->sin_addr), ntohs(to->sin_port),
                       sendmsg_rc, total_sent, total_size);
    }

    if (!rc && total_size != total_sent)
        LOG_MSG(LL_NOTIFY, "incomplete send to %s:%u (%zd:%zd)",
                inet_ntoa(to->sin_addr), ntohs(to->sin_port),
                total_sent, total_size);

    return rc ? rc : total_sent;
}

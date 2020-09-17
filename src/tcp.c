/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */
#include <fcntl.h>
#include "io.h"
#include "tcp.h"
#include "log.h"

REGISTRY_ENTRY_FILE_GENERATE;

static ssize_t maxTcpSize = NIOVA_MAX_TCP_SIZE;

static int tcpDefaultPort = NIOVA_DEFAULT_TCP_PORT;

int
tcp_get_default_port(void)
{
    return tcpDefaultPort;
}


ssize_t
tcp_get_max_size()
{
    return maxTcpSize;
}

void
tcp_set_max_size(ssize_t new_size)
{
    maxTcpSize = new_size;
}


bool
tcp_iov_size_ok(ssize_t sz)
{
    return sz <= NIOVA_MAX_TCP_SIZE;
}

int
tcp_setup_sockaddr_in(const char *ipaddr, int port,
                      struct sockaddr_in *addr_in)
{
    if (port <= 0)
        port = tcpDefaultPort;

    addr_in->sin_family = AF_INET;
    addr_in->sin_port = htons(port);

    int rc = inet_aton(ipaddr, &addr_in->sin_addr);

    return !rc ? -EINVAL : 0;
}

void
tcp_sockaddr_in_2_handle(struct sockaddr_in *addr_in, struct tcp_socket_handle *tsh)
{
    strncpy(tsh->tsh_ipaddr, inet_ntoa(addr_in->sin_addr), sizeof(tsh->tsh_ipaddr));
    tsh->tsh_port = addr_in->sin_port;
}

int
tcp_socket_close(struct tcp_socket_handle *tsh)
{
    if (!tsh)
        return -EINVAL;

    int socket = tsh->tsh_socket;
    tsh->tsh_socket = -1;

    return (socket >= 0) ? close(socket) : 0;
}

int
tcp_socket_setup(struct tcp_socket_handle *tsh)
{
    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_setup()");

    if (!tsh)
        return -EINVAL;

    int rc = 0;

    tsh->tsh_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (tsh->tsh_socket < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "socket(): %s", strerror(-rc));

        tcp_socket_close(tsh);
    }
    return rc;
}

static int
tcp_socket_set_nonblocking(struct tcp_socket_handle *tsh)
{
    NIOVA_ASSERT(tsh && tsh->tsh_socket > 0);

    return fcntl(tsh->tsh_socket, F_SETFL, O_NONBLOCK);
}

/**
 * tcp_socket_bind - called at some point after tcp_socket_setup() to complete
 *    the setup of the UDP socket.
 */
int
tcp_socket_bind(struct tcp_socket_handle *tsh)
{
    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_bind()");
    if (!tsh || tsh->tsh_socket < 0)
        return -EINVAL;

    if (tsh->tsh_port <= 0)
        tsh->tsh_port = tcpDefaultPort;

    int sock_opt = 1;
    setsockopt(tsh->tsh_socket, SOL_SOCKET, SO_REUSEADDR, (char*)&sock_opt, sizeof(sock_opt));

    struct sockaddr_in addr_in = {0};

    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_bind(): %s:%d", tsh->tsh_ipaddr, tsh->tsh_port);

    int rc = tcp_setup_sockaddr_in(tsh->tsh_ipaddr, tsh->tsh_port, &addr_in);
    if (rc)
        goto out;

    rc = bind(tsh->tsh_socket, (struct sockaddr *)&addr_in,
                sizeof(addr_in));
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "bind(): %s", strerror(-rc));
        goto out;
    }

    tcp_socket_set_nonblocking(tsh);

    rc = listen(tsh->tsh_socket, NIOVA_TCP_LISTEN_DEPTH);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "listen(): %s", strerror(-rc));
        goto out;
    }

out:
    if (rc)
        tcp_socket_close(tsh);

    return rc;
}

int
tcp_socket_handle_accept(int fd, struct tcp_socket_handle *tsh)
{
    struct sockaddr_in addr_in;
    unsigned int addr_size = sizeof(struct sockaddr_in);

    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_handle_accept()");

    int rc = accept(fd, (struct sockaddr *)&addr_in, &addr_size);
    if (rc < 0) {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "accept(): %s", strerror(-rc));

        return rc;
    }

    tsh->tsh_socket = rc;
    tcp_sockaddr_in_2_handle(&addr_in, tsh);

    return 0;
};

ssize_t
tcp_socket_recv_fd(int fd, struct iovec *iov, size_t iovlen,
                   struct sockaddr_in *from, bool block)
{
    struct tcp_socket_handle tsh = {
        .tsh_socket = fd,
        .tsh_port = 0,
        .tsh_ipaddr = "0.0.0.0",
    };

    return tcp_socket_recv(&tsh, iov, iovlen, from, block);
}

ssize_t
tcp_socket_recv(const struct tcp_socket_handle *tsh, struct iovec *iov,
                size_t iovlen, struct sockaddr_in *from, bool block)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!tsh || !iov || !iovlen)
        return -EINVAL;
    else if (iovlen > IO_MAX_IOVS)
        return -E2BIG;

    size_t total_size = io_iovs_total_size_get(iov, iovlen);
    if (!total_size || !tcp_iov_size_ok(total_size))
        return -EMSGSIZE;

    int socket = tsh->tsh_socket;

    struct msghdr msg = {
        .msg_name = NULL,
        .msg_namelen = 0,
        .msg_iov = iov,
        .msg_iovlen = iovlen,
        .msg_flags = MSG_WAITALL,
    };

    ssize_t rc = recvmsg(socket, &msg, block ? MSG_WAITALL : MSG_DONTWAIT);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(((rc == -EINTR ||
                         (!block && (rc == -EAGAIN || rc == -EWOULDBLOCK))) ?
                        LL_DEBUG : LL_WARN),
                       "recvmsg(): %s", strerror(-rc));
        return rc;
    }

    if (from) {
        tcp_setup_sockaddr_in(tsh->tsh_ipaddr, tsh->tsh_port, from);
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "fd=%d src=%s:%u nb=%zd flags=%x",
                   socket, tsh->tsh_ipaddr, tsh->tsh_port, rc,
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
tcp_socket_send(const struct tcp_socket_handle *tsh, const struct iovec *iov,
                const size_t iovlen)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!tsh || !iov || !iovlen)
        return -EINVAL;

    else if (iovlen > IO_MAX_IOVS)
        return -E2BIG;

    const ssize_t total_size = io_iovs_total_size_get(iov, iovlen);
    if (!total_size || !tcp_iov_size_ok(total_size))
        return -EMSGSIZE;

    /* sendmsg() should not perform partial sends but it's better to be safe
     * than sorry.
     */
    ssize_t total_sent = 0;
    ssize_t rc = 0;
    for (ssize_t sendmsg_rc = 0; total_sent < total_size;)
    {
        struct iovec my_iovs[iovlen];
        const size_t my_iovlen = io_iovs_map_consumed(iov, my_iovs, iovlen,
                                                      total_sent);
        struct msghdr msg = {
            .msg_name = 0,
            .msg_namelen = 0,
            .msg_iov = my_iovs,
            .msg_iovlen = my_iovlen,
        };

        sendmsg_rc = sendmsg(tsh->tsh_socket, &msg, 0);
        if (sendmsg_rc < 0)
        {
            if (errno == EINTR) // retry the send.
                continue;

            rc = -errno;

            LOG_MSG(LL_NOTIFY, "sendmsg() %s:%u: %s",
                    tsh->tsh_ipaddr, tsh->tsh_port,
                    strerror(-rc));
            break;
        }

        total_sent += sendmsg_rc;

        SIMPLE_LOG_MSG(LL_DEBUG, "sendmsg() %s:%u rc=%zd total=(%zd:%zd)",
                       tsh->tsh_ipaddr, tsh->tsh_port,
                       sendmsg_rc, total_sent, total_size);
    }

    if (!rc && total_size != total_sent)
        LOG_MSG(LL_NOTIFY, "incomplete send to %s:%u (%zd:%zd)",
                tsh->tsh_ipaddr, tsh->tsh_port,
                total_sent, total_size);

    return rc ? rc : total_sent;
}


int
tcp_socket_connect(struct tcp_socket_handle *tsh)
{
    if (!tsh || tsh->tsh_socket < 0)
        return -EINVAL;

    struct sockaddr_in addr_in = {0};
    tcp_setup_sockaddr_in(tsh->tsh_ipaddr, tsh->tsh_port, &addr_in);

    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_connect() fd:%d host: %s:%d", tsh->tsh_socket, tsh->tsh_ipaddr, tsh->tsh_port);

    tcp_socket_set_nonblocking(tsh);

    int rc = connect(tsh->tsh_socket, (struct sockaddr *)&addr_in, sizeof(struct sockaddr_in));
    if (rc < 0) {
        rc = -errno;
        if (rc != -EINPROGRESS)
            SIMPLE_LOG_MSG(LL_NOTIFY, "connect():%d", rc);

        return rc;
    }

    return 0;
}

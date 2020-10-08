#include <sys/ioctl.h>

#include "alloc.h"
#include "log.h"
#include "epoll_mgr.h"
#include "tcp.h"
#include "tcp_mgr.h"
#include "util.h"

#define DEFAULT_BULK_BUFS 32

REGISTRY_ENTRY_FILE_GENERATE;

// XXX should this be on tcp_mgr_instance?
static uint32_t bufsAvail = DEFAULT_BULK_BUFS;

void
set_bulk_bufs_avail(uint32_t cnt)
{
    bufsAvail = cnt;
}

// XXX not particularly thread safe
void *
tcp_mgr_bulk_malloc(size_t sz)
{
    if (bufsAvail <= 0)
        return NULL;

    void *buf = niova_malloc_can_fail(sz);
    if (!buf)
        return NULL;

    bufsAvail--;
    return buf;
}

void
tcp_mgr_bulk_free(void *buf)
{
    niova_free(buf);
    bufsAvail++;
}

static void
tcp_mgr_connection_put(struct tcp_mgr_connection *tmc)
{
    if (tmc->tmc_tmi && tmc->tmc_tmi->tmi_connection_ref_cb)
        tmc->tmc_tmi->tmi_connection_ref_cb(tmc, EPH_REF_PUT);
}

void
tcp_mgr_setup(struct tcp_mgr_instance *tmi, void *data,
              tcp_mgr_ref_cb_t connection_ref_cb,
              tcp_mgr_recv_cb_t recv_cb,
              tcp_mgr_bulk_size_cb_t bulk_size_cb,
              tcp_mgr_handshake_cb_t handshake_cb,
              tcp_mgr_handshake_fill_t handshake_fill,
              size_t handshake_size)
{
    NIOVA_ASSERT(tmi);

    tmi->tmi_data = data;
    tmi->tmi_connection_ref_cb = connection_ref_cb;
    tmi->tmi_recv_cb = recv_cb;
    tmi->tmi_bulk_size_cb = bulk_size_cb;
    tmi->tmi_handshake_cb = handshake_cb;
    tmi->tmi_handshake_fill = handshake_fill;
    tmi->tmi_handshake_size = handshake_size;

    pthread_mutex_init(&tmi->tmi_status_mutex, NULL);
}

int
tcp_mgr_sockets_setup(struct tcp_mgr_instance *tmi, const char *ipaddr,
                      int port)
{
    strncpy(tmi->tmi_listen_socket.tsh_ipaddr, ipaddr, IPV4_STRLEN);
    tmi->tmi_listen_socket.tsh_port = port;

    return tcp_socket_setup(&tmi->tmi_listen_socket);
}

int
tcp_mgr_sockets_close(struct tcp_mgr_instance *tmi)
{
    return tcp_socket_close(&tmi->tmi_listen_socket);
}

int
tcp_mgr_sockets_bind(struct tcp_mgr_instance *tmi)
{
    int rc = tcp_socket_bind(&tmi->tmi_listen_socket);
    if (rc)
        tcp_mgr_sockets_close(tmi);

    return rc;
}

void
tcp_mgr_connection_setup(struct tcp_mgr_instance *tmi,
                         struct tcp_mgr_connection *tmc,
                         size_t header_size)
{
    tcp_socket_handle_init(&tmc->tmc_tsh);
    tmc->tmc_eph.eph_installed = 0;

    tmc->tmc_tmi = tmi;
    tmc->tmc_header_size = header_size;

    tmc->tmc_status = TMCS_DISCONNECTED;
    tmc->tmc_async_buf = NULL;
    tmc->tmc_async_remain = 0;

    SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_mgr_connection_setup() - tmc %p", tmc);
}

void
tcp_mgr_connection_close(struct tcp_mgr_connection *tmc)
{
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "closing");

    if (tmc->tmc_status != TMCS_CONNECTING && tmc->tmc_status != TMCS_CONNECTED)
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "unexpected status: %d",
                        tmc->tmc_status);

    tmc->tmc_status = TMCS_DISCONNECTING;
    if (tmc->tmc_eph.eph_installed)
        epoll_handle_del(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);

    // XXX specifically we don't want this to happen when another thread is using
    if (tmc->tmc_async_buf)
        tcp_mgr_bulk_free(tmc->tmc_async_buf);

    tmc->tmc_async_buf = NULL;
    tmc->tmc_async_offset = 0;
    tmc->tmc_async_remain = 0;

    tcp_socket_close(&tmc->tmc_tsh);
    tmc->tmc_status = TMCS_DISCONNECTED;
}

static struct tcp_mgr_connection *
tcp_mgr_incoming_new(struct tcp_mgr_instance *tmi)
{
    struct tcp_mgr_connection *tmc =
        niova_malloc(sizeof(struct tcp_mgr_connection));
    if (!tmc)
        return NULL;

    tcp_mgr_connection_setup(tmi, tmc, 0);
    tmc->tmc_status = TMCS_CONNECTING;

    return tmc;
}

static void
tcp_mgr_incoming_fini_err(struct tcp_mgr_connection *tmc)
{
    tcp_mgr_connection_close(tmc);
    niova_free(tmc);
}

static void
tcp_mgr_incoming_fini(struct tcp_mgr_connection *tmc)
{
    niova_free(tmc);
}

static int
tcp_mgr_connection_epoll_add(struct tcp_mgr_connection *tmc,
                             uint32_t events,
                             epoll_mgr_cb_t cb,
                             void (*ref_cb)(void *, enum epoll_handle_ref_op))
{
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "events=%d", events);

    int rc = epoll_handle_init(&tmc->tmc_eph, tmc->tmc_tsh.tsh_socket,
                               events, cb, tmc, ref_cb);
    if (rc)
        return rc;

    return epoll_handle_add(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);
}

static int
tcp_mgr_handshake_iov_init(struct tcp_mgr_instance *tmi, struct iovec *iov)
{
    iov->iov_base = niova_malloc_can_fail(tmi->tmi_handshake_size);
    if (!iov->iov_base)
        return -ENOMEM;

    iov->iov_len = tmi->tmi_handshake_size;

    return 0;
}

static void
tcp_mgr_handshake_iov_fini(struct iovec *iov)
{
    NIOVA_ASSERT(iov && iov->iov_base);
    niova_free(iov->iov_base);
}

static int
epoll_handle_rc_get(const struct epoll_handle *eph, uint32_t events)
{
    int rc = 0;

    if (events & (EPOLLHUP | EPOLLERR))
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "received %s",
                       events & EPOLLHUP ? "HUP" : "ERR");
        socklen_t rc_len = sizeof(rc);
        int rc2 = getsockopt(eph->eph_fd, SOL_SOCKET, SO_ERROR, &rc, &rc_len);
        if (rc2)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "Error getting socket error: %d", rc2);
            return rc2;
        }
    }

    return -rc;
}

// should be called with tmi status mutex
static int
tcp_mgr_async_progress_recv(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!tmc->tmc_async_remain)
        return 0;

    void *bulk = tmc->tmc_async_buf + tmc->tmc_async_offset;

    struct iovec iov = {
        .iov_base = bulk,
        .iov_len = tmc->tmc_async_remain,
    };

    ssize_t recv_bytes = tcp_socket_recv(&tmc->tmc_tsh, &iov, 1, NULL, false);

    SIMPLE_LOG_MSG(LL_DEBUG, "recv_bytes=%ld iov_len=%ld", recv_bytes,
                   iov.iov_len);

    if (recv_bytes == -EAGAIN)
        recv_bytes = 0;
    else if (recv_bytes == 0)
        return -ENOTCONN;
    else if (recv_bytes < 0)
        return recv_bytes;

    NIOVA_ASSERT(recv_bytes <= tmc->tmc_async_remain);

    tmc->tmc_async_offset += recv_bytes;
    tmc->tmc_async_remain -= recv_bytes;

    return 0;
}

// should be called with tmi status lock
static int
tcp_mgr_async_prepare_and_recv(struct tcp_mgr_connection *tmc, size_t bulk_size,
                               void *hdr, size_t hdr_size)
{
    NIOVA_ASSERT(tmc && !tmc->tmc_async_buf && !tmc->tmc_async_remain);
    if (!bulk_size)
        return 0;

    size_t buf_size = hdr_size + bulk_size;

    // buffer free'd in tcp_mgr_recv_cb
    void *buf = tcp_mgr_bulk_malloc(buf_size);
    if (!buf)
    {
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "cannot allocate bulk buf");

        int bytes_avail;
        ioctl(tmc->tmc_tsh.tsh_socket, FIONREAD, &bytes_avail);
        if (bytes_avail == bulk_size)
            buf = niova_malloc_can_fail(buf_size);

        if (!buf)
            return -ENOMEM;
    }

    if (hdr && hdr_size)
        memcpy(buf, hdr, hdr_size);

    tmc->tmc_async_buf = buf;
    tmc->tmc_async_offset = hdr_size;
    tmc->tmc_async_remain = bulk_size;

    int rc = tcp_mgr_async_progress_recv(tmc);
    if (rc < 0)
        return rc;

    return 0;
}

// should be called with tmi status lock
static int
tcp_mgr_header_recv(struct tcp_mgr_connection *tmc)
{
    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;

    tcp_mgr_recv_cb_t recv_cb = tmi->tmi_recv_cb;
    tcp_mgr_bulk_size_cb_t bulk_size_cb = tmi->tmi_bulk_size_cb;
    size_t header_size = tmc->tmc_header_size;

    NIOVA_ASSERT(recv_cb && bulk_size_cb && header_size);

    static char sink_buf[TCP_MGR_MAX_HDR_SIZE];
    struct iovec iov;
    iov.iov_base = sink_buf;
    iov.iov_len = header_size;

    // try twice in case interrupts cause short read
    ssize_t rc = tcp_socket_recv_all(&tmc->tmc_tsh, &iov, NULL, 2);
    if (rc != header_size)
        return rc < 0 ? rc : -ECOMM;

    rc = bulk_size_cb(tmc, sink_buf, tmi->tmi_data);
    if (rc > 0)
        return tcp_mgr_async_prepare_and_recv(tmc, rc, sink_buf, header_size);

    recv_cb(tmc, sink_buf, header_size, tmi->tmi_data);
    return 0;
}

static void
tcp_mgr_recv_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_connection *tmc = eph->eph_arg;
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "received events: %d", events);

    int rc = epoll_handle_rc_get(eph, events);
    if (rc)
    {
        if (rc == -ECONNRESET ||
            rc == -ENOTCONN ||
            rc == -ECONNABORTED)
            tcp_mgr_connection_close(tmc);

        SIMPLE_LOG_MSG(LL_NOTIFY, "error received on socket fd=%d, rc=%d",
                       eph->eph_fd, rc);
        return;
    }

    // is this a new RPC?
    if (!tmc->tmc_async_remain)
    {
        int rc = tcp_mgr_header_recv(tmc);
        if (rc < 0)
        {
            SIMPLE_LOG_MSG(LL_NOTIFY, "cannot read RPC header, rc=%d", rc);
            goto out;
        }

        // simple (non-bulk) RPC, we're done
        if (!tmc->tmc_async_buf)
            goto out;
    }

    NIOVA_ASSERT(tmc->tmc_async_buf);

    rc = tcp_mgr_async_progress_recv(tmc);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "cannot complete async read, rc=%d", rc);
        goto out;
    }

    // bulk buffer completed recv
    if (!tmc->tmc_async_remain)
    {
        struct tcp_mgr_instance *tmi = tmc->tmc_tmi;
        NIOVA_ASSERT(tmi->tmi_recv_cb);

        tmi->tmi_recv_cb(tmc, tmc->tmc_async_buf, tmc->tmc_async_offset,
                         tmc->tmc_tmi->tmi_data);
        tcp_mgr_bulk_free(tmc->tmc_async_buf);

        tmc->tmc_async_buf = NULL;
        tmc->tmc_async_offset = 0;
    }

out:
    if (rc < 0)
        tcp_mgr_connection_close(tmc);
}

// should be called with tmi status lock
static int
tcp_mgr_connection_merge_incoming(struct tcp_mgr_connection *incoming,
                                  struct tcp_mgr_connection *owned)
{
    NIOVA_ASSERT(incoming && owned && incoming->tmc_status == TMCS_CONNECTING);

    struct tcp_mgr_instance *tmi = incoming->tmc_tmi;

    if (owned->tmc_status == TMCS_DISCONNECTING ||
        owned->tmc_status == TMCS_CONNECTING ||
        owned->tmc_status == TMCS_CONNECTED)
    {
        DBG_TCP_MGR_CXN(LL_NOTIFY, owned,
                        "incoming connection, but outgoing conn exists,"
                        " status: %d",
                        owned->tmc_status);

        // if peer is attempting to connect again, assume old connection is dead
        // XXX it would be better to send a ping, and handle failure there
        if (owned->tmc_status == TMCS_CONNECTED)
            tcp_mgr_connection_close(owned);

        if (tmi->tmi_connection_ref_cb)
            tmi->tmi_connection_ref_cb(owned, EPH_REF_PUT);
        owned = NULL;
    }

    if (!owned)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "handshake error");
        tcp_mgr_connection_put(owned);
        return -EINVAL;
    }

    owned->tmc_tsh.tsh_socket = incoming->tmc_tsh.tsh_socket;
    owned->tmc_status = TMCS_CONNECTED;

    if (incoming->tmc_eph.eph_installed)
        epoll_handle_del(tmi->tmi_epoll_mgr, &incoming->tmc_eph);

    tcp_mgr_connection_epoll_add(owned, EPOLLIN, tcp_mgr_recv_cb,
                                 tmi->tmi_connection_ref_cb);

    DBG_TCP_MGR_CXN(LL_NOTIFY, owned, "connection established");

    // epoll takes a ref, so we can give up ours
    tcp_mgr_connection_put(owned);
    return 0;
}

static void
tcp_mgr_handshake_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    NIOVA_ASSERT(eph && eph->eph_arg);
    struct tcp_mgr_connection *tmc = eph->eph_arg;
    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;

    struct iovec iov;
    tcp_mgr_handshake_iov_init(tmi, &iov);

    int rc = tcp_socket_recv(&tmc->tmc_tsh, &iov, 1, NULL, 1);
    if (rc <= 0)
    {
        if (rc == 0)
            SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_recv(): connection closed");
        else
            SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_recv(): %s", strerror(-rc));

        tcp_mgr_handshake_iov_fini(&iov);
        tcp_mgr_incoming_fini_err(tmc);

        return;
    }

    SIMPLE_LOG_MSG(LL_TRACE, "tcp_socket_recv(): rc=%d", rc);

    struct tcp_mgr_connection *new_tmc =
        tmi->tmi_handshake_cb(tmi->tmi_data,
                              tmc->tmc_tsh.tsh_socket,
                              iov.iov_base,
                              rc);

    tcp_mgr_handshake_iov_fini(&iov);

    if (new_tmc)
        rc = tcp_mgr_connection_merge_incoming(tmc, new_tmc);
    else
        rc = -EINVAL;

    if (rc)
        tcp_mgr_incoming_fini_err(tmc);
    else
        tcp_mgr_incoming_fini(tmc);
}

static int
tcp_mgr_accept(struct tcp_mgr_instance *tmi, int fd)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tcp_mgr_connection *tmc = tcp_mgr_incoming_new(tmi);
    if (!tmc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "raft_net_tcp_incoming_new(): NOMEM");
        return -ENOMEM;
    }

    int rc = tcp_socket_handle_accept(fd, &tmc->tmc_tsh);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_handle_accept(): %d", rc);
        tcp_mgr_incoming_fini_err(tmc);

        return rc;
    }

    tcp_mgr_connection_epoll_add(tmc, EPOLLIN, tcp_mgr_handshake_cb, NULL);

    return 0;
}

static void
tcp_mgr_listen_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_NOTIFY);
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_instance *tmi = eph->eph_arg;

    int rc = tcp_mgr_accept(tmi, eph->eph_fd);
    if (rc < 0)
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_mgr_accept(): %d", rc);
}

int
tcp_mgr_epoll_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epoll_mgr)
{
    if (!tmi || !epoll_mgr)
        return -EINVAL;

    tmi->tmi_epoll_mgr = epoll_mgr;

    int rc = epoll_handle_init(&tmi->tmi_listen_eph,
                               tmi->tmi_listen_socket.tsh_socket, EPOLLIN,
                               tcp_mgr_listen_cb, tmi, NULL);

    return rc ? rc :
        epoll_handle_add(epoll_mgr, &tmi->tmi_listen_eph);
}

static int
tcp_mgr_connection_epoll_mod(struct tcp_mgr_connection *tmc, int op,
                             epoll_mgr_cb_t cb)
{
    NIOVA_ASSERT(tmc);

    if (cb)
        tmc->tmc_eph.eph_cb = cb;

    if (op)
        tmc->tmc_eph.eph_events = op;

    return epoll_handle_mod(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);
};

static int
tcp_mgr_handshake_fill_send(struct tcp_mgr_connection *tmc)
{
    struct iovec handshake_iov;
    tcp_mgr_handshake_iov_init(tmc->tmc_tmi, &handshake_iov);

    tmc->tmc_tmi->tmi_handshake_fill(tmc->tmc_tmi->tmi_data,
                                     handshake_iov.iov_base,
                                     handshake_iov.iov_len);

    int rc = tcp_socket_send(&tmc->tmc_tsh, &handshake_iov, 1);

    tcp_mgr_handshake_iov_fini(&handshake_iov);

    return rc;
}

static int
tcp_mgr_connect_complete(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    int rc;

    if (tmc->tmc_status != TMCS_CONNECTING)
    {
        DBG_TCP_MGR_CXN(LL_ERROR, tmc, "unexpected connection status %d",
                        tmc->tmc_status);
        rc = -EAGAIN;
        goto out;
    }

    rc = tcp_mgr_handshake_fill_send(tmc);
    if (rc < 0)
        goto out;

    DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "reinstalling epoll handler");

    rc = tcp_mgr_connection_epoll_mod(tmc, EPOLLIN, tcp_mgr_recv_cb);
    if (rc < 0)
    {
        DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "error reinstalling epoll handler");
        goto out;
    }

    DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "connection established");
    tmc->tmc_status = TMCS_CONNECTED;
    rc = 0;
out:
    if (rc < 0)
        tcp_mgr_connection_close(tmc);

    return rc;
}

static void
tcp_mgr_connect_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_connection *tmc = eph->eph_arg;
    SIMPLE_LOG_MSG(LL_NOTIFY,
                   "tcp_mgr_connect_cb(): connecting to %s:%d[%p]",
                   tmc->tmc_tsh.tsh_ipaddr, tmc->tmc_tsh.tsh_port, tmc);

    int rc = epoll_handle_rc_get(eph, events);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_tcp_connect_cb(): error, rc=%d",
                       rc);
        tcp_mgr_connection_close(tmc);
        return;
    }

    tcp_mgr_connect_complete(tmc);
}

static int
tcp_mgr_connection_verify(struct tcp_mgr_connection *tmc,
                          bool do_connect)
{
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "do_connect=%d", do_connect);
    NIOVA_ASSERT(tmc && tmc->tmc_status != TMCS_NEEDS_SETUP);

    enum tcp_mgr_connection_status status = tmc->tmc_status;

    int rc = 0;
    if (status != TMCS_DISCONNECTED || !do_connect)
    {
        if (status == TMCS_CONNECTED)
            rc = 0;
        else if (status == TMCS_CONNECTING)
            rc = -EALREADY;
        else if (!do_connect || status == TMCS_DISCONNECTING)
            rc = -ENOTCONN;
        else
            rc = -EINVAL;

        return rc;
    }

    NIOVA_ASSERT(
        tmc->tmc_tsh.tsh_socket < 0 && status == TMCS_DISCONNECTED);

    tmc->tmc_status = TMCS_CONNECTING;

    rc = tcp_socket_setup(&tmc->tmc_tsh);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_setup(): %d", rc);
        return rc;
    }

    rc = tcp_mgr_connection_epoll_add(tmc, EPOLLOUT, tcp_mgr_connect_cb,
                                      NULL);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_mgr_connection_epoll_add(): %d", rc);
        return rc;
    }

    rc = tcp_socket_connect(&tmc->tmc_tsh);
    if (rc < 0 && rc != -EINPROGRESS)
    {
        tcp_mgr_connection_close(tmc);
        SIMPLE_LOG_MSG(LL_WARN, "tcp_socket_connect(): %d", rc );
    }
    else if (rc == -EINPROGRESS)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "waiting for connect callback, fd = %d",
                       tmc->tmc_tsh.tsh_socket);
    }
    else
    {
        return tcp_mgr_connect_complete(tmc);
    }

    return rc;
}

int
tcp_mgr_send_msg(struct tcp_mgr_connection *tmc, struct iovec *iov,
                 size_t niovs)
{
    int rc = tcp_mgr_connection_verify(tmc, true);
    DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "tcp_mgr_connection_verify(): %d", rc);
    if (rc < 0)
        return -ENOTCONN;

    rc = tcp_socket_send(&tmc->tmc_tsh, iov, niovs);

    if (rc == -ENOTCONN || rc == -ECONNRESET)
        tcp_mgr_connection_close(tmc);

    return rc;
}

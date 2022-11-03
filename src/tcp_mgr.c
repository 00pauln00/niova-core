#include <sys/ioctl.h>

#include "alloc.h"
#include "log.h"
#include "epoll_mgr.h"
#include "io.h"
#include "tcp.h"
#include "tcp_mgr.h"
#include "util.h"

REGISTRY_ENTRY_FILE_GENERATE;

void
tcp_mgr_credits_set(niova_atomic32_t *credits, uint32_t cnt)
{
    niova_atomic_init(credits, cnt);
}

static void *
tcp_mgr_credits_malloc(niova_atomic32_t *credits, size_t sz)
{
    uint32_t new_credits = niova_atomic_dec(credits);
    SIMPLE_LOG_MSG(LL_TRACE, "malloc sz %lu, new cred %u", sz, new_credits);
    if (new_credits < 0)
    {
        niova_atomic_inc(credits);
        return NULL;
    }


    void *buf = niova_malloc_can_fail(sz);
    if (!buf)
        return NULL;

    return buf;
}

static void
tcp_mgr_credits_free(niova_atomic32_t *credits, void *buf)
{
    niova_free(buf);

    niova_atomic_inc(credits);
}

void
tcp_mgr_bulk_credits_set(struct tcp_mgr_instance *tmi, uint32_t cnt)
{
    tcp_mgr_credits_set(&tmi->tmi_bulk_credits, cnt);
}

void
tcp_mgr_incoming_credits_set(struct tcp_mgr_instance *tmi, uint32_t cnt)
{
    tcp_mgr_credits_set(&tmi->tmi_incoming_credits, cnt);
}

static void *
tcp_mgr_bulk_malloc(struct tcp_mgr_instance *tmi, size_t sz)
{
    return tcp_mgr_credits_malloc(&tmi->tmi_bulk_credits, sz);
}

static void
tcp_mgr_bulk_free(struct tcp_mgr_instance *tmi, void *buf)
{
    tcp_mgr_credits_free(&tmi->tmi_bulk_credits, buf);
}

static void
tcp_mgr_connection_put(struct tcp_mgr_connection *tmc)
{
    if (tmc->tmc_tmi && tmc->tmc_tmi->tmi_connection_ref_cb)
        tmc->tmc_tmi->tmi_connection_ref_cb(tmc, EPH_REF_PUT);
}

void
tcp_mgr_setup(struct tcp_mgr_instance *tmi, void *data,
              epoll_mgr_ref_cb_t connection_ref_cb,
              tcp_mgr_recv_cb_t recv_cb,
              tcp_mgr_bulk_size_cb_t bulk_size_cb,
              tcp_mgr_is_conn_for_client_cb_t is_conn_for_client_cb,
              tcp_mgr_handshake_cb_t handshake_cb,
              tcp_mgr_handshake_fill_t handshake_fill,
              size_t handshake_size, uint32_t bulk_credits,
              uint32_t incoming_credits)
{
    NIOVA_ASSERT(tmi);

    tmi->tmi_data = data;
    tmi->tmi_connection_ref_cb = connection_ref_cb;
    tmi->tmi_recv_cb = recv_cb;
    tmi->tmi_bulk_size_cb = bulk_size_cb;
    tmi->tmi_is_conn_for_client_cb = is_conn_for_client_cb;
    tmi->tmi_handshake_cb = handshake_cb;
    tmi->tmi_handshake_fill = handshake_fill;
    tmi->tmi_handshake_size = handshake_size;

    tcp_mgr_bulk_credits_set(tmi, bulk_credits);
    tcp_mgr_incoming_credits_set(tmi, incoming_credits);

    pthread_mutex_init(&tmi->tmi_epoll_ctx_mutex, NULL);
}

int
tcp_mgr_sockets_setup(struct tcp_mgr_instance *tmi, const char *ipaddr,
                      int port)
{
    strncpy(tmi->tmi_listen_socket.tsh_ipaddr, ipaddr, IPV4_STRLEN);
    tmi->tmi_listen_socket.tsh_ipaddr[IPV4_STRLEN - 1] = 0;
    tmi->tmi_listen_socket.tsh_port = port;

    return tcp_socket_setup(&tmi->tmi_listen_socket);
}

int
tcp_mgr_sockets_close(struct tcp_mgr_instance *tmi)
{
    if (tmi->tmi_listen_eph.eph_installed)
        epoll_handle_del(tmi->tmi_epoll_mgr, &tmi->tmi_listen_eph);

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

static int
tcp_mgr_connection_setup_internal(struct tcp_mgr_connection *tmc,
                                  struct tcp_mgr_instance *tmi,
                                  void (*ref_cb)(void *,
                                                 enum epoll_handle_ref_op))
{
    SIMPLE_LOG_MSG(LL_TRACE, "tcp_mgr_connection_setup(): tmc %p", tmc);

    tcp_socket_handle_init(&tmc->tmc_tsh);

    int rc = epoll_handle_init(&tmc->tmc_eph, -1, 0, NULL, tmc, ref_cb);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "epoll_handle_init(): rc=%d", rc);
        return rc;
    }

    tmc->tmc_tmi = tmi;

    // setup during handshake
    tmc->tmc_header_size = 0;

    tmc->tmc_bulk_buf = NULL;
    tmc->tmc_bulk_offset = 0;
    tmc->tmc_bulk_remain = 0;

    tmc->tmc_epoll_ctx_cb = NULL;

    pthread_mutex_init(&tmc->tmc_mutex, NULL);
    tmc->tmc_status = TMCS_DISCONNECTED;

    return 0;
}

void
tcp_mgr_connection_setup(struct tcp_mgr_connection *tmc,
                         struct tcp_mgr_instance *tmi,
                         const char *ipaddr, int port)
{
    if (tmc->tmc_status != TMCS_NEEDS_SETUP)
    {
        DBG_TCP_MGR_CXN(LL_ERROR, tmc,
                        "setup called on existing conn, status=%d",
                        tmc->tmc_status);
        return;
    }
    int rc = tcp_mgr_connection_setup_internal(tmc, tmi,
                                               tmi->tmi_connection_ref_cb);
    if (rc)
        return;

    tcp_socket_handle_set_data(&tmc->tmc_tsh, ipaddr, port);
}

static void
tcp_mgr_connection_close_internal(struct tcp_mgr_connection *tmc)
{
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "closing");

    if (tmc->tmc_status != TMCS_CONNECTING && tmc->tmc_status != TMCS_CONNECTED)
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "unexpected status: %d",
                        tmc->tmc_status);

    tmc->tmc_status = TMCS_DISCONNECTING;
    if (tmc->tmc_eph.eph_installed)
        epoll_handle_del(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);

    if (tmc->tmc_bulk_buf)
        tcp_mgr_bulk_free(tmc->tmc_tmi, tmc->tmc_bulk_buf);

    tmc->tmc_bulk_buf = NULL;
    tmc->tmc_bulk_offset = 0;
    tmc->tmc_bulk_remain = 0;

    tcp_socket_close(&tmc->tmc_tsh);
    tmc->tmc_status = TMCS_DISCONNECTED;
}

/*
 * tcp_mgr_incoming_getput is required by epoll_mgr_ctx_cb_add. Incoming
 * connections are created and destroyed as part of connection workflow,
 * however, so ref counting is only used as a sanity check.
 */
static void
tcp_mgr_incoming_getput(void *tmc, enum epoll_handle_ref_op op)
{
    struct tcp_mgr_incoming_connection *incoming =
        OFFSET_CAST(tcp_mgr_incoming_connection, tmic_tmc, tmc);

    int newref = (op == EPH_REF_GET)
        ? niova_atomic_inc(&incoming->tmic_refcnt)
        : niova_atomic_dec(&incoming->tmic_refcnt);

    SIMPLE_LOG_MSG(LL_TRACE, "newref %d", newref);
    NIOVA_ASSERT(newref > 0);
}

static struct tcp_mgr_connection *
tcp_mgr_incoming_new(struct tcp_mgr_instance *tmi)
{
    struct tcp_mgr_incoming_connection *incoming =
        tcp_mgr_credits_malloc(&tmi->tmi_incoming_credits,
                               sizeof(struct tcp_mgr_incoming_connection));
    if (!incoming)
        return NULL;

    niova_atomic_init(&incoming->tmic_refcnt, 1);
    struct tcp_mgr_connection *tmc = &incoming->tmic_tmc;

    tmc->tmc_status = TMCS_NEEDS_SETUP;
    int rc = tcp_mgr_connection_setup_internal(tmc, tmi,
                                               tcp_mgr_incoming_getput);
    if (rc)
    {
        tcp_mgr_credits_free(&tmi->tmi_incoming_credits, incoming);
        return NULL;
    }

    tmc->tmc_status = TMCS_CONNECTING;

    return tmc;
}

static void
tcp_mgr_incoming_fini_err(struct tcp_mgr_connection *tmc)
{
    struct tcp_mgr_incoming_connection *incoming =
        OFFSET_CAST(tcp_mgr_incoming_connection, tmic_tmc, tmc);
    int refcnt = niova_atomic_read(&incoming->tmic_refcnt);
    NIOVA_ASSERT(refcnt == 1);

    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;
    tcp_mgr_connection_close_internal(tmc);
    tcp_mgr_credits_free(&tmi->tmi_incoming_credits, incoming);
}

static void
tcp_mgr_incoming_fini(struct tcp_mgr_connection *tmc)
{
    struct tcp_mgr_incoming_connection *incoming =
        OFFSET_CAST(tcp_mgr_incoming_connection, tmic_tmc, tmc);
    int refcnt = niova_atomic_read(&incoming->tmic_refcnt);
    NIOVA_ASSERT(refcnt == 1);

    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;
    tcp_mgr_credits_free(&tmi->tmi_incoming_credits, incoming);
}

static epoll_mgr_cb_ctx_t
tcp_mgr_epoll_ctx_cb(void *data)
{
    struct tcp_mgr_connection *tmc = data;
    if (tmc->tmc_epoll_ctx_cb)
        tmc->tmc_epoll_ctx_cb(tmc);

    tmc->tmc_epoll_ctx_cb = NULL;
}

static int
tcp_mgr_connection_epoll_ctx_run(struct tcp_mgr_connection *tmc,
                                 tcp_mgr_connection_epoll_ctx_cb_t cb)
{
    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "");

    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;
    struct epoll_mgr *epm = tmi->tmi_epoll_mgr;
    if (!cb)
        return -EINVAL;

    niova_mutex_lock(&tmi->tmi_epoll_ctx_mutex);
    if (tmc->tmc_epoll_ctx_cb)
    {
        // can happen if other thread is about to call epoll_mgr_ctx_cb_add
        if (!tmc->tmc_eph.eph_ctx_cb)
            SIMPLE_LOG_MSG(LL_DEBUG, "mismatch between tmc and eph ctx cbs");
        niova_mutex_unlock(&tmi->tmi_epoll_ctx_mutex);
        return -EAGAIN;
    }
    else
    {
        tmc->tmc_epoll_ctx_cb = cb;
        niova_mutex_unlock(&tmi->tmi_epoll_ctx_mutex);
    }

    int rc = epoll_mgr_ctx_cb_add(epm, &tmc->tmc_eph, tcp_mgr_epoll_ctx_cb);
    if (rc)
    {
        tmc->tmc_epoll_ctx_cb = NULL;
        SIMPLE_LOG_MSG(LL_TRACE, "epoll_mgr_ctx_cb_add(): rc=%d eph=%p", rc,
                       &tmc->tmc_eph);
        LOG_MSG(LL_ERROR, "cannot start epoll connection task, rc: %d", rc);
    }

    return rc;
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
tcp_mgr_epoll_handle_rc_get(const struct epoll_handle *eph,
                            uint32_t events)
{
    int rc = 0;

    if (events & (EPOLLHUP | EPOLLERR))
    {
        socklen_t rc_len = sizeof(rc);
        int rc2 = getsockopt(eph->eph_fd, SOL_SOCKET, SO_ERROR, &rc, &rc_len);
        if (rc2)
        {
            SIMPLE_LOG_MSG(LL_ERROR, "Error getting socket error: %d", rc2);
            return rc2;
        }
        SIMPLE_LOG_MSG(LL_DEBUG, "ev=%d: received %s, rc=%d", events,
                       events & EPOLLHUP ? "HUP" : "ERR", rc);
    }

    return -rc;
}

static int
tcp_mgr_bulk_progress_recv(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    if (!tmc->tmc_bulk_remain)
        return 0;

    char *bulk = tmc->tmc_bulk_buf + tmc->tmc_bulk_offset;

    struct iovec iov = {
        .iov_base = bulk,
        .iov_len = tmc->tmc_bulk_remain,
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

    NIOVA_ASSERT(recv_bytes <= tmc->tmc_bulk_remain);

    tmc->tmc_bulk_offset += recv_bytes;
    tmc->tmc_bulk_remain -= recv_bytes;

    return 0;
}

static int
tcp_mgr_bulk_prepare_and_recv(struct tcp_mgr_connection *tmc, size_t bulk_size,
                              void *hdr, size_t hdr_size)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    NIOVA_ASSERT(tmc && !tmc->tmc_bulk_buf && !tmc->tmc_bulk_remain);
    if (!bulk_size)
        return 0;

    size_t buf_size = hdr_size + bulk_size;
    if (buf_size > TCP_MGR_MAX_BULK_SIZE)
        return -E2BIG;

    // buffer free'd in tcp_mgr_recv_cb
    void *buf = tcp_mgr_bulk_malloc(tmc->tmc_tmi, buf_size);
    if (!buf)
    {
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "cannot allocate bulk buf");

        int bytes_avail;
        ioctl(tmc->tmc_tsh.tsh_socket, FIONREAD, &bytes_avail);
        if (bytes_avail >= bulk_size)
            buf = niova_malloc_can_fail(buf_size);

        if (!buf)
            return -ENOMEM;
    }

    if (hdr && hdr_size)
        memcpy(buf, hdr, hdr_size);

    tmc->tmc_bulk_buf = buf;
    tmc->tmc_bulk_offset = hdr_size;
    tmc->tmc_bulk_remain = bulk_size;

    return 0;
}

static int
tcp_mgr_peer_bulk_prepare_and_recv(struct tcp_mgr_connection *tmc, size_t bulk_size,
                              void *hdr, size_t hdr_size, char *recv_buf)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    NIOVA_ASSERT(tmc && !tmc->tmc_bulk_buf && !tmc->tmc_bulk_remain);

    size_t buf_size = hdr_size + bulk_size;
    if (buf_size > TCP_MGR_MAX_BULK_SIZE)
        return -E2BIG;

    if (hdr && hdr_size)
        memcpy(recv_buf, hdr, hdr_size);

    tmc->tmc_bulk_buf = recv_buf;
    tmc->tmc_bulk_offset = hdr_size;
    tmc->tmc_bulk_remain = bulk_size;

    return 0;
}

static int
tcp_mgr_get_msg_header(struct tcp_mgr_connection *tmc, char *buffer)
{
    //struct tcp_mgr_instance *tmi = tmc->tmc_tmi;
    //tcp_mgr_bulk_size_cb_t bulk_size_cb = tmi->tmi_bulk_size_cb;
    size_t header_size = tmc->tmc_header_size;
    int socket = tmc->tmc_tsh.tsh_socket;
    int flags = MSG_PEEK;

    ssize_t rc = recv(socket, buffer, header_size, flags);
    if (rc == -EAGAIN)
        rc = 0;

    else if (rc <= 0)
        return rc;


    return 0;
}

static int
tcp_mgr_new_bulk_msg_handler(struct tcp_mgr_connection *tmc, char *recv_buf)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;

    tcp_mgr_recv_cb_t recv_cb = tmi->tmi_recv_cb;
    tcp_mgr_bulk_size_cb_t bulk_size_cb = tmi->tmi_bulk_size_cb;
    size_t header_size = tmc->tmc_header_size;

    NIOVA_ASSERT(recv_cb && bulk_size_cb && header_size);

    // TODO there will be memory allocated from the memtable.
    static char sink_buf[TCP_MGR_MAX_HDR_SIZE];
    struct iovec iov;
    iov.iov_base = sink_buf;
    iov.iov_len = header_size;

    // try twice in case interrupts cause short read
    ssize_t rc = tcp_socket_recv_all(&tmc->tmc_tsh, &iov, NULL, 2);
    if (rc != header_size)
        return rc < 0 ? rc : -ECOMM;

    uint32_t msg_type = 0;
    rc = bulk_size_cb(tmc, sink_buf, tmi->tmi_data, &msg_type);
    if (rc < 0)
        return rc;

    return tcp_mgr_peer_bulk_prepare_and_recv(tmc, rc, sink_buf, header_size, recv_buf);
}

int
tcp_mgr_recv_req_from_socket(struct tcp_mgr_connection *tmc, char *recv_buf,
                             size_t *recv_buf_size,
                             bool take_lock)
{
    int rc;

    if (take_lock)
        niova_mutex_lock(&tmc->tmc_mutex);
    rc = tcp_mgr_new_bulk_msg_handler(tmc, recv_buf);

    if (rc)
    {
        if (take_lock)
            niova_mutex_unlock(&tmc->tmc_mutex);
        return rc;
    }

    while (tmc->tmc_bulk_remain)
    {
        NIOVA_ASSERT(tmc->tmc_bulk_buf);

        rc = tcp_mgr_bulk_progress_recv(tmc);
        if (rc < 0)
            SIMPLE_LOG_MSG(LL_NOTIFY, "cannot complete bulk read, rc=%d", rc);
    }

    if (take_lock)
        niova_mutex_unlock(&tmc->tmc_mutex);
    *recv_buf_size = tmc->tmc_bulk_offset;
    return rc;
}

//XXX we need to call this once read/write completes.
int
tcp_mgr_peer_bulk_complete(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    tmc->tmc_bulk_buf = NULL;
    tmc->tmc_bulk_offset = 0;
    // Rearm the epoll
    int rc = epoll_handle_mod(tmc->tmc_tmi->tmi_epoll_mgr, &tmc->tmc_eph);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "Failed to rearm the epoll");
    }

    return 0;
}

static int
tcp_mgr_new_msg_handler(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

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

    uint32_t msg_type = 0;
    rc = bulk_size_cb(tmc, sink_buf, tmi->tmi_data, &msg_type);
    if (rc < 0)
        return rc;

    return rc == 0
        ? recv_cb(tmc, sink_buf, header_size, tmi->tmi_data)
        : tcp_mgr_bulk_prepare_and_recv(tmc, rc, sink_buf, header_size);
}

static int
tcp_mgr_bulk_complete(struct tcp_mgr_connection *tmc)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;
    NIOVA_ASSERT(tmi->tmi_recv_cb);

    int rc = tmi->tmi_recv_cb(tmc, tmc->tmc_bulk_buf, tmc->tmc_bulk_offset,
                              tmc->tmc_tmi->tmi_data);
    tcp_mgr_bulk_free(tmi, tmc->tmc_bulk_buf);

    tmc->tmc_bulk_buf = NULL;
    tmc->tmc_bulk_offset = 0;

    return rc;
}

static int
tcp_recv_common(struct tcp_mgr_connection *tmc)
{
    int rc = 0;
    // is this a new RPC?
    if (!tmc->tmc_bulk_remain)
    {
        NIOVA_ASSERT(!tmc->tmc_bulk_buf);

        rc = tcp_mgr_new_msg_handler(tmc);
        if (rc < 0)
            SIMPLE_LOG_MSG(LL_NOTIFY, "cannot read RPC, rc=%d", rc);
    }

    // no 'else', tcp_mgr_new_msg_handler can initiate bulk read
    if (rc == 0 && tmc->tmc_bulk_remain)
    {
        NIOVA_ASSERT(tmc->tmc_bulk_buf);

        rc = tcp_mgr_bulk_progress_recv(tmc);
        if (rc < 0)
            SIMPLE_LOG_MSG(LL_NOTIFY, "cannot complete bulk read, rc=%d", rc);
        else if (!tmc->tmc_bulk_remain)
            rc = tcp_mgr_bulk_complete(tmc);
    }

    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "error in recv, closing");
        tcp_mgr_connection_close_internal(tmc);
    }
    return rc;
}

static int
tcp_mgr_recv_with_edge_triggered(struct tcp_mgr_connection *tmc)
{
    size_t header_size = tmc->tmc_header_size;
    char *buffer =  niova_calloc(1UL, TCP_MGR_MAX_HDR_SIZE);
    int rc  = tcp_mgr_get_msg_header(tmc, buffer);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "Can't read the header from socket");
        niova_free(buffer);
        return rc;
    }

    rc = tmc->tmc_tmi->tmi_recv_cb(tmc, buffer, header_size,
                                   tmc->tmc_tmi->tmi_data);

    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tmi_recv_cb failed for server");
        tcp_mgr_connection_close_internal(tmc);
    }
    niova_free(buffer);
    return rc;
}

static epoll_mgr_cb_ctx_t
tcp_mgr_recv_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_connection *tmc = eph->eph_arg;
    DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "received events: %d", events);

    int rc = tcp_mgr_epoll_handle_rc_get(eph, events);
    if (rc)
    {
        if (rc == -ECONNRESET || rc == -ENOTCONN || rc == -ECONNABORTED)
            tcp_mgr_connection_close_internal(tmc);

        SIMPLE_LOG_MSG(LL_NOTIFY, "error received on socket fd=%d, rc=%d",
                       eph->eph_fd, rc);
        return;
    }
    struct tcp_mgr_instance *tmi = tmc->tmc_tmi;

    bool from_client = tmi->tmi_is_conn_for_client_cb(tmc);

    if (from_client)
        tcp_mgr_recv_with_edge_triggered(tmc);
    else
        tcp_recv_common(tmc);
}

static int
tcp_mgr_connection_merge_incoming(struct tcp_mgr_connection *incoming,
                                  struct tcp_mgr_connection *owned)
{
    NIOVA_ASSERT(incoming && owned && incoming->tmc_status == TMCS_CONNECTING);

    struct tcp_mgr_instance *tmi = incoming->tmc_tmi;
    bool tmc_locked = false;

    // Don't treat it as error if owned is already in connected state
    if (owned->tmc_status != TMCS_DISCONNECTED && owned->tmc_status != TMCS_CONNECTED)
    {
        if (owned->tmc_status == TMCS_NEEDS_SETUP)
            DBG_TCP_MGR_CXN(LL_ERROR, owned, "connection not setup");
        else
            DBG_TCP_MGR_CXN(LL_NOTIFY, owned,
                            "incoming connection, but outgoing conn exists,"
                            " status: %d",
                            owned->tmc_status);

        // if peer is attempting to connect again, assume old connection is dead
        // XXX it would be better to send a ping, and handle failure there
        if (owned->tmc_status == TMCS_CONNECTED)
            tcp_mgr_connection_close_internal(owned);

        tcp_mgr_connection_put(owned);

        SIMPLE_LOG_MSG(LL_ERROR, "handshake error");
        return -EINVAL;
    }

    if (owned->tmc_status == TMCS_CONNECTED)
    {
        // Update the owned with lock as reader threads might be
        // accessing the tmc at the same time.
        niova_mutex_lock(&owned->tmc_mutex);
        tmc_locked = true;
    }
    owned->tmc_header_size = incoming->tmc_header_size;
    owned->tmc_tsh.tsh_socket = incoming->tmc_tsh.tsh_socket;
    owned->tmc_status = TMCS_CONNECTED;

    if (incoming->tmc_eph.eph_installed)
        epoll_handle_del(tmi->tmi_epoll_mgr, &incoming->tmc_eph);

    // Check if sender is client. Epoll with edge triggered and oneshot for client
    bool is_sender_client = tmi->tmi_is_conn_for_client_cb(owned);
    uint32_t events = is_sender_client ? EPOLLIN | EPOLLET | EPOLLONESHOT : EPOLLIN;
    tcp_mgr_connection_epoll_add(owned, events, tcp_mgr_recv_cb,
                                 tmi->tmi_connection_ref_cb);

    DBG_TCP_MGR_CXN(LL_NOTIFY, owned, "connection established");

    if (tmc_locked)
        niova_mutex_unlock(&owned->tmc_mutex);

    // epoll takes a ref, so we can give up ours
    tcp_mgr_connection_put(owned);
    return 0;
}

static epoll_mgr_cb_ctx_t
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

    struct tcp_mgr_connection *new_tmc = NULL;

    rc = tmi->tmi_handshake_cb(tmi->tmi_data, &new_tmc, &tmc->tmc_header_size,
                               tmc->tmc_tsh.tsh_socket, iov.iov_base, rc);

    tcp_mgr_handshake_iov_fini(&iov);

    if (rc == 0)
        rc = tcp_mgr_connection_merge_incoming(tmc, new_tmc);

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

static epoll_mgr_cb_ctx_t
tcp_mgr_listen_cb(const struct epoll_handle *eph, uint32_t events)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_instance *tmi = eph->eph_arg;

    int rc = tcp_mgr_accept(tmi, eph->eph_fd);
    if (rc < 0)
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_mgr_accept(): %d", rc);
}

/* must call sockets_setup and sockets_bind first */
int
tcp_mgr_epoll_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epoll_mgr,
                    bool tmi_listen)
{
    if (!tmi || !epoll_mgr)
        return -EINVAL;

    tmi->tmi_epoll_mgr = epoll_mgr;

    // raft client do not need listen socket.
    if (!tmi_listen)
        return 0;

    int rc = epoll_handle_init(&tmi->tmi_listen_eph,
                               tmi->tmi_listen_socket.tsh_socket, EPOLLIN,
                               tcp_mgr_listen_cb, tmi, NULL);

    return rc ? rc : epoll_handle_add(epoll_mgr, &tmi->tmi_listen_eph);
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

    size_t header_size =
        tmc->tmc_tmi->tmi_handshake_fill(tmc->tmc_tmi->tmi_data, tmc,
                                         handshake_iov.iov_base,
                                         handshake_iov.iov_len);
    tmc->tmc_header_size = header_size;

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

    DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "reinstalling epoll handler");
    rc = tcp_mgr_connection_epoll_mod(tmc, EPOLLIN, tcp_mgr_recv_cb);
    if (rc < 0)
    {
        DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "error reinstalling epoll handler");
        goto out;
    }

    rc = tcp_mgr_handshake_fill_send(tmc);
    if (rc < 0)
        goto out;

    DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "connection established");
    tmc->tmc_status = TMCS_CONNECTED;
    rc = 0;
out:
    if (rc < 0)
        tcp_mgr_connection_close_internal(tmc);

    return rc;
}

static epoll_mgr_cb_ctx_t
tcp_mgr_connect_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct tcp_mgr_connection *tmc = eph->eph_arg;
    SIMPLE_LOG_MSG(LL_NOTIFY,
                   "tcp_mgr_connect_cb(): connecting to %s:%d[%p]",
                   tmc->tmc_tsh.tsh_ipaddr, tmc->tmc_tsh.tsh_port, tmc);

    int rc = tcp_mgr_epoll_handle_rc_get(eph, events);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "raft_net_tcp_connect_cb(): error, rc=%d",
                       rc);
        tcp_mgr_connection_close_internal(tmc);
        return;
    }

    tcp_mgr_connect_complete(tmc);
}

static void
tcp_mgr_connection_connect_epoll_ctx(struct tcp_mgr_connection *tmc)
{
    if (tmc->tmc_status != TMCS_DISCONNECTED)
    {
        DBG_TCP_MGR_CXN(LL_NOTIFY, tmc, "no longer disconnected");
        return;
    }

    tmc->tmc_status = TMCS_CONNECTING;

    int rc = tcp_socket_setup(&tmc->tmc_tsh);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_socket_setup(): %d", rc);
        return;
    }

    rc = tcp_mgr_connection_epoll_add(tmc, EPOLLOUT, tcp_mgr_connect_cb,
                                      tmc->tmc_tmi->tmi_connection_ref_cb);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "tcp_mgr_connection_epoll_add(): %d", rc);
        return;
    }

    rc = tcp_socket_connect(&tmc->tmc_tsh);
    if (rc < 0 && rc != -EINPROGRESS)
    {
        tcp_mgr_connection_close_internal(tmc);
    }
    else if (rc == -EINPROGRESS)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "waiting for connect callback, fd = %d",
                       tmc->tmc_tsh.tsh_socket);
    }
    else
    {
        tcp_mgr_connect_complete(tmc);
    }

    return;
}

static int
tcp_mgr_connection_verify(struct tcp_mgr_connection *tmc,
                          bool do_connect)
{
    enum tcp_mgr_connection_status status = tmc->tmc_status;

    DBG_TCP_MGR_CXN(LL_TRACE, tmc, "do_connect=%d status=%d", do_connect,
                    status);

    NIOVA_ASSERT(tmc && status != TMCS_NEEDS_SETUP);

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

    // can't set status to CONNECTING here because we aren't in epoll_ctx
    tcp_mgr_connection_epoll_ctx_run(tmc, tcp_mgr_connection_connect_epoll_ctx);

    return -EAGAIN;
}

int
tcp_mgr_send_msg(struct tcp_mgr_connection *tmc, struct iovec *iov,
                 size_t niovs)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);
    if (tmc->tmc_status == TMCS_NEEDS_SETUP)
    {
        DBG_TCP_MGR_CXN(LL_ERROR, tmc, "connection not setup");
        return -EINVAL;
    }

    int rc = tcp_mgr_connection_verify(tmc, true);
    DBG_TCP_MGR_CXN(LL_DEBUG, tmc, "tcp_mgr_connection_verify(): %d", rc);
    if (rc < 0)
        return rc;

    const ssize_t total_size = niova_io_iovs_total_size_get(iov, niovs);

    ssize_t send_rc = tcp_socket_send(&tmc->tmc_tsh, iov, niovs);

    if (send_rc != total_size)
    {
        DBG_TCP_MGR_CXN(LL_WARN, tmc,
                        "tcp_socket_send(): returns=%zd, expected=%zd",
                        send_rc, total_size);
        tcp_mgr_connection_close(tmc);
    }

    return rc;
}

void
tcp_mgr_connection_close(struct tcp_mgr_connection *tmc)
{
    tcp_mgr_connection_epoll_ctx_run(tmc, tcp_mgr_connection_close_internal);
};

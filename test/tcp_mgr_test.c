#include <stdio.h>

#include "alloc.h"
#include "atomic.h"
#include "epoll_mgr.h"
#include "tcp_mgr.h"
#include "util.h"

#define IPADDR "127.0.0.1"
#define PORT1 1701
#define PORT2 1702

#define MAGIC 0x0D15EA5E

#define TEST_TIME 10*1000

REGISTRY_ENTRY_FILE_GENERATE;

struct message
{
    size_t msg_size;
    char   msg_buf[];
};

struct owned_connection
{
    niova_atomic32_t          oc_ref_cnt;
    struct tcp_mgr_connection oc_tmc;
    struct thread_ctl         oc_send_thread;
    struct thread_ctl         oc_close_thread;
    LIST_ENTRY(owned_connection) oc_lentry;
};
LIST_HEAD(owned_connection_list, owned_connection) oc_head =
    LIST_HEAD_INITIALIZER(oc_head);
pthread_mutex_t oc_head_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct owned_connection *
owned_connection_new(struct tcp_mgr_instance *tmi)
{
    niova_mutex_lock(&oc_head_mutex);
    struct owned_connection *oc =
        niova_malloc_can_fail(sizeof(struct owned_connection));
    if (!oc)
    {
        niova_mutex_unlock(&oc_head_mutex);
        return NULL;
    }

    LIST_INSERT_HEAD(&oc_head, oc, oc_lentry);

    niova_atomic_init(&oc->oc_ref_cnt, 1);
    oc->oc_tmc.tmc_status = TMCS_NEEDS_SETUP;
    tcp_mgr_connection_setup(tmi, &oc->oc_tmc);

    niova_mutex_unlock(&oc_head_mutex);

    SIMPLE_LOG_MSG(LL_TRACE, "oc: %p", oc);

    return oc;
}

static void
owned_connection_fini(struct owned_connection *oc)
{
    SIMPLE_LOG_MSG(LL_TRACE, "oc: %p", oc);

    niova_mutex_lock(&oc_head_mutex);
    LIST_REMOVE(oc, oc_lentry);
    niova_free(oc);
    niova_mutex_unlock(&oc_head_mutex);
}

static void
owned_connection_getput(void *data, enum epoll_handle_ref_op op)
{
    struct tcp_mgr_connection *tmc = (struct tcp_mgr_connection *)data;
    struct owned_connection *oc = OFFSET_CAST(owned_connection, oc_tmc, tmc);
    if (op == EPH_REF_GET)
    {
        niova_atomic_inc(&oc->oc_ref_cnt);
    }
    else
    {
        uint32_t refcnt = niova_atomic_dec(&oc->oc_ref_cnt);
        if (refcnt == 0)
            owned_connection_fini(oc);
    }
}

#define BUF_SZ 255
static void *
send_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    NIOVA_ASSERT(tc && tc->tc_arg);

    static char buf[BUF_SZ];
    static struct iovec iov = {
        .iov_base = buf,
    };
    struct message *msg = (struct message *)buf;

    struct owned_connection *oc = tc->tc_arg;
    owned_connection_getput(oc, EPH_REF_GET);

    int msg_idx = 0;
    THREAD_LOOP_WITH_CTL(tc)
    {
        size_t max_msg = BUF_SZ - sizeof(struct message);
        int msg_size = snprintf(msg->msg_buf, max_msg,
                                "hello from [%s:%d], msg idx=%d",
                                oc->oc_tmc.tmc_tmi->tmi_listen_socket.tsh_ipaddr,
                                oc->oc_tmc.tmc_tmi->tmi_listen_socket.tsh_port,
                                msg_idx);
        msg->msg_size = msg_size;
        iov.iov_len = msg_size + sizeof(struct message);
        SIMPLE_LOG_MSG(LL_NOTIFY, "sending message, msg_size=%d", msg_size);
        int rc = tcp_mgr_send_msg(&oc->oc_tmc, &iov, 1);
        if (rc < 0)
        {
            SIMPLE_LOG_MSG(LL_NOTIFY, "error sending message, rc=%d", rc);
        }
        else
        {
            SIMPLE_LOG_MSG(LL_NOTIFY, "sent message, msg_idx=%d", msg_idx);
            msg_idx++;
        }

        usleep(100 * 1000);
    }
    owned_connection_getput(oc, EPH_REF_PUT);

    return NULL;
}

static void *
close_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    NIOVA_ASSERT(tc && tc->tc_arg);

    struct owned_connection *oc = tc->tc_arg;
    owned_connection_getput(oc, EPH_REF_GET);
    THREAD_LOOP_WITH_CTL(tc)
    {
        sleep(2);

        DBG_TCP_MGR_CXN(LL_NOTIFY, &oc->oc_tmc, "closing connection");
        tcp_mgr_connection_close_async(&oc->oc_tmc);
    }
    owned_connection_getput(oc, EPH_REF_PUT);

    return NULL;
}

void launch_threads(struct owned_connection *oc)
{
    char name[16];
    snprintf(name, 16, "send-%d",
             oc->oc_tmc.tmc_tmi->tmi_listen_socket.tsh_port);
    int rc = thread_create(send_thread, &oc->oc_send_thread, name, oc, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "send thread_create rc=%d", rc);
    thread_ctl_run(&oc->oc_send_thread);

    snprintf(name, 16, "close-%d",
             oc->oc_tmc.tmc_tmi->tmi_listen_socket.tsh_port);
    rc = thread_create(close_thread, &oc->oc_close_thread, name, oc, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "close thread_create rc=%d", rc);


    thread_ctl_run(&oc->oc_close_thread);
}

static int
recv_cb(struct tcp_mgr_connection *tmc, char *buf, size_t buf_size, void *_data)
{
    struct message *msg = (struct message *)buf;

    DBG_TCP_MGR_CXN(LL_NOTIFY, tmc,
                    "[%s:%d] received_message[tmc=%p]: size=%lu str=%s\n",
                    tmc->tmc_tmi->tmi_listen_socket.tsh_ipaddr,
                    tmc->tmc_tmi->tmi_listen_socket.tsh_port,
                    tmc, buf_size, msg->msg_buf);

    return 0;
}

static ssize_t
bulk_size_cb(struct tcp_mgr_connection *tmc, char *header, void *_data)
{
    struct message *msg = (struct message *)header;

    SIMPLE_LOG_MSG(LL_TRACE, "msg_size: %lu", msg->msg_size);

    return msg->msg_size;
}

// XXX data should go at the end of fn signature
static int
handshake_cb(void *data, struct tcp_mgr_connection **tmc_out, int fd,
             void *buf, size_t buf_sz)
{
    if (buf_sz != sizeof(struct message))
        return -EINVAL;

    struct message *msg = (struct message *)buf;

    struct tcp_mgr_instance *tmi = data;
    struct owned_connection *oc = owned_connection_new(tmi);
    if (!oc)
        return -ENOMEM;

    int listen_port = tmi->tmi_listen_socket.tsh_port;
    tcp_socket_handle_set_data(&oc->oc_tmc.tmc_tsh, IPADDR,
                               listen_port == PORT1 ? PORT2 : PORT1);

    *tmc_out = &oc->oc_tmc;
    tcp_mgr_connection_header_size_set(&oc->oc_tmc, sizeof(struct message));

    return (msg->msg_size == MAGIC) ? 0 : -EBADMSG;
}

static ssize_t
handshake_fill(void *_data, struct tcp_mgr_connection *tmc, void *buf, size_t
               buf_sz)
{
    if (buf_sz != sizeof(struct message))
        return -EINVAL;

    struct message *msg = (struct message *)buf;
    msg->msg_size = MAGIC;

    // header size
    return sizeof(struct message);
}

void
test_tcp_mgr_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epm, int
                   port)
{
    tcp_mgr_setup(tmi, tmi, owned_connection_getput, recv_cb, bulk_size_cb,
                  handshake_cb,
                  handshake_fill, sizeof(struct message));

    tcp_mgr_sockets_setup(tmi, IPADDR, port);
    tcp_mgr_sockets_bind(tmi);
    tcp_mgr_epoll_setup(tmi, epm);
}

void *wait_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct epoll_mgr *epm = tc->tc_arg;
    THREAD_LOOP_WITH_CTL(tc)
    {
        epoll_mgr_wait_and_process_events(epm, -1);
    }

    return NULL;
}

void stop_threads(struct owned_connection *oc)
{
    thread_halt_and_destroy(&oc->oc_send_thread);
    thread_halt_and_destroy(&oc->oc_close_thread);
}

void cleanup_connections()
{
    struct owned_connection *oc = LIST_FIRST(&oc_head);
    LIST_INIT(&oc_head);

    while (oc)
    {
        struct owned_connection *next = LIST_NEXT(oc, oc_lentry);

        // fini takes lock
        owned_connection_getput(oc, EPH_REF_PUT);
        oc = next;
    }
}

int
main()
{
    struct epoll_mgr epoll_mgr;
    struct tcp_mgr_instance tcp_mgr1, tcp_mgr2;

    epoll_mgr_setup(&epoll_mgr);
    test_tcp_mgr_setup(&tcp_mgr1, &epoll_mgr, PORT1);
    test_tcp_mgr_setup(&tcp_mgr2, &epoll_mgr, PORT2);

    struct owned_connection *oc1 = owned_connection_new(&tcp_mgr1);
    struct owned_connection *oc2 = owned_connection_new(&tcp_mgr2);

    // tcp mgr will connect to ip/port in tsh
    tcp_socket_handle_set_data(&oc1->oc_tmc.tmc_tsh, IPADDR, PORT2);
    tcp_socket_handle_set_data(&oc2->oc_tmc.tmc_tsh, IPADDR, PORT1);

    launch_threads(oc1);
    launch_threads(oc2);

    struct thread_ctl wait_thread_ctl;
    int rc = thread_create(wait_thread, &wait_thread_ctl, "epoll", &epoll_mgr,
                           NULL);
    NIOVA_ASSERT(!rc);
    thread_ctl_run(&wait_thread_ctl);
    usleep(TEST_TIME * 1000);
    thread_halt_and_destroy(&wait_thread_ctl);

    SIMPLE_LOG_MSG(LL_NOTIFY, "ending");
    stop_threads(oc1);
    stop_threads(oc2);

    cleanup_connections();

    tcp_mgr_sockets_close(&tcp_mgr1);
    tcp_mgr_sockets_close(&tcp_mgr2);

    SIMPLE_LOG_MSG(LL_NOTIFY, "main ended");
}

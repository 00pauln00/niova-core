#include <stdio.h>

#include "alloc.h"
#include "atomic.h"
#include "epoll_mgr.h"
#include "random.h"
#include "tcp_mgr.h"
#include "util.h"

#define OPTS "c:xt:"

#define IPADDR "127.0.0.1"
#define DEFAULT_PORT 1701

#define MAGIC 0x0D15EA5E

#define DEFAULT_TEST_TIME 10

static int testTime = DEFAULT_TEST_TIME;


REGISTRY_ENTRY_FILE_GENERATE;

struct message
{
    size_t msg_size;
    char   msg_buf[];
};

struct handshake
{
    uint32_t hs_magic;
    uint32_t hs_id;
};

struct owned_connection
{
    niova_atomic32_t          oc_ref_cnt;
    struct tcp_mgr_connection oc_tmc;
    LIST_ENTRY(owned_connection) oc_lentry;
};

struct tmt_thread
{
    struct thread_ctl tt_thread;
    LIST_ENTRY(tmt_thread) tt_lentry;
};

LIST_HEAD(owned_connection_list, owned_connection) oc_head =
    LIST_HEAD_INITIALIZER(oc_head);
pthread_mutex_t oc_head_mutex = PTHREAD_MUTEX_INITIALIZER;
int oc_count = 0;

LIST_HEAD(thread_list, tmt_thread) thread_list_head =
    LIST_HEAD_INITIALIZER(thread_list_head);

struct epoll_mgr epoll_mgr;
struct tcp_mgr_instance tcp_mgr;

static struct owned_connection *
owned_connection_new()
{
    struct owned_connection *oc =
        niova_malloc_can_fail(sizeof(struct owned_connection));
    if (!oc)
        return NULL;

    niova_mutex_lock(&oc_head_mutex);

    LIST_INSERT_HEAD(&oc_head, oc, oc_lentry);

    niova_atomic_init(&oc->oc_ref_cnt, 1);
    oc->oc_tmc.tmc_status = TMCS_NEEDS_SETUP;
    tcp_mgr_connection_setup(&tcp_mgr, &oc->oc_tmc);

    oc_count++;
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
    oc_count--;
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

static struct tmt_thread *
thread_new()
{
    struct tmt_thread *thread = niova_malloc_can_fail(sizeof(struct
                                                             tmt_thread));
    if (!thread)
        return NULL;

    LIST_INSERT_HEAD(&thread_list_head, thread, tt_lentry);

    return thread;
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

static struct owned_connection *
owned_connection_random_get()
{
    niova_mutex_lock(&oc_head_mutex);

    struct owned_connection *oc = LIST_FIRST(&oc_head);
    uint32_t idx = random_get() % oc_count;

    for (int i = 0; i < idx; i++)
        oc = LIST_NEXT(oc, oc_lentry);

    niova_mutex_unlock(&oc_head_mutex);

    return oc;
}

static void
close_cb(void *oc)
{
    owned_connection_getput(oc, EPH_REF_PUT);
}

static void *
close_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    THREAD_LOOP_WITH_CTL(tc)
    {
        sleep(2);

        struct owned_connection *oc = owned_connection_random_get();
        DBG_TCP_MGR_CXN(LL_NOTIFY, &oc->oc_tmc, "closing connection");
        tcp_mgr_connection_close_async(&oc->oc_tmc, close_cb, oc);
    }

    return NULL;
}

void threads_start()
{
    struct tmt_thread *thread;
    LIST_FOREACH(thread, &thread_list_head, tt_lentry)
    thread_ctl_run(&thread->tt_thread);
}

void threads_stop()
{
    struct tmt_thread *thread;
    LIST_FOREACH(thread, &thread_list_head, tt_lentry)
    thread_halt_and_destroy(&thread->tt_thread);
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
    if (buf_sz != sizeof(struct handshake))
        return -EINVAL;

    struct handshake *hs = (struct handshake *)buf;
    if (hs->hs_magic != MAGIC)
        return -EBADMSG;

    struct owned_connection *oc = owned_connection_new();
    if (!oc)
        return -ENOMEM;

    tcp_socket_handle_set_data(&oc->oc_tmc.tmc_tsh, IPADDR, hs->hs_id);

    *tmc_out = &oc->oc_tmc;
    tcp_mgr_connection_header_size_set(&oc->oc_tmc, sizeof(struct message));

    return 0;
}

static ssize_t
handshake_fill(void *_data, struct tcp_mgr_connection *tmc, void *buf, size_t
               buf_sz)
{
    if (buf_sz != sizeof(struct handshake))
        return -EINVAL;

    struct handshake *hs = (struct handshake *)buf;
    hs->hs_magic = MAGIC;

    // header size
    return sizeof(struct message);
}

void
test_tcp_mgr_setup(struct tcp_mgr_instance *tmi, struct epoll_mgr *epm, int
                   port)
{
    tcp_mgr_setup(tmi, tmi, owned_connection_getput, recv_cb, bulk_size_cb,
                  handshake_cb,
                  handshake_fill, sizeof(struct handshake));

    tcp_mgr_sockets_setup(tmi, IPADDR, port);
    tcp_mgr_sockets_bind(tmi);
    tcp_mgr_epoll_setup(tmi, epm);
}

void *event_loop_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct epoll_mgr *epm = tc->tc_arg;
    THREAD_LOOP_WITH_CTL(tc)
    {
        epoll_mgr_wait_and_process_events(epm, -1);
    }

    return NULL;
}

void connections_putall()
{
    // getput can take the lock, so don't lock here
    struct owned_connection *oc = LIST_FIRST(&oc_head);
    LIST_INIT(&oc_head);

    while (oc)
    {
        struct owned_connection *next = LIST_NEXT(oc, oc_lentry);

        owned_connection_getput(oc, EPH_REF_PUT);
        oc = next;
    }
}

static void
client_add(int port)
{
    struct owned_connection *oc = owned_connection_new();
    struct tmt_thread *thread = thread_new();
    NIOVA_ASSERT(oc && thread);

    tcp_socket_handle_set_data(&oc->oc_tmc.tmc_tsh, IPADDR, port);

    char name[16];
    snprintf(name, 16, "send-%d", port);
    int rc = thread_create(send_thread, &thread->tt_thread, name, oc, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);
}

static void
server_setup(int port)
{
    epoll_mgr_setup(&epoll_mgr);
    test_tcp_mgr_setup(&tcp_mgr, &epoll_mgr, port);
}

static void
close_thread_add()
{
    struct tmt_thread *thread = thread_new();

    int rc = thread_create(close_thread, &thread->tt_thread, "close", NULL,
                           NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);
}

static void
tcp_mgr_test_print_help(const int error)
{
    fprintf(error ? stderr : stdout,
            "tcp_mgr_test [-t run_time_seconds] [-c port] [-x]\n");

    exit(error);
}

static void
tcp_mgr_test_process_opts(int argc, char **argv)
{
    int opt;
    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 't':
            testTime = atoi(optarg);
            if (testTime < 1 || testTime > 3600)
                testTime = DEFAULT_TEST_TIME;
            break;
        case 'x':
            close_thread_add();
            break;
        case 'c':
            client_add(atoi(optarg));
            break;
        default:
            tcp_mgr_test_print_help(opt != 'h');
            break;
        }
    }

    int port = DEFAULT_PORT;
    if (optind < argc)
        port = atoi(optarg);

    server_setup(port);
}

static void
event_loop_thread_add()
{
    struct tmt_thread *thread = thread_new();

    int rc = thread_create(event_loop_thread, &thread->tt_thread, "event-loop",
                           &epoll_mgr, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);
}

int
main(int argc, char **argv)
{
    tcp_mgr_test_process_opts(argc, argv);
    event_loop_thread_add();

    threads_start();

    usleep(testTime * 1000 * 1000);
    SIMPLE_LOG_MSG(LL_NOTIFY, "ending");

    threads_stop();

    connections_putall();

    tcp_mgr_sockets_close(&tcp_mgr);

    SIMPLE_LOG_MSG(LL_NOTIFY, "main ended");
}

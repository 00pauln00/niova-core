#include <stdio.h>

#include "alloc.h"
#include "atomic.h"
#include "epoll_mgr.h"
#include "random.h"
#include "tcp_mgr.h"
#include "util.h"

#define OPTS "c:x:t:"

#define IPADDR "127.0.0.1"
#define DEFAULT_PORT 1701

#define MAGIC 0x0D15EA5E

#define MAX_TEST_TIME 3600
#define MAX_CLOSE_SLEEP_TIME 360000
#define DEFAULT_TEST_TIME 10
#define DEFAULT_CLOSE_SLEEP_TIME 2

static int testTime = DEFAULT_TEST_TIME;
static int closeSleepTime = DEFAULT_CLOSE_SLEEP_TIME;

REGISTRY_ENTRY_FILE_GENERATE;

struct tmt_message
{
    size_t msg_size;
    char   msg_buf[];
};

struct tmt_handshake
{
    uint32_t hs_magic;
    uint32_t hs_id;
};

struct tmt_owned_connection
{
    niova_atomic32_t          oc_ref_cnt;
    struct tcp_mgr_connection oc_tmc;
    struct tmt_data          *oc_tmt_data;
    LIST_ENTRY(tmt_owned_connection) oc_lentry;
};

struct tmt_thread
{
    struct thread_ctl tt_thread;
    LIST_ENTRY(tmt_thread) tt_lentry;
};

LIST_HEAD(tmt_owned_connection_list, tmt_owned_connection);
LIST_HEAD(tmt_thread_list, tmt_thread);
struct tmt_data
{
    struct epoll_mgr                 td_epoll_mgr;
    struct tcp_mgr_instance          td_tcp_mgr;

    pthread_mutex_t                  td_conn_list_mutex;
    struct tmt_owned_connection_list td_conn_list_head;
    int                              td_conn_count;

    struct tmt_thread_list           td_thread_list_head;
};

static struct tmt_owned_connection *
tmt_owned_connection_new(struct tmt_data *td)
{
    struct tmt_owned_connection *oc =
        niova_malloc_can_fail(sizeof(struct tmt_owned_connection));
    if (!oc)
        return NULL;

    niova_mutex_lock(&td->td_conn_list_mutex);

    LIST_INSERT_HEAD(&td->td_conn_list_head, oc, oc_lentry);

    oc->oc_tmt_data = td;
    niova_atomic_init(&oc->oc_ref_cnt, 1);
    oc->oc_tmc.tmc_status = TMCS_NEEDS_SETUP;
    tcp_mgr_connection_setup(&td->td_tcp_mgr, &oc->oc_tmc);

    td->td_conn_count++;
    niova_mutex_unlock(&td->td_conn_list_mutex);

    SIMPLE_LOG_MSG(LL_TRACE, "oc: %p", oc);

    return oc;
}

static void
tmt_owned_connection_fini(struct tmt_owned_connection *oc)
{
    SIMPLE_LOG_MSG(LL_TRACE, "oc: %p", oc);

    struct tmt_data *td = oc->oc_tmt_data;
    NIOVA_ASSERT(td);

    niova_mutex_lock(&td->td_conn_list_mutex);
    LIST_REMOVE(oc, oc_lentry);
    niova_free(oc);
    td->td_conn_count--;
    niova_mutex_unlock(&td->td_conn_list_mutex);
}

static void
tmt_owned_connection_getput(void *data, enum epoll_handle_ref_op op)
{
    struct tcp_mgr_connection *tmc = (struct tcp_mgr_connection *)data;
    struct tmt_owned_connection *oc = OFFSET_CAST(tmt_owned_connection, oc_tmc,
                                                  tmc);
    if (op == EPH_REF_GET)
    {
        niova_atomic_inc(&oc->oc_ref_cnt);
    }
    else
    {
        uint32_t refcnt = niova_atomic_dec(&oc->oc_ref_cnt);
        if (refcnt == 0)
            tmt_owned_connection_fini(oc);
    }
}

static struct tmt_thread *
tmt_thread_new(struct tmt_data *td)
{
    struct tmt_thread *thread = niova_malloc_can_fail(sizeof(struct
                                                             tmt_thread));
    if (!thread)
        return NULL;

    LIST_INSERT_HEAD(&td->td_thread_list_head, thread, tt_lentry);

    return thread;
}

#define BUF_SZ 255
static void *
tmt_send_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    NIOVA_ASSERT(tc && tc->tc_arg);

    static char buf[BUF_SZ];
    static struct iovec iov = {
        .iov_base = buf,
    };
    struct tmt_message *msg = (struct tmt_message *)buf;

    struct tmt_owned_connection *oc = tc->tc_arg;
    tmt_owned_connection_getput(oc, EPH_REF_GET);

    int msg_idx = 0;
    THREAD_LOOP_WITH_CTL(tc)
    {
        size_t max_msg = BUF_SZ - sizeof(struct tmt_message);
        int msg_size = snprintf(msg->msg_buf, max_msg,
                                "hello from [%s:%d], msg idx=%d",
                                oc->oc_tmc.tmc_tmi->tmi_listen_socket.tsh_ipaddr,
                                oc->oc_tmc.tmc_tmi->tmi_listen_socket.tsh_port,
                                msg_idx);
        msg->msg_size = msg_size;
        iov.iov_len = msg_size + sizeof(struct tmt_message);
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
    tmt_owned_connection_getput(oc, EPH_REF_PUT);

    return NULL;
}

static struct tmt_owned_connection *
tmt_owned_connection_random_get(struct tmt_data *td)
{
    niova_mutex_lock(&td->td_conn_list_mutex);

    struct tmt_owned_connection *oc = LIST_FIRST(&td->td_conn_list_head);
    uint32_t idx = random_get() % td->td_conn_count;

    for (int i = 0; i < idx; i++)
        oc = LIST_NEXT(oc, oc_lentry);

    niova_mutex_unlock(&td->td_conn_list_mutex);

    return oc;
}

static void
tmt_close_done_cb(void *oc)
{
    tmt_owned_connection_getput(oc, EPH_REF_PUT);
}

static void *
tmt_close_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct tmt_data *td = tc->tc_arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        usleep(closeSleepTime * 1000);

        struct tmt_owned_connection *oc = tmt_owned_connection_random_get(td);
        DBG_TCP_MGR_CXN(LL_NOTIFY, &oc->oc_tmc, "closing connection");
        tcp_mgr_connection_close_async(&oc->oc_tmc, tmt_close_done_cb, oc);
    }

    return NULL;
}

void tmt_threads_start(struct tmt_data *td)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tmt_thread *thread;
    LIST_FOREACH(thread, &td->td_thread_list_head, tt_lentry)
    {
        thread_ctl_run(&thread->tt_thread);
    }
}

void tmt_threads_stop(struct tmt_data *td)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tmt_thread *thread;
    LIST_FOREACH(thread, &td->td_thread_list_head, tt_lentry)
    {
        thread_ctl_halt(&thread->tt_thread);
    }
}

static int
tmt_recv_cb(struct tcp_mgr_connection *tmc, char *buf, size_t buf_size,
            void *_data)
{
    struct tmt_message *msg = (struct tmt_message *)buf;

    DBG_TCP_MGR_CXN(LL_NOTIFY, tmc,
                    "[%s:%d] received_message[tmc=%p]: size=%lu str=%s\n",
                    tmc->tmc_tmi->tmi_listen_socket.tsh_ipaddr,
                    tmc->tmc_tmi->tmi_listen_socket.tsh_port,
                    tmc, buf_size, msg->msg_buf);

    return 0;
}

static ssize_t
tmt_bulk_size_cb(struct tcp_mgr_connection *tmc, char *header, void *_data)
{
    struct tmt_message *msg = (struct tmt_message *)header;

    SIMPLE_LOG_MSG(LL_TRACE, "msg_size: %lu", msg->msg_size);

    return msg->msg_size;
}

// XXX data should go at the end of fn signature
static int
tmt_handshake_cb(void *tmt_data, struct tcp_mgr_connection **tmc_out, int fd,
                 void *buf, size_t buf_sz)
{
    if (buf_sz != sizeof(struct tmt_handshake))
        return -EINVAL;

    struct tmt_handshake *hs = (struct tmt_handshake *)buf;
    if (hs->hs_magic != MAGIC)
        return -EBADMSG;

    struct tmt_owned_connection *oc = tmt_owned_connection_new(tmt_data);
    if (!oc)
        return -ENOMEM;

    tcp_socket_handle_set_data(&oc->oc_tmc.tmc_tsh, IPADDR, hs->hs_id);

    *tmc_out = &oc->oc_tmc;
    tcp_mgr_connection_header_size_set(&oc->oc_tmc, sizeof(struct tmt_message));

    return 0;
}

static ssize_t
tmt_handshake_fill(void *_data, struct tcp_mgr_connection *tmc, void *buf,
                   size_t
                   buf_sz)
{
    if (buf_sz != sizeof(struct tmt_handshake))
        return -EINVAL;

    struct tmt_handshake *hs = (struct tmt_handshake *)buf;
    hs->hs_magic = MAGIC;

    // header size
    return sizeof(struct tmt_message);
}

void
tmt_bind(struct tmt_data *data, int port)
{
    tcp_mgr_sockets_setup(&data->td_tcp_mgr, IPADDR, port);
    tcp_mgr_sockets_bind(&data->td_tcp_mgr);
    tcp_mgr_epoll_setup(&data->td_tcp_mgr, &data->td_epoll_mgr);

    printf("added server on port %d\n", port);
}

void *tmt_event_loop_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct epoll_mgr *epm = tc->tc_arg;
    THREAD_LOOP_WITH_CTL(tc)
    {
        epoll_mgr_wait_and_process_events(epm, -1);
    }

    return NULL;
}

void tmt_connections_putall(struct tmt_data *td)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    // getput can take the lock, so don't lock here
    struct tmt_owned_connection *oc = LIST_FIRST(&td->td_conn_list_head);
    LIST_INIT(&td->td_conn_list_head);

    while (oc)
    {
        struct tmt_owned_connection *next = LIST_NEXT(oc, oc_lentry);

        tmt_owned_connection_getput(oc, EPH_REF_PUT);
        oc = next;
    }
}

static void
tmt_client_add(struct tmt_data *td, int port)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tmt_owned_connection *oc = tmt_owned_connection_new(td);
    struct tmt_thread *thread = tmt_thread_new(td);
    NIOVA_ASSERT(oc && thread);

    tcp_socket_handle_set_data(&oc->oc_tmc.tmc_tsh, IPADDR, port);

    char name[16];
    snprintf(name, 16, "send-%d", port);
    int rc = thread_create(tmt_send_thread, &thread->tt_thread, name, oc, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);

    printf("added client, port %d\n", port);
}

static void
tmt_close_thread_add(struct tmt_data *td)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    struct tmt_thread *thread = tmt_thread_new(td);

    int rc = thread_create(tmt_close_thread, &thread->tt_thread, "close", td,
                           NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);

    printf("added connection closing thread\n");
}

static void
tmt_print_help(const int error)
{
    fprintf(error ? stderr : stdout,
            "tcp_mgr_test [-t run_time_sec] [-c port] [-x close_time_msec]\n");

    exit(error);
}

static void
tmt_process_opts(struct tmt_data *td, int argc, char **argv)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    int opt;
    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        SIMPLE_LOG_MSG(LL_DEBUG, "got opt %c", opt);
        switch (opt)
        {
        case 't':
            testTime = atoi(optarg);
            if (testTime < 1 || testTime > MAX_TEST_TIME)
                testTime = DEFAULT_TEST_TIME;
            break;
        case 'x':
            closeSleepTime = atoi(optarg);
            if (closeSleepTime < 1 || closeSleepTime > MAX_CLOSE_SLEEP_TIME)
                closeSleepTime = DEFAULT_CLOSE_SLEEP_TIME;
            tmt_close_thread_add(td);
            break;
        case 'c':
            tmt_client_add(td, atoi(optarg));
            break;
        default:
            tmt_print_help(opt != 'h');
            break;
        }
    }

    int port = DEFAULT_PORT;
    if (optind < argc)
        port = atoi(optarg);

    SIMPLE_LOG_MSG(LL_DEBUG, "binding to port %d", port);
    tmt_bind(td, port);
}

static void
tmt_event_loop_thread_add(struct tmt_data *td)
{
    struct tmt_thread *thread = tmt_thread_new(td);

    int rc = thread_create(tmt_event_loop_thread, &thread->tt_thread,
                           "event-loop",
                           &td->td_epoll_mgr, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);
}

static void
tmt_setup(struct tmt_data *td)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    int rc = epoll_mgr_setup(&td->td_epoll_mgr);
    NIOVA_ASSERT(!rc);

    tcp_mgr_setup(&td->td_tcp_mgr, td, tmt_owned_connection_getput,
                  tmt_recv_cb,
                  tmt_bulk_size_cb,
                  tmt_handshake_cb,
                  tmt_handshake_fill, sizeof(struct tmt_handshake));

    rc = pthread_mutex_init(&td->td_conn_list_mutex, NULL);
    NIOVA_ASSERT(!rc);

    LIST_INIT(&td->td_conn_list_head);
    LIST_INIT(&td->td_thread_list_head);
}

int
main(int argc, char **argv)
{
    struct tmt_data tmt_data;
    tmt_setup(&tmt_data);

    tmt_process_opts(&tmt_data, argc, argv);
    tmt_event_loop_thread_add(&tmt_data);

    tmt_threads_start(&tmt_data);

    printf("running for %d seconds...\n", testTime);
    usleep(testTime * 1000 * 1000);
    SIMPLE_LOG_MSG(LL_NOTIFY, "ending");

    tmt_threads_stop(&tmt_data);

    tmt_connections_putall(&tmt_data);

    tcp_mgr_sockets_close(&tmt_data.td_tcp_mgr);

    SIMPLE_LOG_MSG(LL_NOTIFY, "main ended");
}

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

#define BULK_CREDITS 1
#define INCOMING_CREDITS 1

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
    int                       oc_port;
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

    niova_atomic32_t                 td_recv_cnt;
    niova_atomic32_t                 td_send_cnt;
    niova_atomic32_t                 td_send_err_cnt;
    niova_atomic32_t                 td_close_cnt;
};

static struct tmt_owned_connection *
tmt_tcp_mgr_2_owned_connection(struct tcp_mgr_connection *tmc)
{
    return OFFSET_CAST(tmt_owned_connection, oc_tmc, tmc);
}

static struct tmt_owned_connection *
tmt_owned_connection_new(struct tmt_data *td, int port)
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
    tcp_mgr_connection_setup(&oc->oc_tmc, &td->td_tcp_mgr, IPADDR, port);

    td->td_conn_count++;
    niova_mutex_unlock(&td->td_conn_list_mutex);

    SIMPLE_LOG_MSG(LL_TRACE, "oc: %p port: %d", oc, port);

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
tmt_owned_connection_getput(struct tmt_owned_connection *oc,
                            enum epoll_handle_ref_op op)
{
    uint32_t refcnt = niova_atomic_read(&oc->oc_ref_cnt);
    NIOVA_ASSERT(refcnt > 0);

    if (op == EPH_REF_GET)
    {
        refcnt = niova_atomic_inc(&oc->oc_ref_cnt);
    }
    else
    {
        refcnt = niova_atomic_dec(&oc->oc_ref_cnt);
        if (refcnt == 0)
            tmt_owned_connection_fini(oc);
    }
    LOG_MSG(LL_DEBUG, "oc: %p, [newref %d] %s", oc, refcnt,
                   op == EPH_REF_GET ? "GET" : "PUT");
}

static void
tmt_tcp_mgr_owned_connection_getput_cb(void *data, enum epoll_handle_ref_op op)
{
    SIMPLE_LOG_MSG(LL_TRACE, "data %p op %s", data,
                   op == EPH_REF_GET ? "GET" : "PUT");
    NIOVA_ASSERT(data);

    struct tcp_mgr_connection *tmc = (struct tcp_mgr_connection *)data;
    struct tmt_owned_connection *oc = tmt_tcp_mgr_2_owned_connection(tmc);

    tmt_owned_connection_getput(oc, op);
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

    struct tcp_mgr_instance *tmi = &oc->oc_tmt_data->td_tcp_mgr;

    int msg_idx = 0;
    THREAD_LOOP_WITH_CTL(tc)
    {
        size_t max_msg = BUF_SZ - sizeof(struct tmt_message);
        int msg_size = snprintf(msg->msg_buf, max_msg,
                                "hello from [%s:%d], msg idx=%d",
                                tmi->tmi_listen_socket.tsh_ipaddr,
                                tmi->tmi_listen_socket.tsh_port,
                                msg_idx);
        msg->msg_size = msg_size;
        iov.iov_len = msg_size + sizeof(struct tmt_message);
        SIMPLE_LOG_MSG(LL_NOTIFY, "sending message, msg_size=%d", msg_size);
        int rc = tcp_mgr_send_msg(&oc->oc_tmc, &iov, 1);
        if (rc < 0)
        {
            SIMPLE_LOG_MSG(LL_NOTIFY, "error sending message, rc=%d", rc);
            niova_atomic_inc(&oc->oc_tmt_data->td_send_err_cnt);
        }
        else
        {
            SIMPLE_LOG_MSG(LL_NOTIFY, "sent message, msg_idx=%d", msg_idx);
            msg_idx++;
            niova_atomic_inc(&oc->oc_tmt_data->td_send_cnt);
        }

        usleep(100 * 1000);
    }
    tcp_mgr_connection_close(&oc->oc_tmc);
    tmt_owned_connection_getput(oc, EPH_REF_PUT);

    SIMPLE_FUNC_EXIT(LL_TRACE);

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

    tmt_owned_connection_getput(oc, EPH_REF_GET);
    return oc;
}

static void *
tmt_close_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct tmt_data *td = tc->tc_arg;

    usleep(closeSleepTime * 1000);
    THREAD_LOOP_WITH_CTL(tc)
    {
        struct tmt_owned_connection *oc = tmt_owned_connection_random_get(td);
        DBG_TCP_MGR_CXN(LL_NOTIFY, &oc->oc_tmc, "closing connection");
        tcp_mgr_connection_close(&oc->oc_tmc);
        tmt_owned_connection_getput(oc, EPH_REF_PUT);
        niova_atomic_inc(&td->td_close_cnt);

        // put it at the end so the test happens after sleep
        usleep(closeSleepTime * 1000);
    }

    SIMPLE_FUNC_EXIT(LL_TRACE);

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

    thread = LIST_FIRST(&td->td_thread_list_head);
    while(thread != NULL)
    {
        thread_creator_wait_until_ctl_loop_reached(&thread->tt_thread);
        thread_halt_and_destroy(&thread->tt_thread);

        struct tmt_thread *tmp = LIST_NEXT(thread, tt_lentry);
        niova_free(thread);
        thread = tmp;
    }
}

static int
tmt_recv_cb(struct tcp_mgr_connection *tmc, char *buf, size_t buf_size,
            void *_data)
{
    struct tmt_message *msg = (struct tmt_message *)buf;

    DBG_TCP_MGR_CXN(LL_NOTIFY, tmc,
                    "[%s:%d] received_message[tmc=%p]: size=%lu str=%.*s\n",
                    tmc->tmc_tmi->tmi_listen_socket.tsh_ipaddr,
                    tmc->tmc_tmi->tmi_listen_socket.tsh_port,
                    tmc, buf_size, (int)buf_size, msg->msg_buf);

    struct tmt_owned_connection *oc = tmt_tcp_mgr_2_owned_connection(tmc);
    niova_atomic_inc(&oc->oc_tmt_data->td_recv_cnt);

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
tmt_handshake_cb(void *tmt_data, struct tcp_mgr_connection **tmc_out,
                 size_t *header_size_out, int fd, void *buf, size_t buf_sz)
{
    if (buf_sz != sizeof(struct tmt_handshake))
        return -EINVAL;

    struct tmt_handshake *hs = (struct tmt_handshake *)buf;
    if (hs->hs_magic != MAGIC)
        return -EBADMSG;

    struct tmt_owned_connection *oc = tmt_owned_connection_new(tmt_data,
                                                               hs->hs_id);
    if (!oc)
        return -ENOMEM;

    tmt_owned_connection_getput(oc, EPH_REF_GET);

    *tmc_out = &oc->oc_tmc;
    *header_size_out = sizeof(struct tmt_message);

    SIMPLE_LOG_MSG(LL_TRACE, "%s:%d", IPADDR, oc->oc_port);

    return 0;
}

static ssize_t
tmt_handshake_fill(void *_data, struct tcp_mgr_connection *tmc, void *buf,
                   size_t buf_sz)
{
    if (buf_sz != sizeof(struct tmt_handshake))
        return -EINVAL;

    struct tmt_handshake *hs = (struct tmt_handshake *)buf;
    hs->hs_magic = MAGIC;

    hs->hs_id = tmc->tmc_tmi->tmi_listen_socket.tsh_port;
    SIMPLE_LOG_MSG(LL_TRACE, "port: %d", hs->hs_id);

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

    while (epoll_mgr_close(epm))
        epoll_mgr_wait_and_process_events(epm, -1);

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

    static int send_id = 0;

    struct tmt_owned_connection *oc = tmt_owned_connection_new(td, port);
    struct tmt_thread *thread = tmt_thread_new(td);
    NIOVA_ASSERT(oc && thread);

    char name[16];
    snprintf(name, 16, "send-%d-%d", send_id, port);
    int rc = thread_create(tmt_send_thread, &thread->tt_thread, name, oc, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "thread_create(): rc=%d", rc);

    send_id++;

    printf("added client, port %d\n", port);
}

static void
tmt_close_thread_add(struct tmt_data *td)
{
    SIMPLE_FUNC_ENTRY(LL_TRACE);

    static bool close_thread_started = false;
    if (close_thread_started)
        return;
    close_thread_started = true;

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
        port = atoi(argv[optind]);

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

    rc = tcp_mgr_setup(&td->td_tcp_mgr, td,
                       tmt_tcp_mgr_owned_connection_getput_cb,
                       tmt_recv_cb,
                       tmt_bulk_size_cb,
                       tmt_handshake_cb,
                       tmt_handshake_fill, sizeof(struct tmt_handshake),
                       BULK_CREDITS, INCOMING_CREDITS, false);
    NIOVA_ASSERT(!rc);

    rc = pthread_mutex_init(&td->td_conn_list_mutex, NULL);
    NIOVA_ASSERT(!rc);

    td->td_conn_count = 0;
    LIST_INIT(&td->td_conn_list_head);
    LIST_INIT(&td->td_thread_list_head);

    niova_atomic_init(&td->td_recv_cnt, 0);
    niova_atomic_init(&td->td_send_cnt, 0);
    niova_atomic_init(&td->td_send_err_cnt, 0);
    niova_atomic_init(&td->td_close_cnt, 0);
}

static void
tmt_print_metrics(struct tmt_data *td)
{
    uint32_t sent = niova_atomic_read(&td->td_send_cnt);
    uint32_t sent_errs = niova_atomic_read(&td->td_send_err_cnt);
    uint32_t received = niova_atomic_read(&td->td_recv_cnt);
    uint32_t closed = niova_atomic_read(&td->td_close_cnt);

    printf("sent %d (%d errs)\n", sent, sent_errs);
    printf("received %d\n", received);
    printf("closed %d\n", closed);
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
    SIMPLE_LOG_MSG(LL_NOTIFY, "stopping");
    printf("stopping\n");

    tcp_mgr_sockets_close(&tmt_data.td_tcp_mgr);

    tmt_threads_stop(&tmt_data);

    tmt_connections_putall(&tmt_data);

    SIMPLE_LOG_MSG(LL_NOTIFY, "main ended");

    tmt_print_metrics(&tmt_data);
}

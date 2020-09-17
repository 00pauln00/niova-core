/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <poll.h>

#include "log.h"
#include "tcp.h"

static niova_atomic64_t numPingPongsDone;

#define OPTS "ht:s:"

#define DEFAULT_RUN_TIME 10;
#define DEFAULT_TCP_SIZE 1024*1024;

static size_t tcpSize = DEFAULT_TCP_SIZE;
static size_t runTime = DEFAULT_RUN_TIME;

/**
 * tcp_test_basic - sets up a TCP socket, binds, and does a non-blocking read.
 *    The function simply tests essential operations - it does not exchange
 *    data between sockets.
 */
static int
tcp_test_basic(void)
{
    struct tcp_socket_handle tsh = {
        .tsh_port = tcp_get_default_port(),
        .tsh_ipaddr = "0.0.0.0",
        .tsh_socket = -1,
    };

    int rc = tcp_socket_setup(&tsh);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = tcp_socket_bind(&tsh);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_bind(): %s", strerror(-rc));
        return rc;
    }

    rc = tcp_socket_close(&tsh);

    return rc;
}
static int
tcp_test_connect_helper(struct tcp_socket_handle *tsh_listen,
                        struct tcp_socket_handle *tsh_connect,
                        struct tcp_socket_handle *tsh_accept)
{
    int rc = tcp_socket_setup(tsh_listen);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = tcp_socket_bind(tsh_listen);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_bind(): %s", strerror(-rc));
        return rc;
    }

    // wait for partner thread to listen
    usleep(10000);

    rc = tcp_socket_setup(tsh_connect);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = tcp_socket_connect(tsh_connect);
    if (rc && rc != -EINPROGRESS)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_connect(): %s", strerror(-rc));
        return rc;
    }

    struct pollfd pfd = {
        .fd = tsh_listen->tsh_socket,
        .events = POLLIN | POLLOUT
    };

    rc = poll(&pfd, 1, -1);
    if (rc < 0)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "poll(): %s",
                       strerror(errno));
        return rc;
    }

    rc = tcp_socket_handle_accept(tsh_listen->tsh_socket, tsh_accept);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_handle_accept(): %s",
                       strerror(-rc));
        return rc;
    }

    return 0;
}

static int
tcp_test_connect() 
{
    struct tcp_socket_handle listener = {
        .tsh_port = tcp_get_default_port(),
        .tsh_ipaddr = "127.0.0.1",
        .tsh_socket = -1,
    };
    struct tcp_socket_handle connector = listener;
    struct tcp_socket_handle accepted;

    int rc = tcp_test_connect_helper(&listener, &connector, &accepted);

    tcp_socket_close(&accepted);
    tcp_socket_close(&connector);
    tcp_socket_close(&listener);

    return rc;
}

static void *
tcp_test_pingpong_worker(void *arg)
{
    NIOVA_ASSERT(arg);
    struct thread_ctl *tc = arg;

    NIOVA_ASSERT(tc->tc_arg);
    struct tcp_socket_handle *tsh_listen = tc->tc_arg;

    NIOVA_ASSERT(tsh_listen->tsh_port == tcp_get_default_port() ||
                 tsh_listen->tsh_port == tcp_get_default_port() + 1);

    bool senior_thread = tsh_listen->tsh_port == tcp_get_default_port();
    struct tcp_socket_handle tsh_outgoing = {
        .tsh_port   = (senior_thread ?
                       (tcp_get_default_port() + 1) : tcp_get_default_port()),
        .tsh_ipaddr = "127.0.0.1",
        .tsh_socket = -1,
    };
    struct tcp_socket_handle tsh_incoming;

    int rc = tcp_test_connect_helper(tsh_listen, &tsh_outgoing, &tsh_incoming);
    if (rc)
    {
        tc->tc_ret = rc;
        return NULL;
    }

    char buf[tcpSize];
    struct iovec iov = { .iov_len = tcpSize, .iov_base = buf };
    STDERR_MSG("tcp_test_pingpong(): iov_len %lu", tcpSize);

    THREAD_LOOP_WITH_CTL(tc)
    {
        ssize_t size_rc;

        if (senior_thread)
        {
            size_rc = tcp_socket_send(&tsh_outgoing, &iov, 1);
            if (size_rc != tcpSize)
            {
                SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_send(): %zd",
                               size_rc);
                continue;
            }

            size_rc = tcp_socket_recv(&tsh_incoming, &iov, 1, NULL, true);
            if (!size_rc) {
                // remote connection closed
                break;
            }

            if (size_rc != tcpSize)
            {
                SIMPLE_LOG_MSG((size_rc != -EINTR ? LL_ERROR : LL_DEBUG),
                               "tcp_socket_recv(): %zd", size_rc);
                continue;
            }

            niova_atomic_inc(&numPingPongsDone);
        }
        else
        {
            size_rc = tcp_socket_recv(&tsh_incoming, &iov, 1, NULL, true);
            if (!size_rc) {
                break;
            }
            if (size_rc != tcpSize)
            {
                SIMPLE_LOG_MSG((size_rc != -EINTR ? LL_ERROR : LL_DEBUG),
                               "tcp_socket_recv(): %zd", size_rc);
                continue;
            }

            size_rc = tcp_socket_send(&tsh_outgoing, &iov, 1);
            if (size_rc != tcpSize)
            {
                SIMPLE_LOG_MSG(LL_ERROR, "tcp_socket_send(): %zd",
                               size_rc);
                continue;
            }
        }
    }

    tcp_socket_close(&tsh_incoming);
    tcp_socket_close(&tsh_outgoing);
    tcp_socket_close(tsh_listen);

    return NULL;
}

static int
tcp_test_pingpong(void)
{
#define NTHREADS 2
    struct thread_ctl tc[NTHREADS] = {0};
    struct tcp_socket_handle tsh[NTHREADS];

    for (int i = 0; i < NTHREADS; i++)
    {
        char name[32];
        snprintf(name, 32, "tcp_pingpong-%d", i);

        tsh[i].tsh_port = tcp_get_default_port() + i;
        tsh[i].tsh_socket = -1;
        strncpy(tsh[i].tsh_ipaddr, "127.0.0.1", 16);

        int rc =
            thread_create(tcp_test_pingpong_worker, &tc[i], name, &tsh[i],
                          NULL);

        NIOVA_ASSERT(!rc);
    }

    for (int i = 0; i < NTHREADS; i++)
        thread_ctl_run(&tc[i]);

    sleep(runTime);

    int err[NTHREADS];

    for (int i = 0; i < NTHREADS; i++)
        err[i] = thread_halt_and_destroy(&tc[i]);

    for (int i = 0; i < NTHREADS; i++)
        if (err[i])
            return err[i];

    STDOUT_MSG("IOPs=%zu KiBs=%zu", (size_t)(numPingPongsDone / runTime),
               (size_t)(numPingPongsDone / runTime * tcpSize) >> 10);

    return 0;
}

static void
tcp_test_print_help(const int error)
{
    fprintf(error ? stderr : stdout,
            "tcp_test [-t run_time_seconds] [-s tcp_msg_size]\n");

    exit(error);
}

static void
tcp_test_getopt(int argc, char **argv)
{
    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 't':
            runTime = atoll(optarg);
            if (runTime < 1 || runTime > 3600)
                runTime = DEFAULT_RUN_TIME;
            break;
        case 's':
            tcpSize = atoll(optarg);
            if (tcpSize < 1 || tcpSize > NIOVA_MAX_TCP_SIZE)
                tcpSize = DEFAULT_TCP_SIZE;
            break;
        case 'h':
            tcp_test_print_help(0);
            break;
        default:
            tcp_test_print_help(EINVAL);
            break;
        }
    }
}

int
main(int argc, char **argv)
{
    tcp_test_getopt(argc, argv);

    int rc = tcp_test_basic();

    STDERR_MSG("tcp_test_basic(): %s", rc ? strerror(-rc) : "OK");
    if (rc)
        return rc;

    rc = tcp_test_connect();

    STDERR_MSG("tcp_test_connect(): %s", rc ? strerror(-rc) : "OK");
    if (rc)
        return rc;

    rc = tcp_test_pingpong();

    STDERR_MSG("tcp_test_pingpong(): %s", rc ? strerror(-rc) : "OK");
    if (rc)
        return rc;

    return 0;
}

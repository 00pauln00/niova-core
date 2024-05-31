/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include "niova_backtrace.h"

#include "log.h"
#include "udp.h"

static niova_atomic64_t numPingPongsDone;

#define OPTS "ht:s:"

static ssize_t udpSize = 5000;
static size_t runTime = 1;

/**
 * udp_test_basic - sets up a UDP socket, binds, and does a non-blocking read.
 *    The function simply tests essential operations - it does not exchange
 *    data between sockets.
 */
static int
udp_test_basic(void)
{
    struct udp_socket_handle ush = {
        .ush_port = udp_get_default_port(),
        .ush_ipaddr = "0.0.0.0",
        .ush_socket = -1,
    };

    int rc = udp_socket_setup(&ush);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "udp_socket_setup(): %s", strerror(-rc));
        return rc;
    }

    rc = udp_socket_bind(&ush);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "udp_socket_bind(): %s", strerror(-rc));
        return rc;
    }

    char buf[udpSize];
    struct iovec iov = { .iov_len = udpSize, .iov_base = buf };

    ssize_t recv_rc = udp_socket_recv(&ush, &iov, 1, NULL, false);
    if (recv_rc == -EAGAIN || recv_rc == -EWOULDBLOCK)
        recv_rc = 0;

    if (recv_rc)
        SIMPLE_LOG_MSG(LL_ERROR, "udp_socket_recv(): %s", strerror(-recv_rc));

    rc = udp_socket_close(&ush);

    return recv_rc ? (int)recv_rc : rc;
}

static void *
udp_test_pingpong_worker(void *arg)
{
    NIOVA_ASSERT(arg);
    struct thread_ctl *tc = arg;

    NIOVA_ASSERT(tc->tc_arg);
    struct udp_socket_handle *my_ush = tc->tc_arg;

    NIOVA_ASSERT(my_ush->ush_port == udp_get_default_port() ||
                 my_ush->ush_port == udp_get_default_port() + 1);

    struct udp_socket_handle rem_ush = {
        .ush_port   = (my_ush->ush_port == udp_get_default_port() ?
                       (udp_get_default_port() + 1) : udp_get_default_port()),
        .ush_ipaddr = "127.0.0.1",
        .ush_socket = -1,
    };

    int rc = udp_socket_setup(my_ush);
    if (rc)
    {
        tc->tc_ret = rc;
        return NULL;
    }

    rc = udp_socket_bind(my_ush);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "udp_socket_bind(): %s", strerror(-rc));
        tc->tc_ret = rc;
        return NULL;
    }

    struct sockaddr_in dest;
    rc = udp_setup_sockaddr_in(rem_ush.ush_ipaddr, rem_ush.ush_port,
                               &dest);
    NIOVA_ASSERT(!rc);

    bool senior_thread = my_ush->ush_port == udp_get_default_port() ?
        true : false;

    char buf[udpSize];
    struct iovec iov = { .iov_len = udpSize, .iov_base = buf };

    THREAD_LOOP_WITH_CTL(tc)
    {
        ssize_t size_rc;
        struct sockaddr_in from = {0};

        if (senior_thread)
        {
            size_rc = udp_socket_send(my_ush, &iov, 1, &dest);
            if (size_rc != udpSize)
            {
                SIMPLE_LOG_MSG(LL_ERROR, "udp_socket_send(): %zd",
                               size_rc);
                continue;
            }

            size_rc = udp_socket_recv(my_ush, &iov, 1, &from, true);
            if (size_rc != udpSize)
            {
                SIMPLE_LOG_MSG((size_rc != -EINTR ? LL_ERROR : LL_DEBUG),
                               "udp_socket_recv(): %zd", size_rc);
                continue;
            }

            niova_atomic_inc(&numPingPongsDone);
        }
        else
        {
            size_rc = udp_socket_recv(my_ush, &iov, 1, &from, true);
            if (size_rc != udpSize)
            {
                SIMPLE_LOG_MSG((size_rc != -EINTR ? LL_ERROR : LL_DEBUG),
                               "udp_socket_recv(): %zd", size_rc);
                continue;
            }

            size_rc = udp_socket_send(my_ush, &iov, 1, &dest);
            if (size_rc != udpSize)
            {
                SIMPLE_LOG_MSG(LL_ERROR, "udp_socket_send(): %zd",
                               size_rc);
                continue;
            }
        }
    }

    udp_socket_close(my_ush);

    return NULL;
}

static int
udp_test_pingpong(void)
{
#define NTHREADS 2
    struct thread_ctl tc[NTHREADS] = {0};
    struct udp_socket_handle ush[NTHREADS];

    for (int i = 0; i < NTHREADS; i++)
    {
        char name[32];
        snprintf(name, 32, "udp_pingpong-%d", i);

        ush[i].ush_port = udp_get_default_port() + i;
        ush[i].ush_socket = -1;
        strncpy(ush[i].ush_ipaddr, "127.0.0.1", 16);

        int rc =
            thread_create(udp_test_pingpong_worker, &tc[i], name, &ush[i],
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
               (size_t)(numPingPongsDone / runTime * udpSize) >> 10);

    return 0;
}

static void
udp_test_print_help(const int error)
{
    fprintf(error ? stderr : stdout,
            "udp_test [-t run_time_seconds] [-s udp_msg_size]\n");

    exit(error);
}

static void
udp_test_getopt(int argc, char **argv)
{
    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 't':
            runTime = atoll(optarg);
            if (runTime < 1 || runTime > 3600)
                runTime = 1;
            break;
        case 's':
            udpSize = atoll(optarg);
            if (udpSize < 1 || udpSize > NIOVA_MAX_UDP_SIZE)
                udpSize = 1000;
            break;
        case 'h':
            udp_test_print_help(0);
            break;
        default:
            udp_test_print_help(EINVAL);
            break;
        }
    }
}

int
main(int argc, char **argv)
{
    udp_test_getopt(argc, argv);

    int rc = udp_test_basic();

    STDERR_MSG("udp_test_basic(): %s", rc ? strerror(-rc) : "OK");
    if (rc)
        return rc;

    rc = udp_test_pingpong();

    STDERR_MSG("udp_test_pingpong(): %s", rc ? strerror(-rc) : "OK");
    if (rc)
        return rc;

    return 0;
}

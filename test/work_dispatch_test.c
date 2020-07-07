/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE         /* See feature_test_macros(7) */

#include "common.h"
#include "log.h"
#include "thread.h"
#include "util.h"

#include <pthread.h>
#include <unistd.h>
#include <semaphore.h>
#include <signal.h>
#include <sys/epoll.h>
#include <fcntl.h>

#define NUM_SECONDS_TO_RUN_TEST 2ULL

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

sem_t sem;

size_t num_wakeups;

int pipe_fds[2];
bool pipe_is_closed;

int epoll_pipe_0[2];
int epoll_pipe_1[2];

struct thread_ctl tc[3];

niova_atomic64_t waiters;

static void *
pthread_cond_worker(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        niova_atomic_inc(&waiters);
        NIOVA_WAIT_COND((niova_atomic_read(&waiters) != 0), &mutex, &cond);

        num_wakeups++;
    }

    return NULL;
}

static void *
pthread_cond_dispatcher(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        if (niova_atomic_read(&waiters))
        {
            NIOVA_SET_COND_AND_WAKE(
                signal,
                { NIOVA_ASSERT(niova_atomic_dec(&waiters) >= 0); },
                &mutex, &cond);
        }
    }

    return NULL;
}

static void *
sem_worker(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        NIOVA_ASSERT(niova_atomic_inc(&waiters) >= 0);
        sem_wait(&sem);

        num_wakeups++;
    }

    return NULL;
}

static void *
sem_dispatcher(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        if (niova_atomic_read(&waiters))
        {
            sem_post(&sem);
            NIOVA_ASSERT(niova_atomic_dec(&waiters) >= 0);
        }
    }

    return NULL;
}

static void
pthread_cond_test(void)
{
    num_wakeups = 0;
    niova_atomic_init(&waiters, 0);

    thread_create(pthread_cond_worker, &tc[0], "pthread_cond_worker",
                  NULL, NULL);
    thread_ctl_run(&tc[0]);

    thread_create(pthread_cond_dispatcher, &tc[1], "pthread_cond_dispatcher",
                  NULL, NULL);
    thread_ctl_run(&tc[1]);

    for (int i = 0; i < NUM_SECONDS_TO_RUN_TEST; i++)
        sleep(1);

    thread_halt_and_destroy(&tc[0]);
    thread_halt_and_destroy(&tc[1]);

    fprintf(stdout, "%.02f\t\t%s\n",
            (NUM_SECONDS_TO_RUN_TEST * 1000000000.00 / num_wakeups), __func__);
}

static void
sem_test(void)
{
    num_wakeups = 0;
    niova_atomic_init(&waiters, 0);

    sem_init(&sem, 0, 0);

    thread_create(sem_worker, &tc[0], "sem_worker",
                  NULL, NULL);
    thread_ctl_run(&tc[0]);

    thread_create(sem_dispatcher, &tc[1], "sem_dispatcher",
                  NULL, NULL);
    thread_ctl_run(&tc[1]);

    for (int i = 0; i < NUM_SECONDS_TO_RUN_TEST; i++)
        sleep(1);

    thread_halt_and_destroy(&tc[0]);
    thread_halt_and_destroy(&tc[1]);

    fprintf(stdout, "%.02f\t\t%s\n",
            (NUM_SECONDS_TO_RUN_TEST * 1000000000.00 / num_wakeups), __func__);

}

static void *
pipe_worker(void *arg)
{
    struct thread_ctl *tc = arg;
    char c;

    THREAD_LOOP_WITH_CTL(tc)
    {
        ssize_t rc = read(pipe_fds[0], &c, 1);
        if (rc != 1)
        {
            int save_errno = errno;
            if (!pipe_is_closed)
                STDERR_MSG("read() returned %zd:  %s", rc,
                           strerror(save_errno));
            break;
        }

        num_wakeups++;
    }

    return NULL;
}

static void *
pipe_dispatcher(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        unsigned char c = 0;
        ssize_t rc = write(pipe_fds[1], &c, 1);
        if (rc != 1)
        {
            if (!pipe_is_closed)
                STDERR_MSG("write() returned %zd:  %s", rc, strerror(errno));
            break;
        }
    }

    return NULL;
}

static void
pipe_test(void)
{
    num_wakeups = 0;

    signal(SIGPIPE, SIG_IGN);

    NIOVA_ASSERT(!pipe(pipe_fds));

    thread_create(pipe_dispatcher, &tc[0], "pipe_dispatcher", NULL, NULL);
    thread_create(pipe_worker, &tc[1], "pipe_worker", NULL, NULL);

    thread_ctl_run(&tc[0]);
    thread_ctl_run(&tc[1]);

    for (int i = 0; i < NUM_SECONDS_TO_RUN_TEST; i++)
        sleep(1);

    pipe_is_closed = true;

    NIOVA_ASSERT(!close(pipe_fds[0]));
    NIOVA_ASSERT(!close(pipe_fds[1]));

    thread_halt_and_destroy(&tc[0]);
    thread_halt_and_destroy(&tc[1]);

    fprintf(stdout, "%.02f\t\t%s\n",
            (NUM_SECONDS_TO_RUN_TEST * 1000000000.00 / num_wakeups), __func__);

    signal(SIGPIPE, SIG_DFL);
}

struct epoll_worker_priv
{
    const char *ewp_name;
    size_t      ewp_cnt;
    int         ewp_fd;
};

#define MAX_EPOLL_EVENTS 31

static void *
epoll_worker(void *arg)
{
    struct thread_ctl *tc = arg;

    /* Don't initialize the epoll fd until the master says it's OK to proceed.
     */
    thread_ctl_pause_if_should(tc);

    struct epoll_event my_ev[2] = {0};
    struct epoll_worker_priv ewp[2] = {0};

    ewp[0].ewp_name = "epoll-fd foo";
    ewp[0].ewp_fd   = epoll_pipe_0[0];

    ewp[1].ewp_name = "epoll-fd bar";
    ewp[1].ewp_fd   = epoll_pipe_1[0];

    my_ev[0].events = my_ev[1].events = EPOLLIN;
    my_ev[0].data.ptr = &ewp[0];
    my_ev[1].data.ptr = &ewp[1];

    int epfd = epoll_create1(0);
    FATAL_IF((epfd < 0), "epoll_create1(): %s", strerror(errno));

    FATAL_IF(((fcntl(epoll_pipe_0[0], F_SETFL, O_NONBLOCK) ||
               fcntl(epoll_pipe_1[0], F_SETFL, O_NONBLOCK))),
             "fcntl(): %s", strerror(errno));

    int rc;
    rc = epoll_ctl(epfd, EPOLL_CTL_ADD, epoll_pipe_0[0], &my_ev[0]);
    FATAL_IF((rc != 0), "epoll_ctl(0): %s", strerror(errno));

    rc = epoll_ctl(epfd, EPOLL_CTL_ADD, epoll_pipe_1[0], &my_ev[1]);
    FATAL_IF((rc != 0), "epoll_ctl(1): %s", strerror(errno));

    struct epoll_event events[MAX_EPOLL_EVENTS];

    bool stop = false;

    THREAD_LOOP_WITH_CTL(tc)
    {
        char c;
        int nfds = epoll_wait(epfd, events, MAX_EPOLL_EVENTS, 1);

        FATAL_IF((nfds < 0 && errno != EINTR), "epoll_wait(): %s",
                 strerror(errno));

        for (int i = 0; i < nfds; i++)
        {
            struct epoll_event *ev = &events[i];

            if (!ev->data.ptr)
            {
                STDERR_MSG("event idx=%d, null data.ptr", i);
                continue;
            }
            struct epoll_worker_priv *ewp_ev =
                (struct epoll_worker_priv *)ev->data.ptr;

            ewp_ev->ewp_cnt++;

#define MAX_EVENTS_TO_REAP 32
            for (int j = 0; j < MAX_EVENTS_TO_REAP; j++)
            {
                ssize_t rc = read(ewp->ewp_fd, &c, 1);
                if (rc != 1)
                {
                    int save_errno = errno;
                    if (save_errno == EWOULDBLOCK || save_errno == EAGAIN)
                        break;

                    if (!pipe_is_closed)
                        STDERR_MSG("read() returned %zd:  %s", rc,
                                   strerror(save_errno));
                    {
                        stop = true;
                        break;
                    }
                }
                num_wakeups++;
            }
        }
        if (stop)
            break;
    }

//    fprintf(stdout, "%s cnt=%zu\n", ewp[0].ewp_name, ewp[0].ewp_cnt);
//    fprintf(stdout, "%s cnt=%zu\n", ewp[1].ewp_name, ewp[1].ewp_cnt);

    return NULL;
}

static void *
epoll_dispatcher(void *arg)
{
    struct thread_ctl *tc = arg;

    int fd = *(int *)tc->tc_arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        unsigned char c = 0;
        ssize_t rc = write(fd, &c, 1);
        if (rc != 1)
        {
            if (!pipe_is_closed)
                STDERR_MSG("write() returned %zd:  %s", rc, strerror(errno));
            break;
        }
    }

    return NULL;
}

static void
epoll_test(void)
{
    num_wakeups = 0;
    pipe_is_closed = false;

    signal(SIGPIPE, SIG_IGN);

    NIOVA_ASSERT(!pipe(epoll_pipe_0));
    NIOVA_ASSERT(!pipe(epoll_pipe_1));

    thread_create(epoll_worker, &tc[0], "epoll_worker", NULL, NULL);

    thread_create(epoll_dispatcher, &tc[1], "epoll_dispatcher0",
                  (void *)&epoll_pipe_0[1], NULL);

    thread_create(epoll_dispatcher, &tc[2], "epoll_dispatcher1",
                  (void *)&epoll_pipe_1[1], NULL);

    thread_ctl_run(&tc[0]);
    thread_ctl_run(&tc[1]);
    thread_ctl_run(&tc[2]);

    for (int i = 0; i < NUM_SECONDS_TO_RUN_TEST; i++)
        sleep(1);

    pipe_is_closed = true;

    NIOVA_ASSERT(!close(epoll_pipe_0[0]));
    NIOVA_ASSERT(!close(epoll_pipe_0[1]));
    NIOVA_ASSERT(!close(epoll_pipe_1[0]));
    NIOVA_ASSERT(!close(epoll_pipe_1[1]));

    thread_halt_and_destroy(&tc[1]);
    thread_halt_and_destroy(&tc[2]);
    thread_halt_and_destroy(&tc[0]);

    fprintf(stdout, "%.02f\t\t%s\n",
            (NUM_SECONDS_TO_RUN_TEST * 1000000000.00 / num_wakeups), __func__);

    signal(SIGPIPE, SIG_DFL);
}

int
main(void)
{
    fprintf(stdout, "NS/OP\t\tTest Name\n"
            "---------------------------------------------\n");

    epoll_test();
    pthread_cond_test();
    sem_test();
    pipe_test();

    return 0;
}

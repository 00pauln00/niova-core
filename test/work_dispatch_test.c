/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE         /* See feature_test_macros(7) */

#include "common.h"
#include "log.h"
#include "thread.h"

#include <pthread.h>
#include <unistd.h>
#include <semaphore.h>
#include <signal.h>

#define NUM_SECONDS_TO_RUN_TEST 2ULL

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

sem_t sem;

size_t num_wakeups;

int pipe_fds[2];
bool pipe_is_closed;

struct thread_ctl tc[2];

niova_atomic64_t waiters;

static void *
pthread_cond_worker(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        NIOVA_ASSERT(!pthread_mutex_lock(&mutex));
        NIOVA_ASSERT(niova_atomic_inc(&waiters) >= 0);
        NIOVA_ASSERT(!pthread_cond_wait(&cond, &mutex));
        NIOVA_ASSERT(!pthread_mutex_unlock(&mutex));

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
            NIOVA_ASSERT(niova_atomic_dec(&waiters) >= 0);
            NIOVA_ASSERT(!pthread_mutex_lock(&mutex));
            NIOVA_ASSERT(!pthread_cond_signal(&cond));
            NIOVA_ASSERT(!pthread_mutex_unlock(&mutex));
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

    STDOUT_MSG("pthread_cond: num_wakeups per sec=%zd",
               (size_t)(num_wakeups / NUM_SECONDS_TO_RUN_TEST));
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

    STDOUT_MSG("sem_test: num_wakeups per sec=%zd",
               (size_t)(num_wakeups / NUM_SECONDS_TO_RUN_TEST));
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

    STDOUT_MSG("pipe_test: num_wakeups per sec=%zd",
               (size_t)(num_wakeups / NUM_SECONDS_TO_RUN_TEST));

    signal(SIGPIPE, SIG_DFL);
}

int
main(void)
{
    pthread_cond_test();
    sem_test();
    pipe_test();

    return 0;
}

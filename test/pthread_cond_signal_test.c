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

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
size_t num_wakeups;

struct thread_ctl tc[2];

static void *
worker(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        NIOVA_ASSERT(!pthread_mutex_lock(&mutex));
        NIOVA_ASSERT(!pthread_cond_wait(&cond, &mutex));
        NIOVA_ASSERT(!pthread_mutex_unlock(&mutex));

        num_wakeups++;
    }

    return NULL;
}

static void *
dispatcher(void *arg)
{
    struct thread_ctl *tc = arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        NIOVA_ASSERT(!pthread_mutex_lock(&mutex));
        NIOVA_ASSERT(!pthread_cond_signal(&cond));
        NIOVA_ASSERT(!pthread_mutex_unlock(&mutex));
    }

    return NULL;
}

#define NUM_SECONDS_TO_RUN_TEST 2ULL

int
main(void)
{
    thread_create(worker, &tc[0], "worker", NULL, NULL);
    thread_ctl_run(&tc[0]);

    thread_create(dispatcher, &tc[1], "dispatcher", NULL, NULL);
    thread_ctl_run(&tc[1]);

    for (int i = 0; i < NUM_SECONDS_TO_RUN_TEST; i++)
    {
        sleep(1);
    }

    thread_halt_and_destroy(&tc[0]);
    thread_halt_and_destroy(&tc[1]);

    STDOUT_MSG("num_wakeups per sec=%zd",
               (size_t)(num_wakeups / NUM_SECONDS_TO_RUN_TEST));

    return 0;
}

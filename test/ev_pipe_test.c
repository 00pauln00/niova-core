/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */
#include "common.h"
#include "epoll_mgr.h"
#include "ev_pipe.h"
#include "log.h"
#include "queue.h"
#include "thread.h"
#include "util.h"
#include "util_thread.h"

struct epoll_mgr epollMgr;
struct ev_pipe evPipe;
struct epoll_handle *epollHandle;

static bool allCompleted = false;

#define NUM_THREADS 15
struct thread_ctl workerThrCtl[NUM_THREADS];

struct item
{
    size_t num;
    CIRCLEQ_ENTRY(item) lentry;
};

CIRCLEQ_HEAD(itemq, item);

struct item_set
{
    struct item    *items;
    struct itemq    pending;
    struct itemq    completed;
    pthread_mutex_t mutex;
    size_t          exec_cnt;
};

static struct item_set item_sets[NUM_THREADS + 1];

#define NITEMS 100000

static void
ev_pipe_test_cb(const struct epoll_handle *eph)
{
    struct ev_pipe *evp = (struct ev_pipe *)eph->eph_arg;
    NIOVA_ASSERT(evp == &evPipe);

    EV_PIPE_RESET(evp);

    struct item *item = NULL, *tmp = NULL;
    size_t num_completed = 0;

    for (size_t i = 0; i <= NUM_THREADS; i++)
    {
        struct item_set *set = &item_sets[i];

        pthread_mutex_lock(&set->mutex);
        CIRCLEQ_FOREACH_SAFE(item, &set->pending, lentry, tmp)
        {
            NIOVA_ASSERT(item->num == set->exec_cnt);
            set->exec_cnt++;

            CIRCLEQ_REMOVE(&set->pending, item, lentry);
            CIRCLEQ_INSERT_TAIL(&set->completed, item, lentry);

        }

        if (set->exec_cnt == NITEMS)
            num_completed++;

        pthread_mutex_unlock(&set->mutex);
    }

    if (num_completed == (NUM_THREADS + 1))
        allCompleted = true;
}

static void *
evp_pipe_worker(void *arg)
{
    struct thread_ctl *tc = arg;
    NIOVA_ASSERT(tc && tc->tc_arg);

    struct item_set *set = (struct item_set *)tc->tc_arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        for (size_t i = 0; i < NITEMS; i++)
        {
            set->items[i].num = i;
            pthread_mutex_lock(&set->mutex);
            CIRCLEQ_INSERT_TAIL(&set->pending, &set->items[i], lentry);
            pthread_mutex_unlock(&set->mutex);

            ev_pipe_notify(&evPipe);
        }
        break;
    }

    return NULL;
}

int
main(void)
{
    int rc = ev_pipe_setup(&evPipe);
    if (rc)
        exit(rc);

    rc = util_thread_install_event_src(evp_read_fd_get(&evPipe), EPOLLIN,
                                       ev_pipe_test_cb, &evPipe, &epollHandle);
    FATAL_IF((rc), "util_thread_install_event_src(): %s", strerror(-rc));

    for (int i = 0; i <= NUM_THREADS; i++)
    {
        struct item_set *set = &item_sets[i];
        CIRCLEQ_INIT(&set->pending);
        CIRCLEQ_INIT(&set->completed);
        pthread_mutex_init(&set->mutex, NULL);
        set->items = calloc(NITEMS, sizeof(struct item));
    }

    // Single threaded test
    struct item_set *main_set = &item_sets[NUM_THREADS];
    for (size_t i = 0; i < NITEMS; i++)
    {
        pthread_mutex_lock(&main_set->mutex);
        main_set->items[i].num = i;
        CIRCLEQ_INSERT_TAIL(&main_set->pending, &main_set->items[i], lentry);
        pthread_mutex_unlock(&main_set->mutex);

        ev_pipe_notify(&evPipe);
    }

    // Multi threaded test
    for (int i = 0; i < NUM_THREADS; i++)
    {
        struct item_set *set = &item_sets[i];

        int rc = thread_create(evp_pipe_worker, &workerThrCtl[i], "worker",
                               (void *)set, NULL);
        if (rc)
            exit(rc);
    }

    for (int i = 0; i < NUM_THREADS; i++)
        thread_ctl_run(&workerThrCtl[i]);

    for (int i = 0; i < NUM_THREADS; i++)
        thread_join(&workerThrCtl[i]);

    while (!allCompleted)
        usleep(100);

    for (int i = 0; i < (NUM_THREADS + 1); i++)
    {
        NIOVA_ASSERT(item_sets[i].exec_cnt == NITEMS);
        free(item_sets[i].items);
    }

    return 0;
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <pthread.h>

#include "common.h"
#include "ctor.h"
#include "epoll_mgr.h"
#include "init.h"
#include "log.h"
#include "thread.h"
#include "util_thread.h"

REGISTRY_ENTRY_FILE_GENERATE;

struct util_thread
{
    struct thread_ctl ut_tc;
    struct epoll_mgr  ut_epm;
    unsigned int      ut_started : 1;
};

#define MAX_UT_EPOLL_HANDLES 32

static struct util_thread utilThread;
static struct epoll_handle utilThreadEpollHandles[MAX_UT_EPOLL_HANDLES];
static size_t utilThreadNumEpollHandles;
static pthread_mutex_t utilThreadMutex = PTHREAD_MUTEX_INITIALIZER;

int
util_thread_install_event_src(int fd, int events,
                              void (*ut_cb)(const struct epoll_handle *),
                              void *arg, struct epoll_handle **ret_eph)
{
    if (fd < 0 || !ut_cb)
        return -EINVAL;

    pthread_mutex_lock(&utilThreadMutex);
    if (utilThreadNumEpollHandles >= MAX_UT_EPOLL_HANDLES)
    {
        pthread_mutex_unlock(&utilThreadMutex);
        return -ENOSPC;
    }

    struct epoll_handle *eph =
        &utilThreadEpollHandles[utilThreadNumEpollHandles];

    int rc = epoll_handle_init(eph, fd, events, ut_cb, arg);
    if (!rc)
        if (utilThread.ut_started)
            rc = epoll_handle_add(&utilThread.ut_epm, eph);

    if (rc) // Failure, reset the handle
    {
        memset(eph, 0, sizeof(*eph));
    }
    else // Success
    {
        utilThreadNumEpollHandles++;

        if (ret_eph)
            *ret_eph = eph;
    }

    pthread_mutex_unlock(&utilThreadMutex);

    return rc;
}

static util_thread_ctx_t *
util_thread_main(void *arg)
{
    FUNC_ENTRY(LL_DEBUG);

    struct thread_ctl *tc = arg;
    tc->tc_is_utility_thread = 1;

    thread_ctl_set_self(tc);

    NIOVA_ASSERT(tc && tc->tc_arg);

    struct util_thread *ut = tc->tc_arg;

    THREAD_LOOP_WITH_CTL(tc)
    {
        int rc = epoll_mgr_wait_and_process_events(&ut->ut_epm, 1000);

        if (rc < 0 && rc != -EINTR)
            LOG_MSG(LL_WARN, "epoll_mgr_wait_and_process_events(): %s",
                    strerror(-rc));
    }

    return NULL;
}

static init_ctx_t NIOVA_CONSTRUCTOR(UTIL_THREAD_SUBSYS_CTOR_PRIORITY)
util_thread_subsystem_init(void)
{
    FUNC_ENTRY(LL_DEBUG);

    NIOVA_ASSERT(!utilThread.ut_started);

    int rc = epoll_mgr_setup(&utilThread.ut_epm);
    FATAL_IF(rc, "epoll_mgr_setup(): %s", strerror(-rc));

    //init_ctx_t is always single threaded
    utilThread.ut_started = 1;

    for (size_t i = 0; i < utilThreadNumEpollHandles; i++)
    {
        int rc =
            epoll_handle_add(&utilThread.ut_epm, &utilThreadEpollHandles[i]);

        FATAL_IF(rc, "epoll_handle_add(): %s", strerror(-rc));
    }

    rc = thread_create_watched(util_thread_main, &utilThread.ut_tc,
                               "util_thread", &utilThread, NULL);

    FATAL_IF(rc, "thread_create(): %s", strerror(errno));

    thread_ctl_run(&utilThread.ut_tc);
}

static destroy_ctx_t NIOVA_DESTRUCTOR(UTIL_THREAD_SUBSYS_CTOR_PRIORITY)
util_thread_subsystem_destroy(void)
{
    FUNC_ENTRY(LL_DEBUG);

    if (utilThread.ut_started)
    {
        for (size_t i = 0; i < utilThreadNumEpollHandles; i++)
        {
            if (utilThreadEpollHandles[i].eph_installed)
            {
                int rc =
                    epoll_handle_del(&utilThread.ut_epm,
                                     &utilThreadEpollHandles[i]);

                FATAL_IF(rc, "epoll_handle_del(): %s", strerror(-rc));
            }
        }

        epoll_mgr_close(&utilThread.ut_epm);

        thread_halt_and_destroy(&utilThread.ut_tc);
    }
}

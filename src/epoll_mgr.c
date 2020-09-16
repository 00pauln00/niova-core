/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/epoll.h>
#include <unistd.h>

#include "log.h"
#include "atomic.h"
#include "common.h"
#include "epoll_mgr.h"
#include "env.h"
#include "ctor.h"
#include "util.h"

static size_t epollMgrNumEvents = EPOLL_MGR_DEF_EVENTS;

int
epoll_mgr_setup(struct epoll_mgr *epm)
{
    if (!epm)
        return -EINVAL;

    else if (epm->epm_ready)
        return -EALREADY;

    niova_atomic_init(&epm->epm_num_handles, 0);

    epm->epm_epfd = epoll_create1(0);
    if (epm->epm_epfd < 0)
        return -errno;

    epm->epm_waiting = 0;
    epm->epm_ready = 1;

    return 0;
}

int
epoll_mgr_close(struct epoll_mgr *epm)
{
    if (!epm || !epm->epm_ready)
        return -EINVAL;

    else if (epm->epm_epfd < 0)
        return -EBADF;

    epm->epm_ready = 0;

    return close(epm->epm_epfd);
}

int
epoll_handle_init(struct epoll_handle *eph, int fd, int events,
                  epoll_mgr_cb_t cb, epoll_mgr_cb_t getput, void *arg)
{
    if (!eph || !cb)
        return -EINVAL;

    else if (fd < 0)
        return -EBADF;

    eph->eph_installed    = 0;
    eph->eph_fd           = fd;
    eph->eph_events       = events;
    eph->eph_cb           = cb;
    eph->eph_owner_getput = getput;
    eph->eph_arg          = arg;

    return 0;
}

int
epoll_handle_add(struct epoll_mgr *epm, struct epoll_handle *eph)
{
    if (!epm || !eph || !eph->eph_cb || !epm->epm_ready)
        return -EINVAL;

    else if (eph->eph_fd < 0 || epm->epm_epfd < 0)
        return -EBADF;

    else if (eph->eph_installed)
        return -EALREADY;

    struct epoll_event ev = {.events = eph->eph_events, .data.ptr = eph};

    int rc = epoll_ctl(epm->epm_epfd, EPOLL_CTL_ADD, eph->eph_fd, &ev);
    if (rc < 0)
    {
        return -errno;
    }

    const int num_handles = niova_atomic_inc(&epm->epm_num_handles);
    NIOVA_ASSERT(num_handles > 0);

    eph->eph_installed = 1;

    return 0;
}

int
epoll_handle_del(struct epoll_mgr *epm, struct epoll_handle *eph)
{
    if (!epm || !eph || !epm->epm_ready)
        return -EINVAL;

    else if (epm->epm_epfd < 0 || eph->eph_fd < 0)
        return -EBADF;

    else if (!eph->eph_installed)
        return -EAGAIN;

    struct epoll_event ev = {.events = 0, .data.fd = -1};

    int rc = epoll_ctl(epm->epm_epfd, EPOLL_CTL_DEL, eph->eph_fd, &ev);
    if (!rc)
    {
        const int num_handles = niova_atomic_dec(&epm->epm_num_handles);
        NIOVA_ASSERT(num_handles >= 0);

        eph->eph_installed = 0;
    }

    return rc;
}

/* This version of epoll_handle_del should be used in destructors that
 * destroy the eph. Waits until epoll is guaranteed to be done with eph.
 */
int
epoll_handle_del_wait(struct epoll_mgr *epm, struct epoll_handle *eph)
{
    int rc = epoll_handle_del(epm, eph);
    if (rc)
        return rc;

    struct timespec ts;
    msec_2_timespec(&ts, 100);

    // ensure all handle events have been processed before returning
    while (epm->epm_waiting)
        nanosleep(&ts, NULL);

    return 0;
}

int
epoll_mgr_wait_and_process_events(struct epoll_mgr *epm, int timeout)
{
    if (!epm || !epm->epm_ready)
        return -EINVAL;

    int maxevents = MAX(1, MIN(epollMgrNumEvents,
                               niova_atomic_read(&epm->epm_num_handles)));

    struct epoll_event evs[maxevents];

    // epoll_wait may return epoll handles, so don't remove attached objects
    epm->epm_waiting = 1;

    const int nevents =
        epoll_wait(epm->epm_epfd, evs, maxevents, timeout);

    SIMPLE_LOG_MSG(LL_NOTIFY, "epoll_wait(): %d", nevents);

    if (nevents < 0)
        return -errno;

    for (int i = 0; i < nevents; i++)
    {
        struct epoll_handle *eph = evs[i].data.ptr;
        if (eph->eph_installed && eph->eph_owner_getput)
            eph->eph_owner_getput(eph, 0);
    }
    epm->epm_waiting = 0;

    for (int i = 0; i < nevents; i++)
    {
        struct epoll_handle *eph = evs[i].data.ptr;
        SIMPLE_LOG_MSG(LL_NOTIFY, "epoll_wait(): fd=%d", eph->eph_fd);

        // save eph_put in case eph is destroyed in cb
        epoll_mgr_cb_t eph_put = eph ? eph->eph_owner_getput : NULL;

        if (eph->eph_installed && eph->eph_cb)
            eph->eph_cb(eph, evs[i].events);

        if (eph_put)
            eph_put(eph, 1);
    }

    epm->epm_waiting = 0;

    return nevents;
}

void
epoll_mgr_env_var_cb(const struct niova_env_var *nev)
{
    if (nev && nev->nev_present)
        epollMgrNumEvents = nev->nev_long_value;
}

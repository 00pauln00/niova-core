/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _EPOLL_HANDLE_
#define _EPOLL_HANDLE_ 1

#include <sys/epoll.h>

#include "atomic.h"

/**
 *  There should be 1 event per fd (I think, based on my reading of the kernel's
 *  eventpoll.c).  At this time, this library only supports these 3 static values.
 */
#define EPOLL_MGR_MIN_EVENTS 32
#define EPOLL_MGR_DEF_EVENTS 128
#define EPOLL_MGR_MAX_EVENTS 1024

struct epoll_handle
{
    int          eph_fd;
    int          eph_events;
    unsigned int eph_installed : 1;
    void        *eph_arg;
    void         (*eph_cb)(const struct epoll_handle *);
};

typedef void (*epoll_mgr_cb_t)(const struct epoll_handle *);

struct epoll_mgr
{
    int              epm_epfd;
    niova_atomic32_t epm_num_handles;
    unsigned int     epm_ready : 1;
};

struct niova_env_var;

void
epoll_mgr_env_var_cb(const struct niova_env_var *nev);

int
epoll_mgr_setup(struct epoll_mgr *epm);

int
epoll_mgr_close(struct epoll_mgr *epm);

int
epoll_handle_init(struct epoll_handle *eph, int fd, int events,
                  void (*cb)(const struct epoll_handle *), void *arg);

int
epoll_handle_add(struct epoll_mgr *epm, struct epoll_handle *eph);

int
epoll_handle_del(struct epoll_mgr *epm, struct epoll_handle *eph);

int
epoll_mgr_wait_and_process_events(struct epoll_mgr *epm, int timeout);

#endif

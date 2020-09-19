/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _EPOLL_HANDLE_
#define _EPOLL_HANDLE_ 1

#include <sys/epoll.h>
#include <pthread.h>

#include "thread.h"

/**
 * There should be 1 event per fd (I think, based on my reading of the kernel's
 * eventpoll.c).  At this time, this library only supports these 3 static
 * values.
 */
#define EPOLL_MGR_MIN_EVENTS 32
#define EPOLL_MGR_DEF_EVENTS 128
#define EPOLL_MGR_MAX_EVENTS 1024

enum epoll_handle_ref_op
{
    EPH_REF_GET,
    EPH_REF_PUT
};

struct epoll_handle
{
    int          eph_fd;
    int          eph_events;
    unsigned int eph_installed : 1;
    unsigned int eph_installing : 1;
    unsigned int eph_destroying : 1;
    void        *eph_arg;
    void         (*eph_cb)(const struct epoll_handle *);
    void         (*eph_ref_cb)(void *, enum epoll_handle_ref_op);
    CIRCLEQ_ENTRY(epoll_handle) eph_lentry;
};

CIRCLEQ_HEAD(epoll_handle_list, epoll_handle);

typedef void (*epoll_mgr_cb_t)(const struct epoll_handle *);

struct epoll_mgr
{
    pthread_t        epm_thread_id;
    pthread_mutex_t  epm_mutex;
    int              epm_num_handles;
    int              epm_epfd;
    unsigned int     epm_ready : 1;
    struct epoll_handle_list epm_active_list;
    struct epoll_handle_list epm_destroy_list;
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
                  void (*cb)(const struct epoll_handle *), void *arg,
                  void (*ref_cb)(void *, enum epoll_handle_ref_op));

int
epoll_handle_add(struct epoll_mgr *epm, struct epoll_handle *eph);

int
epoll_handle_del(struct epoll_mgr *epm, struct epoll_handle *eph);

int
epoll_mgr_wait_and_process_events(struct epoll_mgr *epm, int timeout);

#endif

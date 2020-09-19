/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <stdlib.h>

#include "common.h"
#include "epoll_mgr.h"
#include "ev_pipe.h"
#include "log.h"
#include "thread.h"

static void
foo_cb(const struct epoll_handle *eph)
{
    (void)eph;
    return;
}

static void
epoll_mgr_basic_tests(void)
{
    struct epoll_mgr *epm = calloc(1UL, sizeof(struct epoll_mgr));
    if (!epm)
        exit(errno);

    int rc = epoll_mgr_setup(NULL);
    FATAL_IF(rc != -EINVAL,
             "epoll_mgr_setup() expected to return -EINVAL (rc=%d)", rc);

    rc = epoll_mgr_setup(epm);

    // Check epm conditions following setup
    FATAL_IF(rc, "epoll_mgr_setup(): %s", strerror(-rc));
    FATAL_IF(!epm->epm_ready, "epm_ready invalid");
    FATAL_IF(epm->epm_epfd < 0, "epm_epfd invalid");
    FATAL_IF(!CIRCLEQ_EMPTY(&epm->epm_active_list), "active list not empty");
    FATAL_IF(!CIRCLEQ_EMPTY(&epm->epm_destroy_list), "destroy list not empty");

    // > 1 setup should fail
    rc = epoll_mgr_setup(epm);
    FATAL_IF(rc != -EALREADY,
             "epoll_mgr_setup() expected to return -EALREADY (rc=%d)", rc);

    // Insert a bogus eph
    struct epoll_handle eph = {0};
    rc = epoll_handle_init(&eph, -1, 0, NULL, NULL, NULL);
    FATAL_IF(rc != -EINVAL, "epoll_handle_init() expected -EINVAL got %d", rc);

    rc = epoll_handle_init(&eph, -1, 0, foo_cb, NULL, NULL);
    FATAL_IF(rc != -EBADF, "epoll_handle_init() expected -EBADF got %d", rc);

    // Create a legit fd for epoll_handle_init and add
    struct ev_pipe evp;
    rc = ev_pipe_setup(&evp);
    FATAL_IF(rc, "ev_pipe_setup(): %s", strerror(-rc));

    rc = epoll_handle_init(&eph, evp_read_fd_get(&evp), EPOLLIN, foo_cb, NULL,
                           NULL);
    FATAL_IF(rc, "epoll_handle_init() expected 0 got %d", rc);

    rc = epoll_handle_add(epm, &eph);
    FATAL_IF(rc, "epoll_handle_add() expected 0 got %d", rc);

    // Ensure the close fails when a handle is still attached
    rc = epoll_mgr_close(epm);
    FATAL_IF(rc != -EBUSY,
             "epoll_mgr_setup() expected to return 0 (rc=%d)", rc);

    // copy the evp to another stack evp and expect to get ENOENT
    struct epoll_handle eph_copy = eph;
    rc = epoll_handle_del(epm, &eph_copy);
    FATAL_IF(rc != -ENOENT, "epoll_handle_del() expected -ENOENT got %d", rc);

    // Delete the legit handle and check 'installed' state
    rc = epoll_handle_del(epm, &eph);
    FATAL_IF(rc, "epoll_handle_del() expected 0 got %d", rc);

    FATAL_IF(eph.eph_installed,
             "eph_installed is still set (expected synchronous removal)");

    // Close should now succeed
    rc = epoll_mgr_close(epm);
    FATAL_IF(rc, "epoll_mgr_setup() expected to return 0 (rc=%d)", rc);
    FATAL_IF(epm->epm_ready, "emp_ready is still true");

    // Try to add handle after the mgr has been closed
    rc = epoll_handle_init(&eph, evp_read_fd_get(&evp), EPOLLIN, foo_cb, NULL,
                           NULL);
    FATAL_IF(rc, "epoll_handle_init() expected 0 got %d", rc);

    rc = epoll_handle_add(epm, &eph);
    FATAL_IF(rc != -EINVAL, "epoll_handle_add() expected -EINVAL got %d", rc);

    free(epm);
}

int
main(void)
{
    epoll_mgr_basic_tests();
}

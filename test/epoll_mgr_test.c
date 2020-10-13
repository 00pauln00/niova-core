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
#include "ref_tree_proto.h"
#include "thread.h"

enum epm_mgr_test_threads
{
    EPM__MIN = 0,
    EPM_MGR = 0,
    EPM_USER = 1,
    EPM__MAX = 2,
};

static struct thread_ctl epmThreads[EPM__MAX];

struct epm_test_handle
{
    REF_TREE_ENTRY(epm_test_handle) eth_rtentry;
    const int64_t eth_id;
    struct epoll_handle eth_eph;
    struct ev_pipe eth_evp;
};

static int
epm_test_handle_cmp(const struct epm_test_handle *a,
                    const struct epm_test_handle *b)
{
    return a->eth_id == b->eth_id ? 0 : (a->eth_id > b->eth_id ? 1 : -1);
}

REF_TREE_HEAD(epoll_mgr_test_ref_tree, epm_test_handle);
REF_TREE_GENERATE(epoll_mgr_test_ref_tree, epm_test_handle, eth_rtentry,
                  epm_test_handle_cmp);

static struct epoll_mgr_test_ref_tree epollMgrTestRT;

static void
foo_cb(const struct epoll_handle *eph, uint32_t events)
{
    (void)eph;
    return;
}

static int64_t
epm_mgr_test_getid(void)
{
    static niova_atomic64_t id;

    return niova_atomic_inc(&id);
}

static epoll_mgr_cb_ctx_t
epoll_mgr_thread_test_ref_cb(void *arg, enum epoll_handle_ref_op op)
{
    struct epm_test_handle *eth = (struct epm_test_handle *)arg;
    FATAL_IF(!eth, "arg is NULL");

    if (op == EPH_REF_GET)
        REF_TREE_REF_GET_ELEM(&epollMgrTestRT, eth, eth_rtentry);

    else if (op == EPH_REF_PUT) // may enter the destructor
        RT_PUT(epoll_mgr_test_ref_tree, &epollMgrTestRT, eth);

    else
        FATAL_IF(1, "op=%d is neither EPH_REF_GET nor EPH_REF_PUT", op);
}

static epoll_mgr_cb_ctx_t
epoll_mgr_thread_test_cb(const struct epoll_handle *eph, uint32_t events)
{
    NIOVA_ASSERT(eph && eph->eph_arg);

    struct epm_test_handle *eth = (struct epm_test_handle *)eph->eph_arg;

    // Sanity check
    NIOVA_ASSERT(eth == OFFSET_CAST(epm_test_handle, eth_eph, eph));

    EV_PIPE_RESET(&eth->eth_evp);
}

static struct epm_test_handle *
epoll_mgr_test_handle_constructor(const struct epm_test_handle *in)
{
    if (!in)
        return NULL;

    struct epm_test_handle *eth =
        calloc((size_t)1, sizeof(struct epm_test_handle));

    FATAL_IF(!eth, "eth handle allocation failed");

    CONST_OVERRIDE(int64_t, eth->eth_id, in->eth_id);

    /* In this test, some "system" call ops are performed in constructor /
     * destructor context, however, the epoll_handle_[add|del] may not be
     * called here.  This is because epoll_mgr_test_thread_ref_cb() will take
     * ref-tree ref on the object before the RT has completed it's own init
     * procedures which include atomic placement into the tree and initializing
     * of the object's ref cnt
     */
    int rc = ev_pipe_setup(&eth->eth_evp);
    FATAL_IF(rc, "ev_pipe_setup(): %s", strerror(-rc));

    SIMPLE_LOG_MSG(LL_NOTIFY, "eth=%p id=%lu", eth, eth->eth_id);

    return eth;
}

static size_t destructor_cnt;

static int
epoll_mgr_test_handle_destructor(struct epm_test_handle *destroy)
{
    FATAL_IF((!destroy), "destroy obj is invalid %p", destroy);

    SIMPLE_LOG_MSG(LL_NOTIFY, "eth=%p id=%lu", destroy, destroy->eth_id);

    FATAL_IF(destroy->eth_rtentry.rte_ref_cnt, "ref cnt is non-zero (%d)",
             destroy->eth_rtentry.rte_ref_cnt);
    FATAL_IF(destroy->eth_eph.eph_installed, "eph_installed is true");
    FATAL_IF(!destroy->eth_eph.eph_destroying, "eph_destroying is false");
    FATAL_IF(!destroy->eth_eph.eph_async_destroy,
             "eph_async_destroy is false");

    int rc = ev_pipe_cleanup(&destroy->eth_evp);
    FATAL_IF(rc, "ev_pipe_cleanup(): %s", strerror(-rc));

    free(destroy);

    destructor_cnt++;

    return rc;
}

static void
epm_mgr_wait_check(struct epoll_mgr *epm)
{
    int rc;
    // Call the event processor w/out expecting any events
    do
    {
        rc = epoll_mgr_wait_and_process_events(epm, 1);
        FATAL_IF((rc && rc != -EINTR),
                 "epoll_mgr_wait_and_process_events(): %s", strerror(-rc));
    } while (rc == -EINTR);

    // After epoll_mgr_wait_and_process_events(), thread id should be set
    FATAL_IF(epm->epm_thread_id != pthread_self(),
             "epm_thread_id expected %lu, got %lu",
             epm->epm_thread_id, pthread_self());
}

static void *
epoll_mgr_test_thread_user(void *arg)
{
    struct thread_ctl *tc = (struct thread_ctl *)arg;

    struct epoll_mgr *epm = thread_ctl_get_arg(tc);
    NIOVA_ASSERT(epm);

    const int64_t my_id = epm_mgr_test_getid();
    const struct epm_test_handle in = {.eth_id = my_id};

    int rc = 0;
    struct epm_test_handle *eth =
        RT_GET_ADD(epoll_mgr_test_ref_tree, &epollMgrTestRT, &in, &rc);

    FATAL_IF((!eth || rc), "test handle creation failed (epm=%p rc=%d)",
             eth, rc);

    rc = epoll_handle_init(&eth->eth_eph, evp_read_fd_get(&eth->eth_evp),
                           EPOLLIN, epoll_mgr_thread_test_cb, eth,
                           epoll_mgr_thread_test_ref_cb);

    FATAL_IF(rc, "epoll_handle_init() expected 0 got %d", rc);

    rc = epoll_handle_add(epm, &eth->eth_eph);
    FATAL_IF(rc, "epoll_handle_add() expected 0 got %d", rc);

    FATAL_IF(eth->eth_rtentry.rte_ref_cnt != 2,
             "expected ref cnt value to be '2', got %d",
             eth->eth_rtentry.rte_ref_cnt);

    THREAD_LOOP_WITH_CTL(tc)
    {
        /* Wait for the epoll mgr thread to have entered and exited
         * epoll_wait() at least once.  Note that the 'epm' pointer should
         * remain valid as long as our handle is in place.
         */
        if (niova_atomic_read(&epm->epm_epoll_wait_cnt) <= 0)
        {
            thread_issue_sig_alarm_to_thread(epm->epm_thread_id);
            usleep(100);
            continue;
        }
        FATAL_IF((eth->eth_eph.eph_destroying ||
                  eth->eth_eph.eph_async_destroy),
                 "eph_destroying or eph_async_destroy already set");

        // Delete must happen in the context of 'epoll_mgr_test_thread_mgr'
        rc = epoll_handle_del(epm, &eth->eth_eph);
        FATAL_IF(rc, "epoll_handle_del() expected 0 got %d", rc);

        FATAL_IF(!(eth->eth_eph.eph_destroying &&
                   eth->eth_eph.eph_async_destroy),
                 "eph_destroying or eph_async_destroy not set");

        while (eth->eth_eph.eph_installed)
        {
            SIMPLE_LOG_MSG(LL_DEBUG, "waiting for async removal");
            usleep(1000);
        }

        thread_ctl_halt(&epmThreads[EPM_MGR]);
        thread_issue_sig_alarm_to_thread(epm->epm_thread_id);
        break;
    }

    RT_PUT(epoll_mgr_test_ref_tree, &epollMgrTestRT, eth);

    FATAL_IF(destructor_cnt != 1, "destructor_cnt (%zu) != 1",
             destructor_cnt);

    return NULL;
}

static void *
epoll_mgr_test_thread_mgr(void *arg)
{
    struct thread_ctl *tc = (struct thread_ctl *)arg;

    struct epoll_mgr *epm = thread_ctl_get_arg(tc);
    NIOVA_ASSERT(epm);

    // Only this thread may block on the epm (though epm was created by main).
    epm_mgr_wait_check(epm);

    // The user thread tells us when to halt.
    THREAD_LOOP_WITH_CTL(tc)
    {
        int rc = epoll_mgr_wait_and_process_events(epm, -1);

        SIMPLE_LOG_MSG(LL_DEBUG, "sg=%zu wc=%lld rc=%d", tc->tc_sig_cnt,
                       niova_atomic_read(&epm->epm_epoll_wait_cnt), rc);
    }

    return NULL;
}


static void
epoll_mgr_multi_thread_tests(void)
{
    struct epoll_mgr *epm = calloc(1UL, sizeof(struct epoll_mgr));
    FATAL_IF(!epm, "calloc(): ENOMEM");

    int rc = epoll_mgr_setup(epm);
    FATAL_IF(rc, "epoll_mgr_setup(): %s", strerror(-rc));

    rc = thread_create(epoll_mgr_test_thread_mgr, &epmThreads[EPM_MGR],
                       "epm-test-mgr", epm, NULL);
    FATAL_IF(rc, "thread_create(): %s", strerror(-rc));

    rc = thread_create(epoll_mgr_test_thread_user, &epmThreads[EPM_USER],
                       "epm-test-user", epm, NULL);
    FATAL_IF(rc, "thread_create(): %s", strerror(-rc));

    for (enum epm_mgr_test_threads i = EPM__MIN; i < EPM__MAX; i++)
    {
        // thread pauses at main loop until thread_ctl_run() is called
        thread_creator_wait_until_ctl_loop_reached(&epmThreads[i]);

        // This join should fail with -EBUSY
        rc = thread_join_nb(&epmThreads[i]);
        FATAL_IF(rc != -EBUSY,
                 "thread_join_nb() expected return -EBUSY, got %d", rc);

        // Allow the thread into its main loop
        thread_ctl_run(&epmThreads[i]);
    }

    for (enum epm_mgr_test_threads i = EPM__MIN; i < EPM__MAX; i++)
    {
        long int trc = thread_join(&epmThreads[i]);
        FATAL_IF(trc, "thread_join() on thread-idx %d failed: %ld", i, trc);
    }

    /* Close from this thread (which is not the thread that blocked in
     * epoll_wait().
     */
    rc = epoll_mgr_close(epm);
    FATAL_IF(rc, "epoll_mgr_setup() expected to return 0 (rc=%d)", rc);
    FATAL_IF(epm->epm_ready, "emp_ready is still true");
    FATAL_IF(epm->epm_num_handles, "emp_num_handles is not 0 (%d)",
             epm->epm_num_handles);

    free(epm);
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

    epm_mgr_wait_check(epm);

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

    // Call the event processor w/out expecting any events
    epm_mgr_wait_check(epm);

    // Close should now succeed
    rc = epoll_mgr_close(epm);
    FATAL_IF(rc, "epoll_mgr_setup() expected to return 0 (rc=%d)", rc);
    FATAL_IF(epm->epm_ready, "emp_ready is still true");
    FATAL_IF(epm->epm_num_handles, "emp_num_handles is not 0 (%d)",
             epm->epm_num_handles);

    // Try to add handle after the mgr has been closed
    rc = epoll_handle_init(&eph, evp_read_fd_get(&evp), EPOLLIN, foo_cb, NULL,
                           NULL);
    FATAL_IF(rc, "epoll_handle_init() expected 0 got %d", rc);

    rc = epoll_handle_add(epm, &eph);
    FATAL_IF(rc != -EINVAL, "epoll_handle_add() expected -EINVAL got %d", rc);

    rc = ev_pipe_cleanup(&evp);
    FATAL_IF(rc, "ev_pipe_cleanup() expected 0 got %d", rc);

    free(epm);
}

int
main(void)
{
    REF_TREE_INIT(&epollMgrTestRT, epoll_mgr_test_handle_constructor,
              epoll_mgr_test_handle_destructor);

    epoll_mgr_basic_tests();
    epoll_mgr_multi_thread_tests();
}

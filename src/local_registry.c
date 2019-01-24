/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <unistd.h>

#include "log.h"
#include "lock.h"
#include "atomic.h"
#include "local_registry.h"

REGISTRY_ENTRY_FILE_GENERATE;

static struct lreg_node_list lRegInstallingNodes;
static struct lreg_node      lRegRootNode;
static bool                  lRegInitialized = false;
static spinlock_t            lRegLock;
static struct thread_ctl     lRegSvcThreadCtl;
static pthread_rwlock_t      lRegRwLock;

#define LREG_SUBSYS_WRLOCK pthread_rwlock_wrlock(&lRegRwLock)
#define LREG_SUBSYS_RDLOCK pthread_rwlock_rdlock(&lRegRwLock)
#define LREG_SUBSYS_UNLOCK pthread_rwlock_unlock(&lRegRwLock)

#define LREG_NODE_INSTALL_LOCK   spinlock_lock(&lRegLock)
#define LREG_NODE_INSTALL_UNLOCK spinlock_unlock(&lRegLock)

/**
 * lreg_root_node_get - returns the root node of the local registry.
 */
struct lreg_node *
lreg_root_node_get(void)
{
    NIOVA_ASSERT(lRegInitialized);

    return &lRegRootNode;
}

#if 0
static lreg_install_int_ctx_t
lreg_node_install_check_passes_wrlocked(const struct lreg_node *child,
                                        const struct lreg_node *parent)
{
    /* Grab the name of the child object.
     */
    struct lreg_value child_val;
    int rc = child->lrn_cb(LREG_NODE_CB_OP_GET_NAME,
                           (struct lreg_node *)child, &child_val);
    if (rc)
        return rc;

    struct lreg_node *sibling = NULL;

    //XXx do we care about naming collisions here?
    CIRCLEQ_FOREACH(sibling, &parent->lrn_head, lrn_lentry)
    {
        struct lreg_value sibling_val;

        rc = sibling->lrn_cb(LREG_NODE_CB_OP_GET_NAME,
                             (struct lreg_node *)sibling, &child_val);
        NIOVA_ASSERT(!rc); // Bogus entries should not be here.

        if (!strncmp(sibling_val.lrv_string, child_val.lrv_string,
                     LREG_VALUE_STRING_MAX))
            return -EEXIST;
    }

    return 0;
}
#endif

/**
 * lreg_node_install - executed exclusively by the registry service thread
 *    (lreg_svc_ctx_t) when installing new nodes from the lRegInstallingNodes
 *    queue.
 * @child:  the child being installed
 * NOTES:  the parent list head (lrn_head) is exclusively owned by the
 *    registry service thread.
 */
static lreg_svc_ctx_t
lreg_node_install(struct lreg_node *child)
{
    struct lreg_node *parent = child->lrn_parent_for_install_only;

    NIOVA_ASSERT(!lreg_node_needs_installation(parent));

    /* This is really required only for LREG_NODE_TYPE_ARRAY and
     * LREG_NODE_TYPE_OBJECT.
     */
    CIRCLEQ_INIT(&child->lrn_head);

    //int rc = lreg_node_install_check_passes_wrlocked(child, parent);

    int rc = child->lrn_may_destroy ?
        -ESTALE : child->lrn_cb(LREG_NODE_CB_OP_INSTALL_NODE, child, NULL);

    if (!rc)
    {
        const bool install_complete_ok = lreg_node_install_complete(child);
        NIOVA_ASSERT(install_complete_ok);

        CIRCLEQ_INSERT_HEAD(&parent->lrn_head, child, lrn_lentry);

        DBG_LREG_NODE(LL_DEBUG, parent, "parent");
        DBG_LREG_NODE(LL_DEBUG, child, "child");
    }
    else
    {
        int destroy_rc =
            child->lrn_cb(LREG_NODE_CB_OP_DESTROY_NODE, child, NULL);

        DBG_LREG_NODE(LL_WARN, child,
                      "child install failed - install: '%s', destroy: '%s'",
                      strerror(-rc), strerror(-destroy_rc));
    }
}

/**
 * lreg_install_get_queued_node - detects a queued node, removes, and returns
 *    it.
 */
static lreg_svc_lrn_ctx_t
lreg_install_get_queued_node(void)
{
    struct lreg_node *install = NULL;

    LREG_NODE_INSTALL_LOCK;
    if (!CIRCLEQ_EMPTY(&lRegInstallingNodes))
    {
        install = CIRCLEQ_FIRST(&lRegInstallingNodes);
        CIRCLEQ_REMOVE(&lRegInstallingNodes, install, lrn_lentry);
    }
    LREG_NODE_INSTALL_UNLOCK;

    return install;
}

/**
 * lreg_install_queued_nodes - called only by the service thread to install the
 *    registry nodes which are on the install queue.
 */
static lreg_svc_ctx_t
lreg_install_queued_nodes(void)
{
    struct lreg_node *install;

    while ((install = lreg_install_get_queued_node()))
        lreg_node_install(install);
}

/**
 * lreg_node_queue_for_install - Inserts a registry node into the installation
 *    queue.
 * @child - the child node to be installed.
 * NOTE: the child must have its parent object assigned to its
 *       lrn_parent_for_install_only struct member.
 */
lreg_install_int_ctx_t
lreg_node_install_prepare(struct lreg_node *child, struct lreg_node *parent)
{
    NIOVA_ASSERT(child && parent);

    if (!(parent->lrn_node_type == LREG_NODE_TYPE_OBJECT ||
          (parent->lrn_node_type == LREG_NODE_TYPE_ARRAY &&
           parent->lrn_user_type == child->lrn_user_type)))
        return -EINVAL;

    else if (!lreg_node_needs_installation(child))
        return -EALREADY;

    else if (!child->lrn_cb_arg)
        return -ENOENT;

    else if (!lreg_node_install_prep_ok(child))
        return -EALREADY;

    DBG_LREG_NODE(LL_DEBUG, parent, "parent");
    DBG_LREG_NODE(LL_DEBUG, child, "child");

    LREG_NODE_INSTALL_LOCK;

    child->lrn_parent_for_install_only = parent;

    CIRCLEQ_INSERT_TAIL(&lRegInstallingNodes, child, lrn_lentry);

    LREG_NODE_INSTALL_UNLOCK;

    return 0;
}

void
lreg_node_init(struct lreg_node *lrn, enum lreg_node_types node_type,
               enum lreg_user_types user_type, lrn_cb_t cb,
               bool statically_allocated)
{
    lrn->lrn_node_type = node_type;
    lrn->lrn_user_type = user_type;

    lrn->lrn_statically_allocated = !!statically_allocated;

    lrn->lrn_cb = cb;

    CIRCLEQ_ENTRY_INIT(&lrn->lrn_lentry);
    CIRCLEQ_INIT(&lrn->lrn_head);

    if (user_type != LREG_USER_TYPE_ROOT)
    {
        DBG_LREG_NODE(LL_DEBUG, lrn, "");
    }
    else
    {
        lrn->lrn_install_state = LREG_NODE_INSTALLED;
    }
}

static lreg_install_int_ctx_t
lreg_root_node_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                  struct lreg_value *lreg_val)
{
    (void)lrn;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
	snprintf(LREG_VALUE_TO_OUT_STR(lreg_val), LREG_VALUE_STRING_MAX,
                 "root");
        break;
    default:
        break;
    }
    return 0;
}

static lreg_svc_thread_t
lreg_svc_thread(void *arg)
{
    struct thread_ctl *tc = arg;

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");

    /* Cause this thread to sleep at the top of 'THREAD_LOOP_WITH_CTL'.
     */
    thread_ctl_set_user_pause_usec(tc, THR_PAUSE_DEFAULT_USECS);

    THREAD_LOOP_WITH_CTL(tc)
    {
        lreg_install_queued_nodes();
    }

    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye");

    return (void *)0;
}

static void
lreg_svc_enable(void)
{
    thread_ctl_run(&lRegSvcThreadCtl);
}

static init_ctx_t
lreg_svc_thread_start(void)
{
    int rc = thread_create(lreg_svc_thread, &lRegSvcThreadCtl, "lreg_svc",
                           NULL, NULL);

    FATAL_IF(rc, "pthread_create(): %s", strerror(errno));
}

init_ctx_t
lreg_subsystem_init(void)
{
    NIOVA_ASSERT(!lRegInitialized);

    spinlock_init(&lRegLock);
    NIOVA_ASSERT_strerror(!pthread_rwlock_init(&lRegRwLock, NULL));

    CIRCLEQ_INIT(&lRegInstallingNodes);

    lRegRootNode.lrn_root_node = 1;

    lreg_node_init(&lRegRootNode, LREG_NODE_TYPE_OBJECT, LREG_USER_TYPE_ROOT,
                   lreg_root_node_cb, true);

    lreg_svc_thread_start();
    lreg_svc_enable();

    lRegInitialized = true;

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
}

destroy_ctx_t
lreg_subsystem_destroy(void)
{
    int rc = thread_halt_and_destroy(&lRegSvcThreadCtl);

    spinlock_destroy(&lRegLock);
    NIOVA_ASSERT_strerror(!pthread_rwlock_destroy(&lRegRwLock));

    SIMPLE_LOG_MSG(LL_WARN, "svc thread: %s", strerror(-rc));
}

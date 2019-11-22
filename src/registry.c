/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#include "log.h"
#include "lock.h"
#include "registry.h"
#include "util_thread.h"
#include "ev_pipe.h"

REGISTRY_ENTRY_FILE_GENERATE;

static struct lreg_node_list lRegInstallingNodes;
static struct lreg_node      lRegRootNode;
static bool                  lRegInitialized = false;
static spinlock_t            lRegLock;
static pthread_rwlock_t      lRegRwLock;
static struct ev_pipe        lRegEVP;

const char *lRegSeparatorString = "::";

typedef bool (*lrn_walk_cb_t)(struct lreg_node *, void *);

struct lreg_node_lookup_handle
{
    const char       *lnlh_name;
    struct lreg_node *lnlh_node;
};

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

/**
 * lreg_node_walk_locked - with the lock held and starting with the parent,
 *   walk the tree executing the provided callback function.
 * @parent:  root for the walk.
 * @lrn_wcb:  walk call back function.
 * @cb_arg:  opaque argument supplied to the callback.
 */
static void
lreg_node_walk_locked(const struct lreg_node *parent, lrn_walk_cb_t lrn_wcb,
                      void *cb_arg)
{
    struct lreg_node *child;

    DBG_LREG_NODE(LL_DEBUG, (struct lreg_node *)parent, "parent");

    CIRCLEQ_FOREACH(child, &parent->lrn_head, lrn_lentry)
    {
        DBG_LREG_NODE(LL_DEBUG, child, "");

        if (!lrn_wcb(child, cb_arg))
            break;
    }
}

/**
 * lreg_node_walk_locked_cb - generic callback function for a registry walk.
 * @lrn:  registry node which was provided by lreg_node_walk_locked().
 * @arg:  call back arg which was provided to lreg_node_walk_locked().
 * Return:  boolean signifying whether the walk may be stopped.
 */
static bool
lreg_node_lookup_walk_cb(struct lreg_node *lrn, void *arg)
{
    struct lreg_node_lookup_handle *lnlh = arg;
    struct lreg_value lrv;

    if (!lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_GET_NAME, lrn, &lrv) &&
        !strncmp(LREG_VALUE_TO_OUT_STR(&lrv), lnlh->lnlh_name,
                 LREG_VALUE_STRING_MAX))
    {
        DBG_LREG_NODE(LL_DEBUG, lrn, "found");

        lnlh->lnlh_node = lrn;

        return false;
    }

    return true;
}

/**
 * lreg_node_lookup_locked - Find the node which corresponds to the provided
 *   path.
 * @registry_path:  A string which is used to represent a registry path.
 * @lrn:  Pointer for the returned lreg node.
 */
static int
lreg_node_lookup_locked(const char *registry_path, struct lreg_node **lrn)
{
    if (!lrn)
        return -EINVAL;

    char *tmp = strndup(registry_path, LREG_VALUE_STRING_MAX);
    if (!tmp)
    {
        int rc = -errno;
        LOG_MSG(LL_ERROR, "strndup():  %s", strerror(-rc));

        return rc;
    }

    struct lreg_node *parent = lreg_root_node_get();
    struct lreg_node_lookup_handle lnlh;

    char *strtok_save_ptr = NULL;
    char *next_reg_path;

    for (next_reg_path = strtok_r(tmp, lRegSeparatorString, &strtok_save_ptr);
         next_reg_path != NULL;
         next_reg_path = strtok_r(NULL, lRegSeparatorString, &strtok_save_ptr))
    {
        SIMPLE_LOG_MSG(LL_WARN, "%s", next_reg_path);

        lnlh.lnlh_name = next_reg_path;
        lnlh.lnlh_node = NULL;

        lreg_node_walk_locked(parent, lreg_node_lookup_walk_cb, &lnlh);

        parent = lnlh.lnlh_node;
        if (!parent)
            break;
    }

    free(tmp);

    *lrn = parent;

    return parent ? 0 : -ENOENT;
}

/**
 * lreg_node_recurse_locked - worker function used for registry recursion.
 * @parent:  current node to process.
 * @lrn_rcb:  the callback to issue.
 * @depth: the current depth.
 */
static lreg_user_int_ctx_t
lreg_node_recurse_locked(struct lreg_node *parent, lrn_recurse_cb_t lrn_rcb,
                         const int depth)
{
    int indent = (depth + 1) * 4;
    DBG_LREG_NODE(LL_WARN, parent, "here");

    struct lreg_value lrv_parent;

    int rc = lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_GET_NAME, parent,
                                   &lrv_parent);
    if (rc)
        return rc;

    SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth, 0, indent, "", '{');

    unsigned int i, num_keys = lrv_parent.get.lrv_num_keys_out;

    for (i = 0; i < num_keys; i++)
    {
        struct lreg_value lrv = {.lrv_value_idx_in = i};

        lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_READ_VAL, parent, &lrv);

        lrn_rcb(&lrv, depth, i, false);

        if (lrv.get.lrv_request_type_out == LREG_NODE_TYPE_ARRAY ||
            lrv.get.lrv_request_type_out == LREG_NODE_TYPE_OBJECT)
        {
            struct lreg_node *child;
            CIRCLEQ_FOREACH(child, &parent->lrn_head, lrn_lentry)
            {
                lreg_node_recurse_locked(child, lrn_rcb, depth + 1);
                if (child != CIRCLEQ_LAST(&parent->lrn_head))
                    SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth + 1, 0,
                                   indent, "", ',');
            }

        }
        lrn_rcb(&lrv, depth, i, true);
        if (i < num_keys - 1)
            SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth, 0, indent, "", ',');
    }

    SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth, 0, indent, "", '}');

    return 0;
}

lreg_user_ctx_t
lreg_node_recurse_json_cb(struct lreg_value *lrv, const int depth,
                          const int element_number, const bool done)
{
    int indent = (depth + 1) * 4;

    if (LREG_VALUE_TO_REQ_TYPE(lrv) == LREG_NODE_TYPE_ARRAY ||
        LREG_VALUE_TO_REQ_TYPE(lrv) == LREG_NODE_TYPE_OBJECT)
    {
        if (done)
        {
            SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth, element_number,
                           indent, "",
                           LREG_VALUE_TO_REQ_TYPE(lrv) ==
                           LREG_NODE_TYPE_ARRAY ?
                           ']' : '}');
        }
        else
        {
            SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %s\"%s\": %c", depth,
                           element_number, indent, "",
                           element_number ? "" : "",
                           lrv->lrv_key_string,
                           LREG_VALUE_TO_REQ_TYPE(lrv) ==
                           LREG_NODE_TYPE_ARRAY ?
                           '[' : '{');
        }
    }
    else if (LREG_VALUE_TO_REQ_TYPE(lrv) == LREG_NODE_TYPE_STRING && !done)
    {
        SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s%s\"%s\": \"%s\"", depth,
                       element_number, indent, "",
                       element_number ? "" : "",
                       lrv->lrv_key_string,
                       LREG_VALUE_TO_OUT_STR(lrv));
    }
    else if (!done)
    {
        SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s%s\"%s\": %lu", depth,
                       element_number, indent, "",
                       element_number ? "" : "",
                       lrv->lrv_key_string,
                       lrv->get.lrv_value_out.lrv_unsigned_val);
    }
}

/**
 * lreg_node_recurse - public function which attempts to lookup and recurse the
 *   provided registry path.
 * @registry_path:  the path to recurse.
 * @lrn_rcb:  the callback to issue while recursing the registry nodes.
 */
lreg_user_int_ctx_t
//lreg_node_recurse(const char *registry_path, lrn_recurse_cb_t lrn_rcb)
lreg_node_recurse(const char *registry_path)
{
    LREG_SUBSYS_RDLOCK;

    struct lreg_node *recurse_root = NULL;

    int rc = lreg_node_lookup_locked(registry_path, &recurse_root);

    if (!rc && recurse_root)
    {
        DBG_LREG_NODE(LL_DEBUG, recurse_root, "got it");

        rc = lreg_node_recurse_locked(recurse_root, lreg_node_recurse_json_cb,
                                      0);
    }
    else
    {
        log_msg(LL_DEBUG, "lreg_node_lookup_locked() %s: %s",
                registry_path, strerror(-rc));
    }

    LREG_SUBSYS_UNLOCK;

    return rc;
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

static lreg_svc_ctx_t
lreg_node_install_add(struct lreg_node *child, struct lreg_node *parent)
{
    DBG_LREG_NODE(LL_TRACE, parent, "parent");
    DBG_LREG_NODE(LL_DEBUG, child, "parent=%p", parent);

    LREG_SUBSYS_WRLOCK;

    const bool install_complete_ok = lreg_node_install_complete(child);
    NIOVA_ASSERT(install_complete_ok);

    CIRCLEQ_INSERT_HEAD(&parent->lrn_head, child, lrn_lentry);

    LREG_SUBSYS_UNLOCK;
}

/**
 * lreg_node_install - executed exclusively by the registry service thread
 *    (lreg_svc_ctx_t) when installing new nodes from the lRegInstallingNodes
 *    queue.
 * @child:  the child being installed
 * NOTES:  the parent list head (lrn_head) is exclusively owned by the
 *    registry service thread.
 */
static lreg_svc_ctx_t // or init_ctx_t
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
        -ESTALE :
        lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_INSTALL_NODE, child, NULL);

    if (!rc)
    {
        lreg_node_install_add(child, parent);
    }
    else
    {
        int destroy_rc =
            lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_DESTROY_NODE, child, NULL);

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

static lreg_install_ctx_t
lreg_node_queue_for_install(struct lreg_node *child)
{
    LREG_NODE_INSTALL_LOCK;

    CIRCLEQ_INSERT_TAIL(&lRegInstallingNodes, child, lrn_lentry);

    LREG_NODE_INSTALL_UNLOCK;

    ev_pipe_notify(&lRegEVP);
}

/**
 * lreg_node_queue_for_install - Inserts a registry node into the installation
 *    queue.
 * @child: The child node to be installed.
 * @parent:  The child's parent node.
 */
lreg_install_int_ctx_t
lreg_node_install_prepare(struct lreg_node *child, struct lreg_node *parent)
{
    NIOVA_ASSERT(child && parent);
    NIOVA_ASSERT(child != parent);

    if (!(LREG_NODE_IS_OBJECT(parent) ||
          (LREG_NODE_IS_ARRAY(parent) &&
           parent->lrn_user_type == child->lrn_user_type)))
        return -EINVAL;

    else if (!lreg_node_needs_installation(child))
        return -EALREADY;

    else if (!lreg_node_install_prep_ok(child))
        return -EALREADY;

    DBG_LREG_NODE(LL_DEBUG, parent, "parent");
    DBG_LREG_NODE(LL_DEBUG, child, "child parent=%p", parent);

    child->lrn_parent_for_install_only = parent;

    init_ctx() ? lreg_node_install(child) : lreg_node_queue_for_install(child);

    return 0;
}

/**
 * lreg_node_init - public method for initializing a registry node prior to
 *    installation.
 * @lrn: Pointer to the registry node which is to be initialized.
 * @node_type: The type of the registry node.
 * @user_type: The subsystem to which the node belongs.
 * @cb: Callback function used for queries and modifications.
 * @cb_arg:  Callback function argument.
 * @statically_allocated:  Set to 'true' when the memory behind *lrn has not
 *    been allocated from the heap.  This is typically the case with log msg
 *    lrn objects.
 */
void
lreg_node_init(struct lreg_node *lrn, enum lreg_node_types node_type,
               enum lreg_user_types user_type, lrn_cb_t cb, void *cb_arg,
               bool statically_allocated)
{
    lrn->lrn_node_type = node_type;
    lrn->lrn_user_type = user_type;

    lrn->lrn_statically_allocated = !!statically_allocated;

    lrn->lrn_cb = cb;
    lrn->lrn_cb_arg = cb_arg;

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

static util_thread_ctx_t
lreg_util_thread_cb(const struct epoll_handle *eph)
{
    FUNC_ENTRY(LL_DEBUG);

    if (eph->eph_fd != evp_read_fd_get(&lRegEVP))
    {
        LOG_MSG(LL_ERROR, "invalid fd=%d, expected %d",
                eph->eph_fd, evp_read_fd_get(&lRegEVP));

        return;
    }

    ssize_t rc = ev_pipe_drain(&lRegEVP);
    if (rc < 0 && (rc != -EWOULDBLOCK || rc != -EAGAIN))
        LOG_MSG(LL_WARN, "ev_pipe_drain() %s", strerror(-rc));

    SIMPLE_LOG_MSG(LL_DEBUG, "ev_pipe_drain()=%zd", rc);

    lreg_install_queued_nodes();

    evp_increment_reader_cnt(&lRegEVP);
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
                   lreg_root_node_cb, NULL, true);

    lRegInitialized = true;

    int rc = ev_pipe_setup(&lRegEVP);
    FATAL_IF((rc), "ev_pipe_setup(): %s", strerror(-rc));

    evp_increment_reader_cnt(&lRegEVP);

    rc = util_thread_install_event_src(evp_read_fd_get(&lRegEVP), EPOLLIN,
                                       lreg_util_thread_cb, NULL);

    FATAL_IF((rc), "util_thread_install_event_src(): %s", strerror(-rc));

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
}

destroy_ctx_t
lreg_subsystem_destroy(void)
{
    //Remove from util thread?

    spinlock_destroy(&lRegLock);
    NIOVA_ASSERT_strerror(!pthread_rwlock_destroy(&lRegRwLock));

    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye, svc thread");
}

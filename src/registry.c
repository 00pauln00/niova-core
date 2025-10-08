/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#define _GNU_SOURCE
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <sys/eventfd.h>

#include "log.h"
#include "lock.h"
#include "registry.h"
#include "util_thread.h"
#include "ev_pipe.h"
#include "init.h"
#include "io.h"

REGISTRY_ENTRY_FILE_GENERATE;

const char *lRegSeparatorString = "::";

struct lreg_node_lookup_handle
{
    const char       *lnlh_name;
    struct lreg_node *lnlh_node;
};

struct lreg_instance
{
    struct lreg_node          lri_root;
    pthread_mutex_t           lri_mutex;
    struct lreg_node_list     lri_installq;
    struct lreg_destroy_queue lri_destroyq;
    uint8_t                   lri_init:1;
    int                       lri_eventfd;
    pthread_t                 lri_pthread;
    struct epoll_handle      *lri_eph;
};

#ifdef REGISTRY_PER_THREAD
#define STATIC_STRUCT_LREG_INSTANCE static __thread struct lreg_instance
#else
#define STATIC_STRUCT_LREG_INSTANCE static struct lreg_instance
#endif

STATIC_STRUCT_LREG_INSTANCE lregThrInstance =
{
    .lri_eventfd = -1,
    .lri_mutex = PTHREAD_MUTEX_INITIALIZER,
};

#define LREG_NODE_INSTALL_LOCK   \
    pthread_mutex_lock(&lregThrInstance.lri_mutex)
#define LREG_NODE_INSTALL_UNLOCK \
    pthread_mutex_unlock(&lregThrInstance.lri_mutex)

/**
 * lreg_root_node_get - returns the root node of the local registry.
 */
struct lreg_node *
lreg_root_node_get(void)
{
    return (lregThrInstance.lri_init) ? &lregThrInstance.lri_root : NULL;
}

static bool
lreg_node_vnode_entry_exec(const struct lreg_node *parent,
                           lrn_walk_cb_t lrn_wcb,  void *cb_arg,
                           const unsigned int idx, const int depth,
                           const enum lreg_user_types user_type)
{
    struct lreg_node parent_copy = *parent;

    parent_copy.lrn_lvd.lvd_user_type = user_type;
    parent_copy.lrn_lvd.lvd_index = idx;

    SIMPLE_LOG_MSG(LL_DEBUG, "idx=%u", idx);

    return lrn_wcb(&parent_copy, cb_arg, depth);
}

static void
lreg_node_walk_vnode(const struct lreg_node *parent, lrn_walk_cb_t lrn_wcb,
                     void *cb_arg, const int depth,
                     const enum lreg_user_types user_type)
{
    NIOVA_ASSERT(parent->lrn_vnode_child);

    unsigned int cnt = parent->lrn_lvd.lvd_num_entries;

    for (unsigned int i = 0; i < cnt; i++)
    {
        unsigned int idx = parent->lrn_reverse_varray ? (cnt - 1 - i) : i;

        if (!lreg_node_vnode_entry_exec(parent, lrn_wcb, cb_arg, idx, depth,
                                        user_type))
            break;
    }
}

static void
lreg_node_walk_attached_nodes(const struct lreg_node *parent,
                              lrn_walk_cb_t lrn_wcb, void *cb_arg,
                              const int depth,
                              const enum lreg_user_types user_type)
{
    struct lreg_node *child = NULL;

    CIRCLEQ_FOREACH(child, &parent->lrn_head, lrn_lentry)
    {
        DBG_LREG_NODE(LL_DEBUG, child, "search user_type=%d, found=%d",
                      user_type, child->lrn_user_type);

        if (child->lrn_user_type != user_type &&
            user_type != LREG_USER_TYPE_ANY)
            continue;

        DBG_LREG_NODE(LL_DEBUG, child, "");

        if (!lrn_wcb(child, cb_arg, depth))
            break;
    }
}

/**
 * lreg_node_walk - with the lock held and starting with the parent,
 *   walk the tree executing the provided callback function.  @parent:
 *   root for the walk.  @lrn_wcb: walk call back function.  @cb_arg:
 *   opaque argument supplied to the callback.
 */
void
lreg_node_walk(const struct lreg_node *parent, lrn_walk_cb_t lrn_wcb,
               void *cb_arg, const int depth,
               const enum lreg_user_types user_type)
{
    DBG_LREG_NODE(LL_DEBUG, (struct lreg_node *)parent, "parent");

    return parent->lrn_vnode_child ?
        lreg_node_walk_vnode(parent, lrn_wcb, cb_arg, depth, user_type) :
        lreg_node_walk_attached_nodes(parent, lrn_wcb, cb_arg, depth,
                                      user_type);
}

/**
 * lreg_node_walk_cb - generic callback function for a registry walk.
 * @lrn:  registry node which was provided by lreg_node_walk().
 * @arg:  call back arg which was provided to lreg_node_walk().
 * Return:  boolean signifying whether the walk may be stopped.
 */
static bool
lreg_node_lookup_walk_cb(struct lreg_node *lrn, void *arg, const int depth)
{
    struct lreg_node_lookup_handle *lnlh = arg;
    struct lreg_value lrv;

    if (!lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_GET_NAME, lrn, &lrv) &&
        !strncmp(LREG_VALUE_TO_OUT_STR(&lrv), lnlh->lnlh_name,
                 LREG_VALUE_STRING_MAX))
    {
        DBG_LREG_NODE(LL_DEBUG, lrn, "found %d", depth);

        lnlh->lnlh_node = lrn;

        return false;
    }

    return true;
}

/**
 * lreg_node_lookup - Find the node which corresponds to the provided
 *   path.
 * @registry_path:  A string which is used to represent a registry path.
 * @lrn:  Pointer for the returned lreg node.
 */
static int
lreg_node_lookup(const char *registry_path, struct lreg_node **lrn)
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
        SIMPLE_LOG_MSG(LL_NOTIFY, "%s", next_reg_path);

        lnlh.lnlh_name = next_reg_path;
        lnlh.lnlh_node = NULL;

        lreg_node_walk(parent, lreg_node_lookup_walk_cb, &lnlh, -1,
                       LREG_USER_TYPE_ANY);

        parent = lnlh.lnlh_node;
        if (!parent)
            break;
    }

    free(tmp);

    *lrn = parent;

    return parent ? 0 : -ENOENT;
}

/**
 * lreg_node_recurse - worker function used for registry recursion.
 * @parent:  current node to process.
 * @lrn_rcb:  the callback to issue.
 * @depth: the current depth.
 */
static lreg_user_int_ctx_t
lreg_node_recurse_from_parent(struct lreg_node *parent,
                              lrn_recurse_cb_t lrn_rcb, const int depth)
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

        if (lrv.get.lrv_value_type_out == LREG_VAL_TYPE_ARRAY ||
            lrv.get.lrv_value_type_out == LREG_VAL_TYPE_OBJECT)
        {
            struct lreg_node *child;
            CIRCLEQ_FOREACH(child, &parent->lrn_head, lrn_lentry)
            {
                lreg_node_recurse_from_parent(child, lrn_rcb, depth + 1);
                if (child != CIRCLEQ_LAST(&parent->lrn_head))
                    SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth + 1, 0,
                                   indent, "", ',');
            }
        }
        else if (lrv.get.lrv_value_type_out == LREG_VAL_TYPE_VARRAY)
        {
            struct lreg_node varray_child = *parent;
            lreg_value_vnode_data_to_lreg_node(&lrv, &varray_child);

            unsigned int cnt = lrv.get.lrv_varray_out.lvvd_num_keys_out;

            for (unsigned int j = 0; j < cnt; j++)
            {
                unsigned int idx =
                    parent->lrn_reverse_varray ? (cnt - 1 - j) : i;

                varray_child.lrn_lvd.lvd_index = idx;
                lreg_node_recurse_from_parent(&varray_child, lrn_rcb,
                                              depth + 1);
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

    if (LREG_VALUE_TO_REQ_TYPE(lrv) == LREG_VAL_TYPE_ARRAY ||
        LREG_VALUE_TO_REQ_TYPE(lrv) == LREG_VAL_TYPE_OBJECT)
    {
        if (done)
        {
            SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %c", depth, element_number,
                           indent, "",
                           LREG_VALUE_TO_REQ_TYPE(lrv) ==
                           LREG_VAL_TYPE_ARRAY ?
                           ']' : '}');
        }
        else
        {
            SIMPLE_LOG_MSG(LL_WARN, "%d:%d %*s %s\"%s\": %c", depth,
                           element_number, indent, "",
                           element_number ? "" : "",
                           lrv->lrv_key_string,
                           LREG_VALUE_TO_REQ_TYPE(lrv) ==
                           LREG_VAL_TYPE_ARRAY ?
                           '[' : '{');
        }
    }
    else if (LREG_VALUE_TO_REQ_TYPE(lrv) == LREG_VAL_TYPE_STRING && !done)
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
    struct lreg_node *recurse_root = NULL;

    int rc = lreg_node_lookup(registry_path, &recurse_root);

    if (!rc && recurse_root)
    {
        DBG_LREG_NODE(LL_DEBUG, recurse_root, "got it");

        rc = lreg_node_recurse_from_parent(recurse_root,
                                           lreg_node_recurse_json_cb, 0);
    }
    else
    {
        log_msg(LL_DEBUG, "lreg_node_lookup() %s: %s",
                registry_path, strerror(-rc));
    }

    return rc;
}

static lreg_install_bool_ctx_t
lreg_parent_may_accept_child(struct lreg_node *child, struct lreg_node *parent)
{
    if (parent->lrn_inlined_children != child->lrn_inlined_member)
        return false;

    return true;
}

/**
 * lreg_node_install_internal - executed exclusively by the registry service
 *    thread (lreg_svc_ctx_t) when installing new nodes from the
 *    lRegInstallQueue queue.
 * @child:  the child being installed
 * NOTES:  the parent list head (lrn_head) is exclusively owned by the
 *    registry service thread.
 */
static lreg_svc_ctx_t // or init_ctx_t
lreg_node_install_internal(struct lreg_node *child)
{
    NIOVA_ASSERT(child);

    DBG_LREG_NODE(LL_DEBUG, child, "");

    struct lreg_node *parent = child->lrn_parent_for_install_only;

    NIOVA_ASSERT(parent && (!lreg_node_needs_installation(parent) ||
                            child->lrn_inlined_member));

    /* REGISTRY_PER_THREAD does not allow us to the statically initialize the
     * the list head with CIRCLEQ_HEAD_INITIALIZER().
     */
    if (parent->lrn_needs_list_init)
    {
        NIOVA_ASSERT(parent->lrn_head.cqh_first == NULL);
        NIOVA_ASSERT(parent->lrn_head.cqh_last == NULL);

        CIRCLEQ_INIT(&parent->lrn_head);

        parent->lrn_needs_list_init = 0;
    }

    /* This is really required only for LREG_VAL_TYPE_ARRAY and
     * LREG_VAL_TYPE_OBJECT.  Statically allocated nodes may not have
     * initialized their list heads.  Other should have called
     * lreg_node_init().
     */
    if (child->lrn_statically_allocated)
        CIRCLEQ_INIT(&child->lrn_head);

    int rc = child->lrn_may_destroy ?
        -ESTALE :
        lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_INSTALL_NODE, child, NULL);

    if (!rc)
    {
        CIRCLEQ_INSERT_HEAD(&parent->lrn_head, child, lrn_lentry);
        const bool install_complete_ok = lreg_node_install_complete(child);
        NIOVA_ASSERT(install_complete_ok);
    }

    else if (child->lrn_async_install)
    { // only if LREG_NODE_CB_OP_INSTALL_QUEUED_NODE was issued

        int destroy_rc =
            lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_DESTROY_NODE, child, NULL);

        DBG_LREG_NODE((init_ctx() ? LL_FATAL : LL_WARN), child,
                      "child install failed - install: '%s', destroy: '%s'",
                      strerror(-rc), strerror(-destroy_rc));
    }
}

static lreg_svc_ctx_t // or init_ctx_t or destroy_ctx_t
lreg_node_remove_internal(struct lreg_node *child)
{
    NIOVA_ASSERT(child && lreg_node_is_installed(child));

    struct lreg_node *parent = child->lrn_parent_for_remove_only;

    NIOVA_ASSERT(parent && !lreg_node_needs_installation(parent) &&
                 !CIRCLEQ_EMPTY(&parent->lrn_head));

    // 'removing' signifies that the removal is in progress
    const bool removing_ok = lreg_node_set_removing(child);
    NIOVA_ASSERT(removing_ok);

    DBG_LREG_NODE(LL_DEBUG, child, "child");
    DBG_LREG_NODE(LL_DEBUG, parent, "parent");

#if 1 // Be extra paranoid
    NIOVA_ASSERT(CIRCLEQ_ENTRY_IS_MEMBER(&parent->lrn_head, child, lrn_lentry));
#endif

    CIRCLEQ_REMOVE(&parent->lrn_head, child, lrn_lentry);

    const bool uninstalled_ok = lreg_node_set_uninstalled(child);
    NIOVA_ASSERT(uninstalled_ok);

    enum lreg_user_types type = child->lrn_user_type;

    // Consider *child as invalid following the cb execution
    int rc = lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_DESTROY_NODE, child, NULL);
    if (rc)
        LOG_MSG(LL_WARN, "lreg_node_exec_lrn_cb(%p:%d): %s",
                child, type, strerror(-rc));
}

/**
 * lreg_install_get_queued_node - detects a queued node, removes, and returns
 *    it.
 */
static lreg_svc_lrn_ctx_t
lreg_install_get_queued_node(void)
{
    NIOVA_ASSERT(lregThrInstance.lri_init);

    struct lreg_node *install = NULL;

    LREG_NODE_INSTALL_LOCK;
    if (!CIRCLEQ_EMPTY(&lregThrInstance.lri_installq))
    {
        install = CIRCLEQ_FIRST(&lregThrInstance.lri_installq);
        CIRCLEQ_REMOVE(&lregThrInstance.lri_installq, install, lrn_lentry);
    }
    LREG_NODE_INSTALL_UNLOCK;

    return install;
}

static lreg_svc_lrn_ctx_t
lreg_remove_get_queued_node(void)
{
    NIOVA_ASSERT(lregThrInstance.lri_init);

    struct lreg_node *remove = NULL;

    LREG_NODE_INSTALL_LOCK;

    if ((remove = STAILQ_FIRST(&lregThrInstance.lri_destroyq)))
        STAILQ_REMOVE_HEAD(&lregThrInstance.lri_destroyq, lrn_removal_lentry);

    LREG_NODE_INSTALL_UNLOCK;

    return remove;
}

/**
 * lreg_process_install_queue - called only by the service thread to install
 *    the registry nodes which are on the install queue.
 */
static lreg_svc_ctx_t
lreg_process_install_queue(void)
{
    struct lreg_node *install;

    while ((install = lreg_install_get_queued_node()))
        lreg_node_install_internal(install);
}

static lreg_svc_ctx_t
lreg_process_remove_queue(void)
{
    struct lreg_node *remove;

    while ((remove = lreg_remove_get_queued_node()))
        lreg_node_remove_internal(remove);
}

int
lreg_notify(void)
{
    FUNC_ENTRY(LL_DEBUG);
    uint64_t val = 1;

    return write(lregThrInstance.lri_eventfd, &val, sizeof(val));
}

bool
lreg_install_has_queued_nodes(void)
{
    LREG_NODE_INSTALL_LOCK;
    bool empty =
        (CIRCLEQ_EMPTY(&lregThrInstance.lri_installq) &&
         STAILQ_EMPTY(&lregThrInstance.lri_destroyq));
    LREG_NODE_INSTALL_UNLOCK;

    return !empty;
}

static lreg_install_ctx_t
lreg_node_install_queue(struct lreg_node *child)
{
    // Notify owner that node is queuing for async install
    int rc = lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_INSTALL_QUEUED_NODE, child,
                                   NULL);
    if (rc)
        DBG_LREG_NODE(LL_WARN, child,
                      "LREG_NODE_CB_OP_INSTALL_QUEUED_NODE cb failed: %s",
                      strerror(-rc));

    LREG_NODE_INSTALL_LOCK;

    CIRCLEQ_INSERT_TAIL(&lregThrInstance.lri_installq, child, lrn_lentry);
    child->lrn_async_install = 1;

    LREG_NODE_INSTALL_UNLOCK;

    lreg_notify();
}

static lreg_destroy_ctx_t
lreg_node_remove_queue(struct lreg_node *child)
{
    LREG_NODE_INSTALL_LOCK;

    STAILQ_INSERT_TAIL(&lregThrInstance.lri_destroyq, child, lrn_removal_lentry);
    child->lrn_async_remove = 1;

    LREG_NODE_INSTALL_UNLOCK;

    lreg_notify();
}

/**
 * lreg_node_install - Inserts a registry node into the installation queue or
 *    performs the full installation if the proper conditions are met.
 * @child: The child node to be installed.
 * @parent:  The child's parent node.
 */
lreg_install_int_ctx_t
lreg_node_install(struct lreg_node *child, struct lreg_node *parent)
{
    if (destroy_ctx())
        return 0;

    NIOVA_ASSERT(child && parent);
    NIOVA_ASSERT(child != parent);

    // Inlined and non-inlined children may not exist inside the same parent
    if (!lreg_parent_may_accept_child(child, parent))
        return -EINVAL;

    /* May install in this thread ctx if:
     * 1 - we're in initialization context
     * 2 - this thread runs the registry subsys
     * 3 - the parent has yet to be installed and this child is a sub object of
     *     the parent's structure.
     * 4 - xxx registry runs in single threaded (io-uring) mode
     */
    const bool install_here = (init_ctx() || lreg_thread_ctx() ||
                               (child->lrn_inlined_member &&
                                lreg_node_needs_installation(parent)));

    if (!lreg_node_needs_installation(child))
        return -EALREADY;

    else if (!lreg_node_install_prep_ok(child))
        return -EAGAIN;

    DBG_LREG_NODE(LL_DEBUG, parent, "parent");
    DBG_LREG_NODE(LL_DEBUG, child, "child (install-here=%d)", install_here);

    child->lrn_parent_for_install_only = parent;

    install_here ?
        lreg_node_install_internal(child) : lreg_node_install_queue(child);

    return 0;
}

int
lreg_node_remove(struct lreg_node *child, struct lreg_node *parent)
{
    if (!child || !parent || parent == child ||
        !lreg_node_has_children(parent))
        return -EINVAL;

    else if (!lreg_node_is_installed(child) || !lreg_node_is_installed(parent))
        return -EALREADY;

    else if (lreg_node_has_children(child) &&
             !lreg_node_children_are_inlined(child))
        return -EBUSY;

    const bool remove_here = init_ctx() || destroy_ctx() || lreg_thread_ctx();

    DBG_LREG_NODE(LL_DEBUG, parent, "parent");
    DBG_LREG_NODE(LL_DEBUG, child, "child (remove-here=%d)", remove_here);

    /* Capture the parent pointer here.  The above check,
     * lreg_node_has_children(), will protect the parent from removal via
     * this method.
     */
    child->lrn_parent_for_remove_only = parent;

    remove_here ?
        lreg_node_remove_internal(child) : lreg_node_remove_queue(child);

    return 0;
}

/**
 * lreg_node_wait_for_install_state_change - wait until the lrn state has
 *    transitioned from the provided state.  Note that we don't wait for a
 *    specific state since
 */
int
lreg_node_wait_for_install_state(const struct lreg_node *lrn,
                                 enum lreg_node_states state)
{
    if (!lrn)
        return -EINVAL;

    ssize_t remaining_ms = 60000;

    while (!(lrn->lrn_install_state & state))
    {
        size_t sleep_ms = MIN(1000, remaining_ms);
        struct timespec ts = {0}, rem = {0};

        msec_2_timespec(&ts, sleep_ms);

        for (int rc = -1; rc != 0; ts = rem)
        {
            rc = nanosleep(&ts, &rem);
            if (rc)
                NIOVA_ASSERT(errno == EINTR);
        }

        remaining_ms -= sleep_ms;

        FATAL_IF(remaining_ms <= 0, "");
        if (remaining_ms <= 0)
            return -ETIMEDOUT;
    }

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
lreg_node_init(struct lreg_node *lrn, enum lreg_user_types user_type,
               lrn_cb_t cb, void *cb_arg, enum lreg_init_options opts)
{
    if (!lrn)
        return;

    if (!(opts & LREG_INIT_OPT_STATIC))
        memset(lrn, 0, sizeof(*lrn));

    lrn->lrn_user_type = user_type;

    lrn->lrn_statically_allocated = !!(opts & LREG_INIT_OPT_STATIC);
    lrn->lrn_ignore_items_with_value_zero =
        !!(opts & LREG_INIT_OPT_IGNORE_NUM_VAL_ZERO);
    lrn->lrn_reverse_varray = !!(opts & LREG_INIT_OPT_REVERSE_VARRAY);
    lrn->lrn_inlined_member = !!(opts & LREG_INIT_OPT_INLINED_MEMBER);
    lrn->lrn_inlined_children = !!(opts & LREG_INIT_OPT_INLINED_CHILDREN);
    lrn->lrn_cb = cb;
    lrn->lrn_cb_arg = cb_arg;

    CIRCLEQ_ENTRY_INIT(&lrn->lrn_lentry);
    CIRCLEQ_INIT(&lrn->lrn_head);

    if (user_type != LREG_USER_TYPE_ROOT)
        DBG_LREG_NODE(LL_DEBUG, lrn, "");
    else
        lrn->lrn_install_state = LREG_NODE_INSTALLED;
}

static util_thread_ctx_reg_int_t
lreg_root_node_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                  struct lreg_value *lv)
{
    if (!lrn || !lv)
        return EINVAL;

    switch (op)
    {
    case LREG_NODE_CB_OP_GET_NAME:
        /* This is imprecise since only a subset of LREG_USERs are children
         * of the root object. Xxx
         */
        lv->get.lrv_num_keys_out = LREG_USER_TYPE_ANY;

        // The root object is anonymous and has no "key".
        lv->get.lrv_value_type_out = LREG_VAL_TYPE_ANON_OBJECT;
        strncpy(lv->lrv_key_string, "ROOT", LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
    case LREG_NODE_CB_OP_READ_VAL: // Xxx this can be modified to allow reads
                                   //   of

        return -EOPNOTSUPP;

    default:
        break;
    }
    return 0;
}

int
lreg_util_processor(void)
{
    if (!lreg_thread_ctx())
        return -EXDEV;

    lreg_process_install_queue();
    lreg_process_remove_queue();

    return 0;
}

int
lreg_get_eventfd(void)
{
    return lregThrInstance.lri_eventfd;
}

static util_thread_ctx_t
lreg_util_thread_cb(const struct epoll_handle *eph, uint32_t events)
{
    FUNC_ENTRY(LL_DEBUG);

    (void)events;

    if (eph->eph_fd != lregThrInstance.lri_eventfd)
    {
        LOG_MSG(LL_ERROR, "invalid fd=%d, expected %d",
                eph->eph_fd, lregThrInstance.lri_eventfd);

        return;
    }

    eventfd_t x = {0};

    /* Reap the FD before processing the work queue, otherwise a livelock may
     * occur where an item is on the list w/out an accompanying event in
     * the fd.
     */
    ssize_t rrc = niova_io_read(lregThrInstance.lri_eventfd, (char *)&x,
                                sizeof(eventfd_t));
    if (rrc == -EAGAIN)
        return;

    FATAL_IF(rrc != sizeof(eventfd_t),
             "Invalid read size (%ld) from eventfd read x=%lu", rrc, x);

    int rc = lreg_util_processor();
    if (rc == -EXDEV)
    {
        SIMPLE_LOG_MSG(LL_WARN, "util thread is not longer lreg handler");
        lreg_notify();
    }
    else
    {
        FATAL_IF(rc, "lreg_util_processor(): %s", strerror(-rc));
    }
}

#define LREG_NODE_INSTALL_COMPLETION_WAIT_USEC 30000000
lreg_install_int_ctx_t
lreg_node_wait_for_completion(const struct lreg_node *lrn, bool install)
{
    if (!lrn)
        return -EINVAL;

    bool (*tfunc)(const struct lreg_node *) = install ?
        lreg_node_is_installed : lreg_node_is_removed;

    if (!tfunc(lrn))
    {
        if ((init_ctx() || lreg_thread_ctx() || lrn->lrn_inlined_member))
            DBG_LREG_NODE(LL_FATAL, (struct lreg_node *)lrn,
                          "invalid installation or removal context");

        for (int i = 0; i < LREG_NODE_INSTALL_COMPLETION_WAIT_USEC; i++)
        {
            if (tfunc(lrn))
                return 0;
            else
                usleep(1);
        }
        return -ETIMEDOUT;
    }

    return 0;
}

void
lreg_set_thread_ctx(pthread_t pthread_id)
{
    lregThrInstance.lri_pthread = pthread_id;
}

bool
lreg_thread_ctx(void)
{
    return (lregThrInstance.lri_init &&
            pthread_self() == lregThrInstance.lri_pthread);
}

int
lreg_remove_event_src(void)
{
    return util_thread_remove_event_src(lregThrInstance.lri_eph);
}

static init_ctx_t NIOVA_CONSTRUCTOR(LREG_SUBSYS_CTOR_PRIORITY)
lreg_subsystem_init(void)
{
    NIOVA_ASSERT(!lregThrInstance.lri_init);

    CIRCLEQ_INIT(&lregThrInstance.lri_installq);
    STAILQ_INIT(&lregThrInstance.lri_destroyq);

    lregThrInstance.lri_root.lrn_root_node = 1;

    lreg_node_init(&lregThrInstance.lri_root, LREG_USER_TYPE_ROOT,
                   lreg_root_node_cb, NULL, LREG_INIT_OPT_STATIC);

    lregThrInstance.lri_eventfd = eventfd(0, EFD_NONBLOCK);
    FATAL_IF((lregThrInstance.lri_eventfd < 0), "eventfd(): %s",
             strerror(-lregThrInstance.lri_eventfd));

    int rc = util_thread_install_event_src(lregThrInstance.lri_eventfd, EPOLLIN,
                                           lreg_util_thread_cb, NULL,
                                           &lregThrInstance.lri_eph);

    FATAL_IF((rc || !lregThrInstance.lri_eph),
             "util_thread_install_event_src(): %s",
             strerror(-rc));

    lregThrInstance.lri_init = true;

    /* NOTE:  util_thread_subsytem_init() calls lreg_set_thread_ctx().  This
     *        is due to the constructor startup order which prioritizes
     *        registry initialization.
     */

    SIMPLE_LOG_MSG(LL_DEBUG, "hello lregThrInstance.lri_eventfd=%d",
                   lregThrInstance.lri_eventfd);
}

static destroy_ctx_t NIOVA_DESTRUCTOR(LREG_SUBSYS_CTOR_PRIORITY)
lreg_subsystem_destroy(void)
{
    //Remove from util thread?

    SIMPLE_LOG_MSG(LL_DEBUG,
                   "goodbye, svc thread lregThrInstance.lri_eventfd=%d",
                   lregThrInstance.lri_eventfd);

    if (lregThrInstance.lri_eventfd >= 0)
    {
        int rc = close(lregThrInstance.lri_eventfd);
        if (rc)
            SIMPLE_LOG_MSG(LL_ERROR,
                           "close(lregThrInstance.lri_eventfd=%d): %s",
                           lregThrInstance.lri_eventfd, strerror(-rc));
    }
}

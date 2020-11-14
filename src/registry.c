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
#include "init.h"

REGISTRY_ENTRY_FILE_GENERATE;

static struct lreg_node_list lRegInstallingNodes;
static struct lreg_node lRegRootNode;
static bool lRegInitialized = false;
static spinlock_t lRegLock;
static pthread_rwlock_t lRegRwLock;
static struct ev_pipe lRegEVP;

const char *lRegSeparatorString = "::";

struct lreg_node_lookup_handle
{
    const char       *lnlh_name;
    struct lreg_node *lnlh_node;
};

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

    unsigned int max_idx = parent->lrn_lvd.lvd_num_entries;

    if (parent->lrn_reverse_varray)
    {
        for (unsigned int i = max_idx - 1; i >= 0; i--)
            if (!lreg_node_vnode_entry_exec(parent, lrn_wcb, cb_arg, i, depth,
                                            user_type))
                break;
    }
    else
    {
        for (unsigned int i = 0; i < max_idx; i++)
            if (!lreg_node_vnode_entry_exec(parent, lrn_wcb, cb_arg, i, depth,
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

            if (parent->lrn_reverse_varray)
            {
                for (unsigned int j =
                         lrv.get.lrv_varray_out.lvvd_num_keys_out - 1;
                     j >= 0; j--)
                {
                    varray_child.lrn_lvd.lvd_index = j;
                    lreg_node_recurse_from_parent(&varray_child, lrn_rcb,
                                                  depth + 1);
                }
            }
            else
            {
                for (unsigned int j = 0;
                     j < lrv.get.lrv_varray_out.lvvd_num_keys_out; j++)
                {
                    varray_child.lrn_lvd.lvd_index = j;
                    lreg_node_recurse_from_parent(&varray_child, lrn_rcb,
                                                  depth + 1);
                }
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

    const bool install_complete_ok = lreg_node_install_complete(child);
    NIOVA_ASSERT(install_complete_ok);

    CIRCLEQ_INSERT_HEAD(&parent->lrn_head, child, lrn_lentry);
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

    /* This is really required only for LREG_VAL_TYPE_ARRAY and
     * LREG_VAL_TYPE_OBJECT.
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

        DBG_LREG_NODE((init_ctx() ? LL_FATAL : LL_WARN), child,
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
    if (destroy_ctx())
        return 0;

    NIOVA_ASSERT(child && parent);
    NIOVA_ASSERT(child != parent);

//    if (parent->lrn_user_type == child->lrn_user_type)
//        return -EINVAL;

    if (!lreg_node_needs_installation(child))
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

static util_thread_ctx_t
lreg_util_thread_cb(const struct epoll_handle *eph, uint32_t events)
{
    FUNC_ENTRY(LL_DEBUG);

    if (eph->eph_fd != evp_read_fd_get(&lRegEVP))
    {
        LOG_MSG(LL_ERROR, "invalid fd=%d, expected %d",
                eph->eph_fd, evp_read_fd_get(&lRegEVP));

        return;
    }

    EV_PIPE_RESET(&lRegEVP);

    lreg_install_queued_nodes();
}

static init_ctx_t NIOVA_CONSTRUCTOR(LREG_SUBSYS_CTOR_PRIORITY)
lreg_subsystem_init(void)
{
    NIOVA_ASSERT(!lRegInitialized);

    spinlock_init(&lRegLock);
    NIOVA_ASSERT_strerror(!pthread_rwlock_init(&lRegRwLock, NULL));

    CIRCLEQ_INIT(&lRegInstallingNodes);

    lRegRootNode.lrn_root_node = 1;

    lreg_node_init(&lRegRootNode, LREG_USER_TYPE_ROOT, lreg_root_node_cb,
                   NULL, LREG_INIT_OPT_STATIC);

    lRegInitialized = true;

    int rc = ev_pipe_setup(&lRegEVP);
    FATAL_IF((rc), "ev_pipe_setup(): %s", strerror(-rc));

    rc = util_thread_install_event_src(evp_read_fd_get(&lRegEVP), EPOLLIN,
                                       lreg_util_thread_cb, NULL, NULL);

    FATAL_IF((rc), "util_thread_install_event_src(): %s", strerror(-rc));

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
}

static destroy_ctx_t NIOVA_DESTRUCTOR(LREG_SUBSYS_CTOR_PRIORITY)
lreg_subsystem_destroy(void)
{
    //Remove from util thread?

    spinlock_destroy(&lRegLock);
    NIOVA_ASSERT_strerror(!pthread_rwlock_destroy(&lRegRwLock));

    SIMPLE_LOG_MSG(LL_DEBUG, "goodbye, svc thread");
}

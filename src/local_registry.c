/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include <pthread.h>

#include "log.h"
#include "atomic.h"
#include "local_registry.h"

REGISTRY_ENTRY_FILE_GENERATE;

static struct lreg_node lRegRootNode;
static bool             lRegInitialized = false;
static pthread_rwlock_t lRegRwLock = PTHREAD_RWLOCK_INITIALIZER;

#define LREG_SUBSYS_WRLOCK pthread_rwlock_wrlock(&lRegRwLock)
#define LREG_SUBSYS_RDLOCK pthread_rwlock_rdlock(&lRegRwLock)
#define LREG_SUBSYS_UNLOCK pthread_rwlock_unlock(&lRegRwLock)

static lreg_install_int_ctx_t
lreg_node_install_check_passes_wrlocked(const struct lreg_node *child,
                                        const struct lreg_node *parent)
{
    if (!(parent->lrn_node_type == LREG_NODE_TYPE_OBJECT ||
          (parent->lrn_node_type == LREG_NODE_TYPE_ARRAY &&
           parent->lrn_user_type == child->lrn_user_type)))
        return -EINVAL;

    /* Grab the name of the child object.
     */
    struct lreg_value child_val;
    int rc = child->lrn_cb(LREG_NODE_CB_OP_GET_NAME,
                           (struct lreg_node *)child, &child_val);
    if (rc)
        return rc;

    struct lreg_node *sibling = NULL;

    SLIST_FOREACH(sibling, &parent->lrn_head, lrn_lentry)
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

lreg_install_int_ctx_t
lreg_node_install(struct lreg_node *child, struct lreg_node *parent)
{
    NIOVA_ASSERT(lRegInitialized);
    NIOVA_ASSERT(!lreg_node_needs_installation(parent));

    if (!lreg_node_needs_installation(child))
        return -EALREADY;

    else if (!child->lrn_cb_arg)
        return -EINVAL;

    else if (!lreg_node_install_prep_ok(child))
        return -EALREADY;

    SLIST_INIT(&child->lrn_head);

    NIOVA_ASSERT(SLIST_ENTRY_DETACHED(&child->lrn_lentry));

    LREG_SUBSYS_WRLOCK;

    int rc = lreg_node_install_check_passes_wrlocked(child, parent);
    if (!rc)
    {
        rc = child->lrn_cb(LREG_NODE_CB_OP_INSTALL_NODE, child, NULL);
        if (!rc)
        {
            const bool install_complete_ok = lreg_node_install_complete(child);
            NIOVA_ASSERT(install_complete_ok);

            SLIST_INSERT_HEAD(&parent->lrn_head, child, lrn_lentry);
        }
    }

    LREG_SUBSYS_UNLOCK;

    DBG_LREG_NODE(LL_DEBUG, parent, "parent");
    DBG_LREG_NODE(LL_DEBUG, child, "child");

    return rc;
}

lreg_install_int_ctx_t
lreg_node_install_in_root(struct lreg_node *child)
{
    return lreg_node_install(child, &lRegRootNode);
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

    SLIST_ENTRY_INIT(&lrn->lrn_lentry);
    SLIST_INIT(&lrn->lrn_head);

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
	snprintf(lreg_val->lrv_string, LREG_VALUE_STRING_MAX, "root");
        break;
    default:
        break;
    }
    return 0;
}

init_ctx_t
lreg_subsystem_init(void)
{
    NIOVA_ASSERT(!lRegInitialized);

    lRegRootNode.lrn_root_node = 1;

    lreg_node_init(&lRegRootNode, LREG_NODE_TYPE_OBJECT, LREG_USER_TYPE_ROOT,
                   lreg_root_node_cb, true);

    lRegInitialized = true;

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */

#include "niova_backtrace.h"

#include "ctor.h"
#include "init.h"
#include "registry.h"
#include "log.h"
//#include "ctl_interface.h"

REGISTRY_ENTRY_FILE_GENERATE;

LREG_ROOT_ENTRY_GENERATE(test_entry_init_ctx, LREG_USER_TYPE_UNIT_TEST0);
LREG_ROOT_ENTRY_GENERATE(test_entry, LREG_USER_TYPE_UNIT_TEST1);

static int
null_lrn_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
            struct lreg_value *lv)
{
    (void)op;
    (void)lrn;
    (void)lv;

    return 0;
}

static struct lreg_node null_lrn =
{.lrn_cb = null_lrn_cb,
 .lrn_user_type = LREG_USER_TYPE_UNIT_TEST2,
 .lrn_statically_allocated = 1};

static struct lreg_node inlined_child_lrn =
{.lrn_cb = null_lrn_cb,
 .lrn_user_type = LREG_USER_TYPE_UNIT_TEST4,
 .lrn_inlined_member = 1};

static void
registry_test_wait_for_install_or_removal(struct lreg_node *lrn, bool install)
{
    int rc = lreg_node_wait_for_completion(lrn, install);
    NIOVA_ASSERT(!rc);
}

static void
registry_test_install_and_remove_node(void)
{
    struct lreg_node *lrn = LREG_ROOT_ENTRY_PTR(test_entry);

    if (lreg_node_is_installed(lrn) || !lrn->lrn_statically_allocated ||
        lrn->lrn_async_install || lrn->lrn_root_node ||
        lrn->lrn_vnode_child || lrn->lrn_inlined_member ||
        lrn->lrn_async_remove)
    {
        DBG_LREG_NODE(LL_FATAL, lrn, "invalid state detected (pre)");
    }

    int rc = lreg_node_install(lrn, lreg_root_node_get());
    if (rc || !lrn->lrn_async_install || lrn->lrn_async_remove)
        DBG_LREG_NODE(LL_FATAL, lrn,
                      "invalid state detected (status=%s)",
                      strerror(-rc));

    registry_test_wait_for_install_or_removal(lrn, true);

    rc = lreg_node_remove(lrn, lreg_root_node_get());
    if (rc || !lrn->lrn_async_install || !lrn->lrn_async_remove)
        DBG_LREG_NODE(LL_FATAL, lrn,
                      "invalid state detected (status=%s)",
                      strerror(-rc));

    registry_test_wait_for_install_or_removal(lrn, false);
}

int
main(void)
{
    FUNC_ENTRY(LL_WARN);
    registry_test_install_and_remove_node();

    return 0;
}

/**
 * registry_test_init - acts as a unit test in the init_ctx() mode where all
 *    registry ops may be done synchronously w/out the help of the util thread.
 *    Here the basic cases are tested.
 */
static init_ctx_t NIOVA_CONSTRUCTOR(UNIT_TEST_CTOR_PRIORITY)
registry_test_init(void)
{
    FUNC_ENTRY(LL_WARN);

    // Install via macro which wraps lreg_node_install()
    LREG_ROOT_ENTRY_INSTALL(test_entry_init_ctx);

    struct lreg_node *lrn = LREG_ROOT_ENTRY_PTR(test_entry_init_ctx);

    if (!lreg_node_is_installed(lrn) || !lrn->lrn_statically_allocated ||
        lrn->lrn_async_install || lrn->lrn_root_node ||
        lrn->lrn_vnode_child || lrn->lrn_inlined_member ||
        lrn->lrn_async_remove)
    {
        DBG_LREG_NODE(LL_FATAL, lrn, "invalid state detected");
    }

    lreg_node_init(&null_lrn, LREG_USER_TYPE_UNIT_TEST2, null_lrn_cb, NULL,
                   LREG_INIT_OPT_INLINED_CHILDREN);
    NIOVA_ASSERT(lreg_node_children_are_inlined(&null_lrn));

    /* Removing node before installation should fail w/ -EINVAL since, in this
     * case, the parent has no children attached.
     */
    int rc = lreg_node_remove(&null_lrn, lrn);
    NIOVA_ASSERT(rc == -EINVAL);

    // Install an 'inlined' child into the null_lrn
    rc = lreg_node_install(&inlined_child_lrn, &null_lrn);
    NIOVA_ASSERT(!rc);

    // Install a test node onto our root lrn
    rc = lreg_node_install(&null_lrn, lrn);
    NIOVA_ASSERT(!rc);

    // Adding node again should fail w/ -EALREADY
    rc = lreg_node_install(&null_lrn, lrn);
    NIOVA_ASSERT(rc == -EALREADY);

    /* Removing a node which has not yet been installed when the parent has
     * at least one entry.
     */
    struct lreg_node test_lrn = {.lrn_cb = null_lrn_cb,
        .lrn_user_type = LREG_USER_TYPE_UNIT_TEST3};

    rc = lreg_node_remove(&test_lrn, &null_lrn);
    NIOVA_ASSERT(rc == -EALREADY);

    // Will fail because test_lrn is not an inlined member
    rc = lreg_node_install(&test_lrn, &null_lrn);
    NIOVA_ASSERT(rc == -EINVAL);

    // Removing 'lrn' should fail w/ -EBUSY since it has a child
    rc = lreg_node_remove(lrn, lreg_root_node_get());
    NIOVA_ASSERT(rc == -EBUSY);
}

static destroy_ctx_t NIOVA_DESTRUCTOR(UNIT_TEST_CTOR_PRIORITY)
registry_test_destroy(void)
{
    FUNC_ENTRY(LL_WARN);

    struct lreg_node *lrn = LREG_ROOT_ENTRY_PTR(test_entry_init_ctx);

    // null_lrn should still be installed
    NIOVA_ASSERT(!CIRCLEQ_EMPTY(&lrn->lrn_head));
    NIOVA_ASSERT((CIRCLEQ_FIRST(&lrn->lrn_head) ==
                  CIRCLEQ_LAST(&lrn->lrn_head)) &&
                 CIRCLEQ_FIRST(&lrn->lrn_head) == &null_lrn);

    // Removing 'lrn' should fail w/ -EBUSY since it has a child
    int rc = lreg_node_remove(lrn, lreg_root_node_get());
    NIOVA_ASSERT(rc == -EBUSY);

    NIOVA_ASSERT(lreg_node_children_are_inlined(&null_lrn));
    rc = lreg_node_remove(&null_lrn, lrn);
    NIOVA_ASSERT(!rc);

    if (!lreg_node_is_installed(lrn) || !lrn->lrn_statically_allocated ||
        lrn->lrn_async_install || lrn->lrn_root_node ||
        lrn->lrn_vnode_child || lrn->lrn_inlined_member ||
        lrn->lrn_async_remove)
    {
        DBG_LREG_NODE(LL_FATAL, lrn, "invalid state detected (pre)");
    }

    // Remove the lrn entry now w/ the macro (equiv to lreg_node_remove())
    LREG_ROOT_ENTRY_REMOVE(test_entry_init_ctx);

    if (lreg_node_is_installed(lrn) || lrn->lrn_async_remove)
    {
        DBG_LREG_NODE(LL_FATAL, lrn, "invalid state detected (post)");
    }
}

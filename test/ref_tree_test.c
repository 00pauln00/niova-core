/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "log.h"

#include "ref_tree_proto.h"

REF_TREE_HEAD(ref_tree_test_head, test_entry);

struct test_entry
{
    int val;
    REF_TREE_ENTRY(test_entry) te_tentry;
};

static int
te_cmp(const struct test_entry *a, const struct test_entry *b)
{
    if (a->val == b->val)
        return 0;

    return a->val > b->val ? 1 : -1;
}

/* Generate after the cmp function and structure are defined.
 */
REF_TREE_GENERATE(ref_tree_test_head, test_entry, te_tentry, te_cmp);

static struct test_entry *
te_construct(const struct test_entry *in)
{
    struct test_entry *new_te = niova_malloc(sizeof(struct test_entry));
    new_te->val = in->val;

    return new_te;
}

static int
te_destruct(struct test_entry *destroy)
{
    log_msg(LL_WARN, "destroy item %d@%p", destroy->val, destroy);
    niova_free(destroy);

    return 0;
}

static void
ref_tree_tests(void)
{
#define N_ITERATIONS 10
    struct test_entry te_lookup;
    struct test_entry *captured_te[N_ITERATIONS];
    struct ref_tree_test_head test_rt;

    REF_TREE_INIT(&test_rt, te_construct, te_destruct);

    int i;
    for (i = 0; i < N_ITERATIONS; i++)
    {
        int j;
        for (j = 0; j < N_ITERATIONS; j++)
        {
            te_lookup.val = j;
            struct test_entry *te =
                RT_GET_ADD(ref_tree_test_head, &test_rt, &te_lookup);

            NIOVA_ASSERT(te->te_tentry.rbe_ref_cnt == i + 1);

            if (!i)
                captured_te[j] = te;
            else
                NIOVA_ASSERT(captured_te[j] == te);
        }
    }

    for (i = 0; i < N_ITERATIONS; i++)
    {
        int j;
        for (j = 0; j < N_ITERATIONS; j++)
            RT_PUT(ref_tree_test_head, &test_rt, captured_te[j]);
    }

    for (i = 0; i < N_ITERATIONS; i++)
    {
        te_lookup.val = i;
        struct test_entry *te =
            RT_GET(ref_tree_test_head, &test_rt, &te_lookup);

        NIOVA_ASSERT(!te);
    }
}

int
main(void)
{
    ref_tree_tests();

    return 0;
}

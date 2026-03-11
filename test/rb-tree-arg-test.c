/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski
 */
#include <time.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include "common.h"
#include "log.h"
#include "random.h"
#include "tree.h"

#define RB_TREE_SIZE 16384
struct rb_xentry
{
    union
    {
        struct
        {
            RB_ENTRY(rb_xentry) rbx_tentry;
            uint32_t            rbx_key;
        };
        char x[64];
    };
};

static int
rb_xentry_cmp(const struct rb_xentry *a, const struct rb_xentry *b, void *arg)
{
    static uint64_t arg_test;
    uint64_t *test_argp = (uint64_t *)arg;

    NIOVA_ASSERT(arg_test == *test_argp);

    arg_test++;
    (*test_argp)++;

    return a->rbx_key == b->rbx_key ? 0 :
           a->rbx_key  > b->rbx_key ? 1 : -1;
}

static uint64_t test_arg;

RB_HEAD_ARG(rbx_tree, rb_xentry);
RB_GENERATE_ARG(rbx_tree, rb_xentry, rbx_tentry, rb_xentry_cmp,
                (void *)(&test_arg));

RB_PROTOTYPE_INTERNAL_ARG(rbx_tree, rb_xentry, rbx_tentry, rb_xentry_cmp);

struct rb_xentry rbxTreeEntries[RB_TREE_SIZE];

static void
rb_tree_test(void)
{
    struct rbx_tree rbx_tree;
    RB_INIT_ARG(&rbx_tree, &test_arg);

    for (unsigned int i = 0; i < RB_TREE_SIZE; i++)
    {
        rbxTreeEntries[i].rbx_key = random_get();
        RB_INSERT(rbx_tree, &rbx_tree, &rbxTreeEntries[i]);
    }
}

int
main(void)
{
    rb_tree_test();
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2020
 */
#include "common.h"
#include "log.h"

#include "queue.h"

struct circleq_entry
{
    unsigned int ce_value;
    unsigned int ce_version;
    CIRCLEQ_ENTRY(circleq_entry) ce_lentry;
};

CIRCLEQ_HEAD(circleq, circleq_entry);

#define NUM_CES 10

static void
circleq_splice_tail_test(void)
{
    struct circleq qx = CIRCLEQ_HEAD_INITIALIZER(qx);
    struct circleq qy = CIRCLEQ_HEAD_INITIALIZER(qy);

    NIOVA_ASSERT(CIRCLEQ_EMPTY(&qx));
    NIOVA_ASSERT(CIRCLEQ_EMPTY(&qy));

    struct circleq_entry ce[NUM_CES] = {0};
    struct circleq_entry *tmp;


    // Basic head insert operation
    int i;
    for (i = 0; i < NUM_CES; i++)
    {
        ce[i].ce_value = i;
        NIOVA_ASSERT(CIRCLEQ_ENTRY_DETACHED(&ce[i].ce_lentry));
        CIRCLEQ_INSERT_HEAD(&qx, &ce[i], ce_lentry);

        NIOVA_ASSERT(!CIRCLEQ_EMPTY(&qx));
        NIOVA_ASSERT(!CIRCLEQ_ENTRY_DETACHED(&ce[i].ce_lentry));
    }

    // Foreach in both directions
    int j = NUM_CES;
    CIRCLEQ_FOREACH(tmp, &qx, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == --j);

    CIRCLEQ_FOREACH_REVERSE(tmp, &qx, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == j++);

    // Try noop splice
    CIRCLEQ_SPLICE_TAIL(&qy, &qx, ce_lentry);
    NIOVA_ASSERT(CIRCLEQ_EMPTY(&qy));
    NIOVA_ASSERT(!CIRCLEQ_EMPTY(&qx));
    // Recheck the order
    j = NUM_CES;
    CIRCLEQ_FOREACH(tmp, &qx, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == --j);


    // Splice the list to an empty list - 'qy'
    CIRCLEQ_SPLICE_TAIL(&qx, &qy, ce_lentry);
    NIOVA_ASSERT(CIRCLEQ_EMPTY(&qx));
    NIOVA_ASSERT(!CIRCLEQ_EMPTY(&qy));

    // Ensure the slice destination has the correctly ordered entries
    j = NUM_CES;
    CIRCLEQ_FOREACH(tmp, &qy, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == --j);

    CIRCLEQ_FOREACH_REVERSE(tmp, &qy, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == j++);

    // Add more entries to the original list
    struct circleq_entry ce2[NUM_CES] = {0};

    for (i = 0; i < NUM_CES; i++)
    {
        ce2[i].ce_value = i + NUM_CES;
        NIOVA_ASSERT(CIRCLEQ_ENTRY_DETACHED(&ce2[i].ce_lentry));
        CIRCLEQ_INSERT_HEAD(&qx, &ce2[i], ce_lentry);

        NIOVA_ASSERT(!CIRCLEQ_EMPTY(&qx));
        NIOVA_ASSERT(!CIRCLEQ_ENTRY_DETACHED(&ce2[i].ce_lentry));
    }

    // Splice the original set to the tail of the new set.
    CIRCLEQ_SPLICE_TAIL(&qy, &qx, ce_lentry);
    NIOVA_ASSERT(CIRCLEQ_EMPTY(&qy));
    NIOVA_ASSERT(!CIRCLEQ_EMPTY(&qx));

    // Verify the ordering of all entries.
    j = NUM_CES * 2;
    CIRCLEQ_FOREACH(tmp, &qx, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == --j && tmp->ce_version == 0);

    CIRCLEQ_FOREACH_REVERSE(tmp, &qx, ce_lentry)
    NIOVA_ASSERT(tmp->ce_value == j++ && tmp->ce_version == 0);
}

int
main(void)
{
    circleq_splice_tail_test();
}

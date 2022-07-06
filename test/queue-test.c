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

static void
circle_queue_checks(void)
{
    struct cq_entry {
        int num;
        CIRCLEQ_ENTRY(cq_entry) lentry;
    };
#define NENTRIES 4

    CIRCLEQ_HEAD(cq_head, cq_entry);
    struct cq_head head = CIRCLEQ_HEAD_INITIALIZER(head);

    struct cq_entry unattached = {0};
    NIOVA_ASSERT(CIRCLEQ_ENTRY_DETACHED(&unattached.lentry));
    NIOVA_ASSERT(!CIRCLEQ_ENTRY_IS_MEMBER(&head, &unattached, lentry));

    struct cq_entry entries[NENTRIES];

    for (int i = 0; i < NENTRIES; i++)
    {
        entries[i].num = i;
        CIRCLEQ_INSERT_HEAD(&head, &entries[i], lentry);
        NIOVA_ASSERT(!CIRCLEQ_ENTRY_DETACHED(&entries[i].lentry));
    }

    for (int i = 0; i < NENTRIES; i++)
    {
        NIOVA_ASSERT(CIRCLEQ_ENTRY_IS_MEMBER(&head, &entries[i], lentry));
    }

    struct cq_entry *tmp;
    int i = 0;
    CIRCLEQ_FOREACH_REVERSE(tmp, &head, lentry)
    {
        NIOVA_ASSERT(tmp->num == i);
        i++;
    }

    CIRCLEQ_FOREACH(tmp, &head, lentry)
    {
        NIOVA_ASSERT(tmp->num == i - 1);
        i--;
    }

    CIRCLEQ_REMOVE(&head, &entries[1], lentry);
    NIOVA_ASSERT(!CIRCLEQ_ENTRY_IS_MEMBER(&head, &entries[1], lentry));

    i = NENTRIES - 1;
    CIRCLEQ_FOREACH(tmp, &head, lentry)
    {
        NIOVA_ASSERT(tmp->num == i);
        i == 2 ? i -= 2 : i--;
    }

    NIOVA_ASSERT(CIRCLEQ_FIRST(&head) == &entries[NENTRIES - 1]);
    NIOVA_ASSERT(CIRCLEQ_LAST(&head) == &entries[0]);
}

static void
list_queue_checks(void)
{
    struct lq_entry {
        int num;
        LIST_ENTRY(lq_entry) lentry;
    };
#define NENTRIES 4

    LIST_HEAD(lq_head, lq_entry);
    struct lq_head head = LIST_HEAD_INITIALIZER(head);
    struct lq_entry unattached = {0};

    NIOVA_ASSERT(LIST_ENTRY_DETACHED(&unattached.lentry));
    NIOVA_ASSERT(!LIST_ENTRY_IS_MEMBER(&head, &unattached, lentry));

    struct lq_entry entries[NENTRIES];

    for (int i = 0; i < NENTRIES; i++)
    {
        entries[i].num = i;
        LIST_INSERT_HEAD(&head, &entries[i], lentry);
        NIOVA_ASSERT(!LIST_ENTRY_DETACHED(&entries[i].lentry));
    }

    for (int i = 0; i < NENTRIES; i++)
    {
        NIOVA_ASSERT(LIST_ENTRY_IS_MEMBER(&head, &entries[i], lentry));
    }

    struct lq_entry *tmp;
    int i = NENTRIES - 1;
    LIST_FOREACH(tmp, &head, lentry)
    {
        NIOVA_ASSERT(tmp->num == i);
        i--;
    }

    LIST_REMOVE(&entries[1], lentry);
    NIOVA_ASSERT(!LIST_ENTRY_IS_MEMBER(&head, &entries[1], lentry));

    i = NENTRIES - 1;
    LIST_FOREACH(tmp, &head, lentry)
    {
        NIOVA_ASSERT(tmp->num == i);
        i == 2 ? i -= 2 : i--;
    }

    NIOVA_ASSERT(LIST_FIRST(&head) == &entries[NENTRIES - 1]);

    struct lq_entry *x;
    LIST_FOREACH_SAFE(x, &head, lentry, tmp)
    {
        LIST_REMOVE(x, lentry);
        LIST_ENTRY_INIT(&x->lentry);
    }

    NIOVA_ASSERT(LIST_EMPTY(&head));
}

int
main(void)
{
    circle_queue_checks();
    circleq_splice_tail_test();

    list_queue_checks();
}

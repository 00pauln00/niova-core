/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2021
 */

#include <stdio.h>

#include "random.h"
#include "log.h"
#include "bitmap.h"

static void
niova_bitmap_tests(size_t size)
{
    struct x
    {
        struct niova_bitmap nb;
    };

    bitmap_word_t bar[size];

    struct x x = {0};

    int rc = niova_bitmap_init(&x.nb); // must attach() prior to init()
    NIOVA_ASSERT(rc == -EINVAL);

    const size_t nbits = size * NB_WORD_TYPE_SZ_BITS;

    rc = niova_bitmap_attach(&x.nb, bar, size);
    FATAL_IF(rc, "niova_bitmap_attach(): %s", strerror(rc));

    rc = niova_bitmap_init(&x.nb);

    NIOVA_ASSERT(niova_bitmap_size_bits(&x.nb) == nbits);

    FATAL_IF(rc, "niova_bitmap_init(): %s", strerror(rc));

    NIOVA_ASSERT(!niova_bitmap_inuse(&x.nb));
    NIOVA_ASSERT(!niova_bitmap_full(&x.nb));

    rc = niova_bitmap_unset(&x.nb, nbits + 1);
    FATAL_IF(rc != -ERANGE,
             "niova_bitmap_unset() expects -ERANGE, got %d", rc);

    rc = niova_bitmap_unset(&x.nb, 1);
    FATAL_IF(rc != -EALREADY,
             "niova_bitmap_unset() expects -EALREADY, got %d", rc);

    rc = niova_bitmap_set(&x.nb, 1);
    FATAL_IF(rc,
             "niova_bitmap_set() expects 0, got %d", rc);

    rc = niova_bitmap_set(&x.nb, 1);
    FATAL_IF(rc != -EBUSY,
             "niova_bitmap_set() expects -EBUSY, got %d", rc);

    rc = niova_bitmap_unset(&x.nb, 1);
    FATAL_IF(rc,
             "niova_bitmap_unset() expects 0, got %d", rc);

    NIOVA_ASSERT(!niova_bitmap_inuse(&x.nb));

    for (size_t i = 0; i < nbits; i++)
    {
        unsigned int idx = 0;
        rc = niova_bitmap_lowest_free_bit_assign(&x.nb, &idx);
        NIOVA_ASSERT(!rc && idx == i);
    }
    NIOVA_ASSERT(niova_bitmap_full(&x.nb));

    unsigned int idx = 0;
    NIOVA_ASSERT(niova_bitmap_lowest_free_bit_assign(&x.nb, &idx) == -ENOSPC);

    if (size > 64)
    {
        for (size_t i = 2; i <= 17; i++)
        {
            NIOVA_ASSERT(!niova_bitmap_unset(&x.nb, (nbits/i)));
            NIOVA_ASSERT(!niova_bitmap_is_set(&x.nb, (nbits/i)));
            // check adjacent bits have not changed
            NIOVA_ASSERT(niova_bitmap_is_set(&x.nb, (nbits/i)-1));
            NIOVA_ASSERT(niova_bitmap_is_set(&x.nb, (nbits/i)+1));
        }

        for (size_t i = 17; i >= 2; i--)
        {
            unsigned int idx = 0;
            rc = niova_bitmap_lowest_free_bit_assign(&x.nb, &idx);
            NIOVA_ASSERT(!rc && idx == (nbits/i));
        }

        // reinit
        NIOVA_ASSERT(!niova_bitmap_init(&x.nb));

        for (size_t i = 2; i <= 17; i++)
        {
            NIOVA_ASSERT(!niova_bitmap_set(&x.nb, (nbits/i)));
            NIOVA_ASSERT(niova_bitmap_is_set(&x.nb, (nbits/i)));
            // check adjacent bits have not changed
            NIOVA_ASSERT(!niova_bitmap_is_set(&x.nb, (nbits/i)-1));
            NIOVA_ASSERT(!niova_bitmap_is_set(&x.nb, (nbits/i)+1));
        }

        for (size_t i = 17; i >= 2; i--)
        {
            unsigned int idx = 0;
            rc = niova_bitmap_lowest_free_bit_release(&x.nb, &idx);
            NIOVA_ASSERT(!rc && idx == (nbits/i));
        }
        NIOVA_ASSERT(!niova_bitmap_inuse(&x.nb));
    }

    NIOVA_ASSERT(!niova_bitmap_init(&x.nb));

#define ARRAY_SZ 1000000
    bool should_be_set[ARRAY_SZ] = {0};

    for (int i = 0; i < ARRAY_SZ; i++)
    {
        // try to set one idx
        unsigned int set_idx = random_get() % nbits;

        rc = niova_bitmap_set(&x.nb, set_idx);
        NIOVA_ASSERT(!rc || rc == -EBUSY);
        if (!rc)
        {
            NIOVA_ASSERT(!should_be_set[set_idx]);
            should_be_set[set_idx] = true;
        }
        else
        {
            NIOVA_ASSERT(should_be_set[set_idx]);
        }

        // try to unset another
        unsigned int unset_idx = random_get() % nbits;

        rc = niova_bitmap_unset(&x.nb, unset_idx);
        NIOVA_ASSERT(!rc || rc == -EALREADY);
        if (!rc)
        {
            NIOVA_ASSERT(should_be_set[unset_idx]);
            should_be_set[unset_idx] = false;
        }
        else
        {
            NIOVA_ASSERT(!should_be_set[unset_idx]);
        }

//        fprintf(stdout, "%x %x\n", set_idx, unset_idx);
    }

    for (size_t i = 0; i < nbits; i++)
    {
        NIOVA_ASSERT(niova_bitmap_is_set(&x.nb, i) == should_be_set[i]);
    }
}

static void
niova_bitmap_merge_test(void)
{
    size_t size = 64;

    struct niova_bitmap x;
    bitmap_word_t x_map[size];

    int rc = niova_bitmap_attach_and_init(&x, x_map, size);
    NIOVA_ASSERT(!rc);

    struct niova_bitmap y;
    bitmap_word_t y_map[size];

    rc = niova_bitmap_attach_and_init(&y, y_map, size);
    NIOVA_ASSERT(!rc);

    rc = niova_bitmap_exclusive(&x, &y);
    FATAL_IF(rc, "niova_bitmap_exclusive(): %s", strerror(-rc));

    NIOVA_ASSERT(!niova_bitmap_set(&x, 0));
    NIOVA_ASSERT(!niova_bitmap_set(&y, 0));

    rc = niova_bitmap_exclusive(&x, &y);
    FATAL_IF(rc != -EALREADY, "niova_bitmap_exclusive(): %s", strerror(-rc));

    NIOVA_ASSERT(!niova_bitmap_unset(&y, 0));
    NIOVA_ASSERT(!niova_bitmap_set(&y, 1));

    rc = niova_bitmap_exclusive(&x, &y);
    FATAL_IF(rc, "niova_bitmap_exclusive(): %s", strerror(-rc));

    for (int i = 0; i < size; i++)
    {
        x.nb_map[i] = 0xa55555555555555aULL;
        y.nb_map[i] = 0x5aaaaaaaaaaaaaa5ULL;
    }

    rc = niova_bitmap_exclusive(&x, &y);
    FATAL_IF(rc, "niova_bitmap_exclusive(): %s", strerror(-rc));

    rc = niova_bitmap_merge(&x, &y);
    FATAL_IF(rc, "niova_bitmap_merge(): %s", strerror(-rc));

    FATAL_IF(!niova_bitmap_full(&x), "niova_bitmap_full() fails");

    rc = niova_bitmap_exclusive(&x, &y);
    FATAL_IF(rc != -EALREADY, "niova_bitmap_exclusive(): %s", strerror(-rc));
}

int
main(void)
{
    NIOVA_ASSERT(NB_NUM_WORDS(1) == 1);
    NIOVA_ASSERT(NB_NUM_WORDS(NB_WORD_TYPE_SZ_BITS) == 1);
    NIOVA_ASSERT(NB_NUM_WORDS(NB_WORD_TYPE_SZ_BITS - 1) == 1);
    NIOVA_ASSERT(NB_NUM_WORDS(NB_WORD_TYPE_SZ_BITS + 1) == 2);
    NIOVA_ASSERT(NB_NUM_WORDS(1023) == 16);
    NIOVA_ASSERT(NB_NUM_WORDS(1024) == 16);
    NIOVA_ASSERT(NB_NUM_WORDS(1025) == 17);

    niova_bitmap_tests(1UL);
    niova_bitmap_tests(7UL);
    niova_bitmap_tests(63UL);
    niova_bitmap_tests(64UL);
    niova_bitmap_tests(65UL);

    niova_bitmap_tests(1023UL);
    niova_bitmap_tests(1024UL);
    niova_bitmap_tests(1025UL);

    niova_bitmap_merge_test();

    return 0;
}

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
            rc = niova_bitmap_lowest_used_bit_release(&x.nb, &idx);
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

    struct niova_bitmap copy = {0};
    bitmap_word_t bf[size];
    rc = niova_bitmap_attach(&copy, bf, size);
    FATAL_IF(rc, "niova_bitmap_attach(): %s", strerror(-rc));

    rc = niova_bitmap_copy(&copy, &x.nb);
    FATAL_IF(rc, "niova_bitmap_copy(): %s", strerror(-rc));

    for (size_t i = 0; i < nbits; i++)
    {
        NIOVA_ASSERT(niova_bitmap_is_set(&x.nb, i) == should_be_set[i]);
        NIOVA_ASSERT(niova_bitmap_is_set(&copy, i) == should_be_set[i]);
    }

    // Deeper copy check
    for (size_t i = 0; i < size; i++)
        NIOVA_ASSERT(x.nb.nb_map[i] == bf[i]);
}

static void
niova_bitmap_merge_test(void)
{
    size_t size = 64;

    struct niova_bitmap x = {0};
    bitmap_word_t x_map[size];

    int rc = niova_bitmap_attach_and_init(&x, x_map, size);
    NIOVA_ASSERT(!rc);

    struct niova_bitmap y = {0};
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

    for (size_t i = 0; i < size; i++)
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

static void
niova_bitmap_bulk_unset_test(void)
{
    size_t size = 64;

    struct niova_bitmap x = {0};
    bitmap_word_t x_map[size];

    int rc = niova_bitmap_attach_and_init(&x, x_map, size);
    NIOVA_ASSERT(!rc);

    struct niova_bitmap y = {0};
    bitmap_word_t y_map[size];

    rc = niova_bitmap_attach_and_init(&y, y_map, size);
    NIOVA_ASSERT(!rc);

    rc = niova_bitmap_shared(&x, &y);
    FATAL_IF(rc, "niova_bitmap_shared(): %s", strerror(-rc));

    NIOVA_ASSERT(!niova_bitmap_set(&x, 0));
    NIOVA_ASSERT(!niova_bitmap_set(&y, 0));

    rc = niova_bitmap_shared(&x, &y);
    FATAL_IF(rc, "niova_bitmap_shared(): %s", strerror(-rc));

    NIOVA_ASSERT(!niova_bitmap_set(&x, 1));
    rc = niova_bitmap_shared(&x, &y);
    FATAL_IF(rc, "niova_bitmap_shared(): %s", strerror(-rc));

    // switch x and y
    rc = niova_bitmap_shared(&y, &x);
    FATAL_IF(rc != -ENOENT, "niova_bitmap_shared(): %s", strerror(-rc));

    for (size_t i = 0; i < size; i++)
    {
        x.nb_map[i] = -1ULL;
        y.nb_map[i] = 0x5555555555555555ULL;
    }

    rc = niova_bitmap_shared(&x, &y);
    FATAL_IF(rc, "niova_bitmap_shared(): %s", strerror(-rc));

    x.nb_map[size - 1] -= 1; // remove one bit and retest
    rc = niova_bitmap_shared(&x, &y);
    FATAL_IF(rc != -ENOENT, "niova_bitmap_shared(): %s", strerror(-rc));

    x.nb_map[size - 1] = -1ULL; // restore this item

    rc = niova_bitmap_bulk_unset(&x, &y);
    FATAL_IF(rc, "niova_bitmap_unset(): %s", strerror(-rc));

    for (size_t i = 0; i < size; i++)
    {
        niova_bitmap_init(&y);

        y.nb_map[i] = 0xaaaaaaaaaaaaaaaaULL;

        rc = niova_bitmap_bulk_unset(&x, &y);
        FATAL_IF(rc, "niova_bitmap_unset(): %s", strerror(-rc));
    }

    rc = niova_bitmap_inuse(&x);
    FATAL_IF(rc, "niova_bitmap_inuse() returns non-zero (rc=%d)", rc);
}

static void
niova_bitmap_size_test(void)
{
    bitmap_word_t x_map[1] = {0};
    struct niova_bitmap x = {.nb_nwords = NB_NUM_WORDS_MAX, .nb_map = x_map};

    NIOVA_ASSERT(niova_bitmap_init(&x) == -E2BIG);
}

static void
niova_bitmap_alloc_hint_test(void)
{
#define BHAT_NWORDS 64

    bitmap_word_t x_map[BHAT_NWORDS] = {0};

    struct niova_bitmap x =
        {.nb_nwords = BHAT_NWORDS, .nb_map = x_map,
         .nb_alloc_hint = BHAT_NWORDS - 1};

    NIOVA_ASSERT(!niova_bitmap_init(&x));

    unsigned int idx = 0;

    int rc = niova_bitmap_lowest_free_bit_assign(&x, &idx);
    FATAL_IF((rc || idx != ((BHAT_NWORDS - 1) * NB_WORD_TYPE_SZ_BITS)),
             "idx=%u rc=%d", idx, rc);

    /* This should be overridden by niova_bitmap_lowest_free_bit_assign()
     * since it overflows the bitmap.
     */
    x.nb_alloc_hint = BHAT_NWORDS;
    rc = niova_bitmap_lowest_free_bit_assign(&x, &idx);
    FATAL_IF((rc || idx != 0), "idx=%u rc=%d", idx, rc);
}

static void
niova_bitmap_init_max_idx(void)
{
    bitmap_word_t x_map[32] = {0};

    struct niova_bitmap x =
        {.nb_nwords = 1, .nb_map = x_map, .nb_max_idx = 65};

    NIOVA_ASSERT(niova_bitmap_init(&x) == -EOVERFLOW);

    x.nb_max_idx = 1;
    NIOVA_ASSERT(niova_bitmap_init(&x) == 0);

    size_t nfree = niova_bitmap_nfree(&x);

    NIOVA_ASSERT(nfree == x.nb_max_idx);


    unsigned int idx = NB_MAX_IDX_ANY;

    for (unsigned int i = 0; i < x.nb_max_idx; i++)
    {
        idx = NB_MAX_IDX_ANY;
        NIOVA_ASSERT(!niova_bitmap_lowest_free_bit_assign(&x,  &idx));
        NIOVA_ASSERT(idx == i);
    }

    idx = NB_MAX_IDX_ANY;
    NIOVA_ASSERT(niova_bitmap_lowest_free_bit_assign(&x,  &idx) == -ENOSPC);

    NIOVA_ASSERT(niova_bitmap_set(&x, x.nb_max_idx) == -ERANGE);
    NIOVA_ASSERT(niova_bitmap_unset(&x, x.nb_max_idx) == -ERANGE);

    x.nb_max_idx = 63;
    NIOVA_ASSERT(niova_bitmap_init(&x) == 0);
    NIOVA_ASSERT(niova_bitmap_nfree(&x) == x.nb_max_idx);

    for (unsigned int i = 0; i < x.nb_max_idx; i++)
    {
        idx = NB_MAX_IDX_ANY;
        NIOVA_ASSERT(!niova_bitmap_lowest_free_bit_assign(&x,  &idx));
        NIOVA_ASSERT(idx == i);
    }

    x.nb_max_idx = 1806;
    x.nb_nwords = 29;
    x.nb_alloc_hint = 28;
    NIOVA_ASSERT(niova_bitmap_init(&x) == 0);
    NIOVA_ASSERT(niova_bitmap_nfree(&x) == x.nb_max_idx);

    for (unsigned int i = 0; i < x.nb_max_idx + 2; i++)
    {
        idx = NB_MAX_IDX_ANY;

        int rc = niova_bitmap_lowest_free_bit_assign(&x, &idx);
        if (i < x.nb_max_idx)
        {
            NIOVA_ASSERT(rc == 0);
        }
        else
        {
            NIOVA_ASSERT(rc == -ENOSPC);
        }
    }
}

struct xarg
{
    struct niova_bitmap *x;
    size_t cnt;
    size_t total_bits;
};

static void
niova_bitmap_iterate_cb(size_t idx, size_t len, void *arg)
{
    NIOVA_ASSERT(len > 0 && arg != NULL);
    struct xarg *xarg = (struct xarg *)arg;

    NIOVA_ASSERT(idx < niova_bitmap_size_bits(xarg->x));
    NIOVA_ASSERT((idx + len) <= niova_bitmap_size_bits(xarg->x));

    xarg->cnt++;
    xarg->total_bits += len;
}

static void
niova_bitmap_iterate_test(void)
{
    size_t size = 64;

    struct niova_bitmap x = {0};
    bitmap_word_t x_map[size];

    struct xarg xarg = {.x = &x};

    int rc = niova_bitmap_attach_and_init(&x, x_map, size);

    rc = niova_bitmap_iterate(&x, niova_bitmap_iterate_cb, &xarg);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(xarg.cnt == 0 && xarg.total_bits == 0);

    for (size_t i = 0; i < size; i++)
        x_map[i] = -1ULL;

    rc = niova_bitmap_iterate(&x, niova_bitmap_iterate_cb, &xarg);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(xarg.cnt == 1 &&
                 xarg.total_bits == niova_bitmap_size_bits(&x));

    xarg.cnt = 0;
    xarg.total_bits = 0;

    for (size_t i = 0; i < size; i++)
        x_map[i] = 0x1010101010101010;

    rc = niova_bitmap_iterate(&x, niova_bitmap_iterate_cb, &xarg);
    NIOVA_ASSERT(rc == 0);
    // '8' is the number of ones groups in 0x1010101010101010
    NIOVA_ASSERT(xarg.cnt == (8 * size) &&
                 xarg.total_bits ==
                 (number_of_ones_in_val(0x1010101010101010) * size));


    xarg.cnt = 0;
    xarg.total_bits = 0;

    for (size_t i = 0; i < size; i++)
        x_map[i] = 0x7070707070707070;

    rc = niova_bitmap_iterate(&x, niova_bitmap_iterate_cb, &xarg);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(xarg.cnt == (8 * size) &&
                 xarg.total_bits ==
                 (number_of_ones_in_val(0x7070707070707070) * size));
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

    niova_bitmap_size_test();
    niova_bitmap_alloc_hint_test();

    niova_bitmap_tests(1UL);
    niova_bitmap_tests(7UL);
    niova_bitmap_tests(63UL);
    niova_bitmap_tests(64UL);
    niova_bitmap_tests(65UL);

    niova_bitmap_tests(1023UL);
    niova_bitmap_tests(1024UL);
    niova_bitmap_tests(1025UL);

    niova_bitmap_merge_test();
    niova_bitmap_bulk_unset_test();

    niova_bitmap_init_max_idx();

    niova_bitmap_iterate_test();

    return 0;
}

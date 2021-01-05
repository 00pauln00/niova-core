/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */

#include "binary_hist.h"

static void
negative_tests(void)
{
    struct binary_hist bh;

    NIOVA_ASSERT(binary_hist_init(NULL, 0, 0) == -EINVAL);
    NIOVA_ASSERT(binary_hist_init(&bh,  0, 0) == -EINVAL);
    NIOVA_ASSERT(binary_hist_init(&bh,  1, 0) == -EINVAL);

    NIOVA_ASSERT(binary_hist_init(&bh, BIN_HIST_MAX_BIT + 1,
                                  BIN_HIST_MAX_BIT + 2) == -EINVAL);

    NIOVA_ASSERT(binary_hist_init(&bh, 0, BIN_HIST_MAX_BIT + 1) == -EINVAL);

    NIOVA_ASSERT(!binary_hist_init(&bh,  0, 1));
    NIOVA_ASSERT(binary_hist_size(&bh) == 1);

    NIOVA_ASSERT(!binary_hist_init(&bh,  0, 2));
    NIOVA_ASSERT(binary_hist_size(&bh) == 2);

    NIOVA_ASSERT(binary_hist_lower_bucket_range(&bh, 0) == 0);
    NIOVA_ASSERT(binary_hist_upper_bucket_range(&bh, 1) == -1);
}

static void
dump_hist(const struct binary_hist *bh)
{
    for (int i = 0; i < binary_hist_size(bh); i++)
        fprintf(stdout, "pos %02d: range=%lld,%lld val=%lld\n",
                i, binary_hist_lower_bucket_range(bh, i),
                binary_hist_upper_bucket_range(bh, i),
                binary_hist_get_cnt(bh, i));
}

static void
fill_bh(int start_bit, int num_buckets)
{
    struct binary_hist bh;

    NIOVA_ASSERT(!binary_hist_init(&bh, start_bit, num_buckets));
    NIOVA_ASSERT(bh.bh_start_bit == start_bit);
    NIOVA_ASSERT(bh.bh_num_buckets == num_buckets);
    NIOVA_ASSERT(binary_hist_size(&bh) == num_buckets);

    for (int i = 0; i < binary_hist_size(&bh); i++)
    {
        unsigned long long val = i ? (1ULL << (start_bit + i - 1)) : 0;

        for (int i = 0; i < 10; i++)
            binary_hist_incorporate_val(&bh, val);

        if (binary_hist_get_cnt(&bh, i) != 10)
            dump_hist(&bh);
    }
//    dump_hist(&bh);
}

static void
postive_tests(void)
{
    fill_bh(0, 1);
    fill_bh(0, 10);
    fill_bh(1, 1);
    fill_bh(1, 10);
    fill_bh(13, 19);
}

int
main(void)
{
    negative_tests();
    postive_tests();

    return 0;
}

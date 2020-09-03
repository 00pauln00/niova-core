/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */
#ifndef NIOVA_BINARY_HIST_H
#define NIOVA_BINARY_HIST_H 1

#include "common.h"
#include "log.h"

#define BIN_HIST_BUCKETS_MAX 20
#define BIN_HIST_MAX_BIT 64

struct binary_hist
{
    const int bh_start_bit;
    const int bh_num_buckets;
    size_t    bh_values[BIN_HIST_BUCKETS_MAX];
};

static inline int
binary_hist_init(struct binary_hist *bh, int start_bit, int num_buckets)
{
    if (!bh ||
        start_bit < 0 ||
        num_buckets < 1 ||
        start_bit > BIN_HIST_MAX_BIT ||
        num_buckets > BIN_HIST_BUCKETS_MAX)
        return -EINVAL;

    memset(bh, 0, sizeof(*bh));

    CONST_OVERRIDE(int, bh->bh_start_bit, start_bit);
    CONST_OVERRIDE(int, bh->bh_num_buckets, num_buckets);

    return 0;
}

static inline int
binary_hist_size(const struct binary_hist *bh)
{
    return bh->bh_num_buckets;
}

static inline long long
binary_hist_get_cnt(const struct binary_hist *bh, int pos)
{
    if (!bh || pos >= bh->bh_num_buckets)
        return -EINVAL;

    NIOVA_ASSERT(pos <= BIN_HIST_BUCKETS_MAX);

    return bh->bh_values[pos];
}

static inline void
binary_hist_incorporate_val(struct binary_hist *bh, unsigned long long val)
{
    if (bh)
    {
        NIOVA_ASSERT(bh->bh_num_buckets <= BIN_HIST_BUCKETS_MAX);

        int pos = 0;

        val >>= bh->bh_start_bit;

        if (val)
            pos = MIN((TYPE_SZ_BITS(unsigned long long) -
                       __builtin_clzll(val)),
                      (bh->bh_num_buckets - 1));

        bh->bh_values[pos]++;
    }
}

static inline long long
binary_hist_lower_bucket_range(const struct binary_hist *bh, int pos)
{
    if (!bh || pos >= bh->bh_num_buckets)
        return -EINVAL;

    return !pos ?
        0 : (unsigned long long)(1ULL << (pos - 1 + bh->bh_start_bit));
}


static inline long long
binary_hist_upper_bucket_range(const struct binary_hist *bh, int pos)
{
    if (!bh || pos >= bh->bh_num_buckets)
        return -EINVAL;

    return pos == bh->bh_num_buckets - 1 ?
        -1 : (unsigned long long)((1ULL << (pos + bh->bh_start_bit)) - 1);
}

#endif

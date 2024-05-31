/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2019
 */
#ifndef NIOVA_BINARY_HIST_H
#define NIOVA_BINARY_HIST_H 1

#include <stdio.h>

#include "common.h"
#include "log.h"

#define BIN_HIST_BUCKETS_MAX 20
#define BIN_HIST_MAX_BIT 64

struct binary_hist
{
    const unsigned int bh_start_bit;
    const unsigned int bh_num_buckets;
    size_t             bh_values[BIN_HIST_BUCKETS_MAX];
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
binary_hist_get_cnt(const struct binary_hist *bh, unsigned int pos)
{
    if (!bh || pos >= bh->bh_num_buckets || pos >= BIN_HIST_BUCKETS_MAX)
        return -EINVAL;

    return bh->bh_values[pos];
}

static inline bool
binary_hist_is_empty(const struct binary_hist *bh)
{
    for (int i = 0; i < binary_hist_size(bh); i++)
        if (binary_hist_get_cnt(bh, i))
            return false;

    return true;
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

static inline void
binary_hist_incorporate_val_multi(struct binary_hist *bh,
                                  unsigned long long val,
                                  unsigned long long cnt)
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

        bh->bh_values[pos] += cnt;
    }
}

static inline long long
binary_hist_lower_bucket_range(const struct binary_hist *bh, unsigned int pos)
{
    if (!bh || pos >= bh->bh_num_buckets)
        return -EINVAL;

    return !pos ?
        0 : (unsigned long long)(1ULL << (pos - 1 + bh->bh_start_bit));
}

static inline long long
binary_hist_upper_bucket_range(const struct binary_hist *bh, unsigned int pos)
{
    if (!bh || pos >= bh->bh_num_buckets)
        return -EINVAL;

    return pos == bh->bh_num_buckets - 1 ?
        -1LL : (long long)((1ULL << (pos + bh->bh_start_bit)) - 1);
}

static inline void
binary_hist_print(const struct binary_hist *bh, size_t num_hist,
                  const char * (*name_cb)(int))
{
    if (!bh || !num_hist)
        return;

    for (size_t i = 0; i < num_hist; i++)
    {
        int values_printed = 0;

        name_cb ?
            fprintf(stdout, "\t%s = {", name_cb(i)) :
            fprintf(stdout, "\thist-%zu = {", i);

        const struct binary_hist *b = &bh[i];
        for (unsigned int j = 0; j < b->bh_num_buckets; j++)
        {
            if (binary_hist_get_cnt(b, j))
            {
                fprintf(stdout, "%s\n\t\t%7lld: %lld",
                        values_printed ? "," : "",
                        binary_hist_lower_bucket_range(b, j),
                        binary_hist_get_cnt(b, j));

                values_printed++;
            }
        }
        fprintf(stdout, "%s}%s\n", values_printed ? "\n\t" : "",
                (i == (num_hist - 1)) ? "" : ",");
    }
}

#endif

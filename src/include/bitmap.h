/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */
#ifndef __NIOVA_BITMAP_H
#define __NIOVA_BITMAP_H 1

#include "common.h"
#include "log.h"

typedef uint64_t bitmap_word_t;
#define NB_WORD_TYPE_SZ      (sizeof(bitmap_word_t))
#define NB_WORD_ANY          -1ULL
#define NB_WORD_TYPE_SZ_BITS (NB_WORD_TYPE_SZ * NBBY)
#define NB_MAP_WORD_IDX(x)   ((x) / NB_WORD_TYPE_SZ_BITS)
#define NB_NUM_WORDS(nbits)  (NB_MAP_WORD_IDX(nbits) +                  \
                              ((nbits % NB_WORD_TYPE_SZ_BITS) ? 1 : 0))

struct niova_bitmap
{
    size_t        nb_nwords;
    bitmap_word_t WORD_ALIGN_MEMBER(nb_map[]);
};

static inline int
niova_bitmap_init(struct niova_bitmap *nb, unsigned int nwords)
{
    if (!nb || !nwords)
        return -EINVAL;

    nb->nb_nwords = nwords;

    memset(nb->nb_map, 0, nwords * NB_WORD_TYPE_SZ);

    return 0;
}

static inline size_t
niova_bitmap_size_bits(const struct niova_bitmap *nb)
{
    return nb ? (nb->nb_nwords * NB_WORD_TYPE_SZ_BITS) : 0;
}

static inline size_t
niova_bitmap_inuse(const struct niova_bitmap *nb)
{
    size_t total = 0;

    if (nb)
    {
        unsigned int nw = nb->nb_nwords;

        for (unsigned int i = 0; i < nw; i++)
            total += number_of_ones_in_val(nb->nb_map[i]);
    }

    return total;
}

static inline bool
niova_bitmap_full(const struct niova_bitmap *nb)
{
    return (nb && (niova_bitmap_inuse(nb) ==
                   (nb->nb_nwords * NB_WORD_TYPE_SZ_BITS))) ? true : false;
}

static inline bool
niova_bitmap_is_set(const struct niova_bitmap *nb, unsigned int idx)
{
    if (!nb || idx >= (nb->nb_nwords * NB_WORD_TYPE_SZ_BITS))
        return false;

    unsigned int word_idx = NB_MAP_WORD_IDX(idx);
    bitmap_word_t mask = ((bitmap_word_t)1) << (idx % NB_WORD_TYPE_SZ_BITS);

    return (nb->nb_map[word_idx] & mask) ? true : false;
}

static inline int
niova_bitmap_set_unset(struct niova_bitmap *nb, unsigned int idx, bool set)
{
    if (!nb)
        return -EINVAL;
    if (idx >= (nb->nb_nwords * NB_WORD_TYPE_SZ_BITS))
        return -ERANGE;

    unsigned int word_idx = NB_MAP_WORD_IDX(idx);
    bitmap_word_t mask = ((bitmap_word_t)1) << (idx % NB_WORD_TYPE_SZ_BITS);

    if (set)
    {
        if (nb->nb_map[word_idx] & mask)
            return -EBUSY;

        nb->nb_map[word_idx] |= mask;
    }
    else
    {
        if (!(nb->nb_map[word_idx] & mask))
            return -EALREADY;

        nb->nb_map[word_idx] &= ~mask;
    }

    return 0;
}

static inline int
niova_bitmap_unset(struct niova_bitmap *nb, unsigned int idx)
{
    return niova_bitmap_set_unset(nb, idx, false);
}

static inline int
niova_bitmap_set(struct niova_bitmap *nb, unsigned int idx)
{
    return niova_bitmap_set_unset(nb, idx, true);
}

static inline int
niova_bitmap_lowest_free_bit_assign(struct niova_bitmap *nb, unsigned int *idx)
{
    if (!nb || !idx)
        return -EINVAL;

    unsigned int nw = nb->nb_nwords;

    for (unsigned int i = 0; i < nw; i++)
    {
        if (nb->nb_map[i] != NB_WORD_ANY)
        {
            uint64_t x = lowest_bit_set_and_return(&nb->nb_map[i]);

            *idx = ((i * NB_WORD_TYPE_SZ_BITS) +
                    (highest_set_bit_pos_from_val(x) - 1));

            return 0;
        }
    }
    return -ENOSPC;
}

static inline int
niova_bitmap_lowest_free_bit_release(struct niova_bitmap *nb,
                                     unsigned int *idx)
{
    if (!nb || !idx)
        return -EINVAL;

    unsigned int nw = nb->nb_nwords;

    for (unsigned int i = 0; i < nw; i++)
    {
        if (nb->nb_map[i] != 0)
        {
            uint64_t x = lowest_bit_unset_and_return(&nb->nb_map[i]);

            *idx = ((i * NB_WORD_TYPE_SZ_BITS) +
                    (highest_set_bit_pos_from_val(x) - 1));

            return 0;
        }
    }
    return -ENOENT;
}

#endif

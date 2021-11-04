/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */
#ifndef _NIOVA_BUFFER_H
#define _NIOVA_BUFFER_H 1

#include <sys/uio.h>
#include <stdint.h>

#include "common.h"
#include "queue.h"

struct buffer_set;
struct buffer_item
{
    struct buffer_set         *bi_bs;
    struct iovec               bi_iov; // may be modified by user
    const struct iovec         bi_iov_save;
    CIRCLEQ_ENTRY(buffer_item) bi_lentry;
    SLIST_ENTRY(buffer_item)   bi_user_slentry;
    const char                *bi_allocator_func;
    unsigned long int          bi_alloc_lineno:32;
    unsigned long int          bi_num:31;
    unsigned long int          bi_allocated:1;
};

CIRCLEQ_HEAD(buffer_list, buffer_item);
SLIST_HEAD(buffer_user_slist, buffer_item);

struct buffer_set
{
    ssize_t            bs_num_bufs;
    ssize_t            bs_num_allocated;
    ssize_t            bs_num_pndg_alloc;
    size_t             bs_item_size;
    bool               bs_init;
    struct buffer_list bs_free_list;
    struct buffer_list bs_inuse_list;
};

size_t
buffer_page_size(void);

static inline void
buffer_item_touch(struct buffer_item *bi)
{
    if (!bi)
        return;

    char *x = (char *)bi->bi_iov.iov_base;

    for (off_t off = 0; off < bi->bi_iov.iov_len; off += buffer_page_size())
            x[off] = 0xff;
}

size_t
buffer_set_size_to_nblks(const struct buffer_set *bs);

size_t
buffer_set_navail(const struct buffer_set *bs);

struct buffer_item *
buffer_set_allocate_item(struct buffer_set *bs);

void
buffer_set_release_item(struct buffer_item *bi);

struct buffer_item *
buffer_set_allocate_item_from_pending(struct buffer_set *bs);

int
buffer_set_release_pending_alloc(struct buffer_set *bc, const size_t nitems);

int
buffer_set_pending_alloc(struct buffer_set *bs, const size_t nitems);

int
buffer_set_destroy(struct buffer_set *bs);

int
buffer_set_init(struct buffer_set *bs, size_t nbufs, size_t buf_size,
                bool use_posix_memalign);

#endif

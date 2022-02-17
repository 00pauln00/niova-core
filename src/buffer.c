/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */

#include <sys/uio.h>
#include <unistd.h>

#include "common.h"

#include "alloc.h"
#include "buffer.h"
#include "log.h"

static size_t bufferSetPageSize;
static size_t bufferSetPageBits;

REGISTRY_ENTRY_FILE_GENERATE;

static void
buffer_page_size_set(void)
{
    if (bufferSetPageSize)
        return;

    bufferSetPageSize = sysconf(_SC_PAGESIZE);
    NIOVA_ASSERT(bufferSetPageSize == 4096);

    bufferSetPageBits = highest_set_bit_pos_from_val(bufferSetPageSize) - 1;
    NIOVA_ASSERT(bufferSetPageBits == 12);
}

size_t
buffer_page_size(void)
{
    return bufferSetPageSize;
}

size_t
buffer_set_size_to_nblks(const struct buffer_set *bs)
{
    return bs ? bs->bs_item_size >> bufferSetPageBits : 0;
}

size_t
buffer_set_navail(const struct buffer_set *bs)
{
    NIOVA_ASSERT(bs);
    NIOVA_ASSERT(bs->bs_num_allocated  >= 0);
    NIOVA_ASSERT(bs->bs_num_pndg_alloc >= 0);
    NIOVA_ASSERT(bs->bs_num_bufs >= (bs->bs_num_allocated +
                                     bs->bs_num_pndg_alloc));

    return bs->bs_num_bufs - (bs->bs_num_allocated + bs->bs_num_pndg_alloc);
}

struct buffer_item *
buffer_set_allocate_item(struct buffer_set *bs)
{
    if (!bs)
        return NULL;

    size_t navail = buffer_set_navail(bs);

    if (!navail)
    {
        NIOVA_ASSERT(CIRCLEQ_EMPTY(&bs->bs_free_list));
        return NULL;
    }

    NIOVA_ASSERT(!CIRCLEQ_EMPTY(&bs->bs_free_list));

    struct buffer_item *bi = CIRCLEQ_FIRST(&bs->bs_free_list);
    CIRCLEQ_REMOVE(&bs->bs_free_list, bi, bi_lentry);
    CIRCLEQ_INSERT_TAIL(&bs->bs_inuse_list, bi, bi_lentry);

    bs->bs_num_allocated++;
    bi->bi_allocated = true;

    return bi;
}

struct buffer_item *
buffer_set_allocate_item_from_pending(struct buffer_set *bs)
{
    NIOVA_ASSERT(bs);
    NIOVA_ASSERT(bs->bs_num_pndg_alloc > 0);

    bs->bs_num_pndg_alloc--;
    struct buffer_item *bi = buffer_set_allocate_item(bs);
    NIOVA_ASSERT(bi);

    return bi;
}

int
buffer_set_release_pending_alloc(struct buffer_set *bs, const size_t nitems)
{
    if (!bs || nitems > bs->bs_num_bufs)
        return -EINVAL;

    if (nitems > bs->bs_num_pndg_alloc)
        return -EOVERFLOW;

    bs->bs_num_pndg_alloc -= nitems;

    return 0;
}

int
buffer_set_pending_alloc(struct buffer_set *bs, const size_t nitems)
{
    if (!bs || !nitems)
        return -EINVAL;

    if (bs->bs_num_bufs < nitems)
        return -ENOMEM;

    else if (buffer_set_navail(bs) < nitems)
        return -ENOBUFS;

    bs->bs_num_pndg_alloc += nitems;

    return 0;
}

void
buffer_set_release_item(struct buffer_item *bi)
{
    if (!bi)
        return;

    struct buffer_set *bs = bi->bi_bs;

    NIOVA_ASSERT(bs);
    NIOVA_ASSERT(bi->bi_allocated);
    NIOVA_ASSERT(bs->bs_num_allocated > 0);
    bi->bi_iov = bi->bi_iov_save;
    bi->bi_allocated = false;

    bs->bs_num_allocated--;

    SLIST_ENTRY_INIT(&bi->bi_user_slentry);

    CIRCLEQ_REMOVE(&bs->bs_inuse_list, bi, bi_lentry);
    CIRCLEQ_INSERT_TAIL(&bs->bs_free_list, bi, bi_lentry);
}

int
buffer_set_destroy(struct buffer_set *bs)
{
    if (!bs)
        return -EINVAL;

    else if (!bs->bs_init)
        return -EALREADY;

    else if (bs->bs_num_allocated)
        return -EBUSY;

    NIOVA_ASSERT(!bs->bs_num_allocated);

    struct buffer_item *bi, *tmp;

    CIRCLEQ_FOREACH_SAFE(bi, &bs->bs_free_list, bi_lentry, tmp)
    {
        CIRCLEQ_REMOVE(&bs->bs_free_list, bi, bi_lentry);
        free(bi->bi_iov.iov_base);
        free(bi);

        bs->bs_num_bufs--;
    }
    NIOVA_ASSERT(!bs->bs_num_bufs);

    bs->bs_init = 0;

    return 0;
}

int
buffer_set_init(struct buffer_set *bs, size_t nbufs, size_t buf_size,
                bool use_posix_memalign)
{
    buffer_page_size_set();

    if (!bs || !buf_size || bs->bs_init)
        return -EINVAL;

    bs->bs_item_size = buf_size;
    bs->bs_num_bufs = 0;
    CIRCLEQ_INIT(&bs->bs_free_list);
    CIRCLEQ_INIT(&bs->bs_inuse_list);

    int error = 0;

    for (size_t i = 0; i < nbufs; i++)
    {
        struct buffer_item *bi = calloc(1, sizeof(struct buffer_item));
        if (!bi)
        {
            error = -ENOMEM;
            break;
        }

        bi->bi_bs = bs;
        bi->bi_iov.iov_len = buf_size;
        bi->bi_register_idx = -1;

        if (use_posix_memalign)
        {
            bi->bi_iov.iov_base =
                niova_posix_memalign(buf_size, buffer_page_size());

            FATAL_IF(bi->bi_iov.iov_base == NULL, "niova_posix_memalign()");
        }
        else
        {
            bi->bi_iov.iov_base = malloc(buf_size);
        }

        if (!bi->bi_iov.iov_base)
        {
            free(bi);
            error = -ENOMEM;
            break;
        }

        CONST_OVERRIDE(struct iovec, bi->bi_iov_save, bi->bi_iov);

        CIRCLEQ_INSERT_HEAD(&bs->bs_free_list, bi, bi_lentry);
        bs->bs_num_bufs++;

        buffer_item_touch(bi);
    }

    if (error)
        buffer_set_destroy(bs);

    bs->bs_init = true;

    return error;
}

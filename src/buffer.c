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
#include "registry.h"

static size_t bufferSetPageSize;
static size_t bufferSetPageBits;

REGISTRY_ENTRY_FILE_GENERATE;

LREG_ROOT_ENTRY_GENERATE(buffer_set_nodes, LREG_USER_TYPE_BUFFER_SET);

#define BUFFER_SECTOR_SIZE 512UL

enum buffer_set_lreg_stats
{
    BUFFER_SET_NAME,              // string
    BUFFER_SET_LREG_ITEM_SZ,      // unsigned int
    BUFFER_SET_LREG_NUM_BUFS,     // signed int
    BUFFER_SET_LREG_OUTSTANDING,  // signed int
    BUFFER_SET_LREG_TOTAL_ALLOCS, // unsigned int
    BUFFER_SET_LREG_MAX_USED,     // signed int
    BUFFER_SET_LREG___MAX,
};

static int
buffer_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
               struct lreg_value *lv)
{
    const struct buffer_set *bs = lrn->lrn_cb_arg;
    if (!bs)
        return -EINVAL;

    int rc = 0;

    switch (op)
    {
    case LREG_NODE_CB_OP_WRITE_VAL:           // fall through
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE: // fall through
    case LREG_NODE_CB_OP_INSTALL_NODE:        // fall through
    case LREG_NODE_CB_OP_DESTROY_NODE:
        break;

    case LREG_NODE_CB_OP_GET_NAME:
        if (!lv)
            return -EINVAL;
        lv->get.lrv_num_keys_out = BUFFER_SET_LREG___MAX;
        strncpy(lv->lrv_key_string, "buffer-sets", LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
        switch (lv->lrv_value_idx_in)
        {
        case BUFFER_SET_NAME:
            lreg_value_fill_string(lv, "name", bs->bs_name);
            break;
        case BUFFER_SET_LREG_ITEM_SZ:
            lreg_value_fill_unsigned(lv, "buf-size", bs->bs_item_size);
            break;
        case BUFFER_SET_LREG_NUM_BUFS:
            lreg_value_fill_signed(lv, "num-bufs", bs->bs_num_bufs);
            break;
        case BUFFER_SET_LREG_OUTSTANDING:
            lreg_value_fill_signed(lv, "in-use", bs->bs_num_allocated);
            break;
        case BUFFER_SET_LREG_TOTAL_ALLOCS:
            lreg_value_fill_unsigned(lv, "total-used", bs->bs_total_alloc);
            break;
        case BUFFER_SET_LREG_MAX_USED:
            lreg_value_fill_signed(lv, "max-in-use", bs->bs_max_allocated);
            break;
        };
        break;

    default:
        rc = -ENOENT;
        break;
    }

    return rc;
}

/* Use cast to override 'const *bs' which is needed for at least one public
 * routine.
 */
#define BS_LOCK(bs)                                                  \
    do {                                                             \
        if ((bs)->bs_serialize)                                      \
            niova_mutex_lock((pthread_mutex_t *)&(bs)->bs_mutex);    \
    } while (0)

#define BS_UNLOCK(bs)                                                   \
    do {                                                                \
        if ((bs)->bs_serialize)                                         \
            niova_mutex_unlock((pthread_mutex_t *)&(bs)->bs_mutex);     \
    } while (0)

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

static size_t
buffer_set_navail_locked(const struct buffer_set *bs)
{
    NIOVA_ASSERT(bs);

    NIOVA_ASSERT(bs->bs_num_allocated  >= 0);
    NIOVA_ASSERT(bs->bs_num_pndg_alloc >= 0);
    NIOVA_ASSERT(bs->bs_num_bufs >= (bs->bs_num_allocated +
                                     bs->bs_num_pndg_alloc));

    return bs->bs_num_bufs - (bs->bs_num_allocated + bs->bs_num_pndg_alloc);
}

size_t
buffer_set_navail(const struct buffer_set *bs)
{
    if (!bs)
        return 0;

    BS_LOCK(bs);
    size_t navail = buffer_set_navail_locked(bs);
    BS_UNLOCK(bs);

    return navail;
}

static struct buffer_item *
buffer_set_allocate_item_locked(struct buffer_set *bs)
{
    NIOVA_ASSERT(bs);

    size_t navail = buffer_set_navail_locked(bs);

    if (!navail)
    {
        NIOVA_ASSERT(CIRCLEQ_EMPTY(&bs->bs_free_list));

        return NULL;
    }

    NIOVA_ASSERT(!CIRCLEQ_EMPTY(&bs->bs_free_list));

    struct buffer_item *bi = CIRCLEQ_FIRST(&bs->bs_free_list);
    CIRCLEQ_REMOVE(&bs->bs_free_list, bi, bi_lentry);
    CIRCLEQ_INSERT_TAIL(&bs->bs_inuse_list, bi, bi_lentry);

    bs->bs_total_alloc++;

    bs->bs_num_allocated++;
    bi->bi_allocated = true;

    if (bs->bs_num_allocated > bs->bs_max_allocated)
        bs->bs_max_allocated = bs->bs_num_allocated;

    return bi;
}

struct buffer_item *
buffer_set_allocate_item(struct buffer_set *bs)
{
    if (!bs)
        return NULL;

    BS_LOCK(bs);

    struct buffer_item *bi = buffer_set_allocate_item_locked(bs);

    BS_UNLOCK(bs);

    return bi;
}

struct buffer_item *
buffer_set_allocate_item_from_pending(struct buffer_set *bs)
{
    NIOVA_ASSERT(bs);
    NIOVA_ASSERT(bs->bs_num_pndg_alloc > 0);

    BS_LOCK(bs);

    bs->bs_num_pndg_alloc--;
    struct buffer_item *bi = buffer_set_allocate_item_locked(bs);

    NIOVA_ASSERT(bi);

    BS_UNLOCK(bs);

    return bi;
}

int
buffer_set_release_pending_alloc(struct buffer_set *bs, const size_t nitems)
{
    if (!bs || nitems > bs->bs_num_bufs)
        return -EINVAL;

    BS_LOCK(bs);

    if (nitems > bs->bs_num_pndg_alloc)
    {
        BS_UNLOCK(bs);

        return -EOVERFLOW;
    }

    bs->bs_num_pndg_alloc -= nitems;

    BS_UNLOCK(bs);

    return 0;
}

int
buffer_set_pending_alloc(struct buffer_set *bs, const size_t nitems)
{
    if (!bs || !nitems)
        return -EINVAL;

    BS_LOCK(bs);

    if (bs->bs_num_bufs < nitems)
    {
        BS_UNLOCK(bs);
        return -ENOMEM;
    }
    else if (buffer_set_navail_locked(bs) < nitems)
    {
        BS_UNLOCK(bs);
        return -ENOBUFS;
    }

    bs->bs_num_pndg_alloc += nitems;

    BS_UNLOCK(bs);

    return 0;
}

void
buffer_set_release_item(struct buffer_item *bi)
{
    if (!bi)
        return;

    struct buffer_set *bs = bi->bi_bs;
    NIOVA_ASSERT(bs);

    BS_LOCK(bs);

    NIOVA_ASSERT(bi->bi_allocated);
    NIOVA_ASSERT(bs->bs_num_allocated > 0);
    bi->bi_iov = bi->bi_iov_save;
    bi->bi_allocated = false;

    bs->bs_num_allocated--;

    SLIST_ENTRY_INIT(&bi->bi_user_slentry);

    CIRCLEQ_REMOVE(&bs->bs_inuse_list, bi, bi_lentry);
    CIRCLEQ_INSERT_TAIL(&bs->bs_free_list, bi, bi_lentry);

    BS_UNLOCK(bs);
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

    if (bs->bs_serialize)
        pthread_mutex_destroy(&bs->bs_mutex);

    if (bs->bs_ctl_interface)
    {
        NIOVA_ASSERT(lreg_node_is_installed(&bs->bs_lrn));

        int rc = lreg_node_remove(&bs->bs_lrn,
                                  LREG_ROOT_ENTRY_PTR(buffer_set_nodes));
        NIOVA_ASSERT(rc == 0);

        // Ensure removal before returning
        rc = lreg_node_wait_for_completion(&bs->bs_lrn, false);
        NIOVA_ASSERT(rc == 0);

        bs->bs_ctl_interface = 0;
    }

    return 0;
}

int
buffer_set_init(struct buffer_set *bs, size_t nbufs, size_t buf_size,
                bool use_posix_memalign, bool serialize)
{
    buffer_page_size_set();

    if (!bs || !buf_size || bs->bs_init)
        return -EINVAL;

    if (serialize)
    {
        bs->bs_serialize = 1;
        pthread_mutex_init(&bs->bs_mutex, NULL);
    }

    memset(bs, 0, sizeof(struct buffer_set));

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
                niova_posix_memalign(buf_size, BUFFER_SECTOR_SIZE);

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

int
buffer_set_init_lreg(struct buffer_set *bs, size_t nbufs, size_t buf_size,
                     bool use_posix_memalign, bool serialize)
{
    int rc = buffer_set_init(bs, nbufs, buf_size, use_posix_memalign,
                             serialize);
    if (rc)
        return rc;

    lreg_node_init(&bs->bs_lrn, LREG_USER_TYPE_BUFFER_SET,
                   buffer_lreg_cb, bs, LREG_INIT_OPT_NONE);

    rc = lreg_node_install(&bs->bs_lrn,
                           LREG_ROOT_ENTRY_PTR(buffer_set_nodes));
    NIOVA_ASSERT(rc == 0);

    rc = lreg_node_wait_for_completion(&bs->bs_lrn, true);
    NIOVA_ASSERT(rc == 0);

    bs->bs_ctl_interface = 1;

    return 0;
}

int
buffer_set_apply_name(struct buffer_set *bs, const char *name)
{
    if (bs == NULL || name == NULL)
        return -EINVAL;

    strncpy(bs->bs_name, name, BUFFER_SET_NAME_MAX);

    return 0;
}

static init_ctx_t NIOVA_CONSTRUCTOR(BUFFER_SET_CTOR_PRIORITY)
buffer_set_ctor(void)
{
    LREG_ROOT_ENTRY_INSTALL(buffer_set_nodes);

    return;
}

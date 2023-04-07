/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */

#include "buffer.h"
#include "log.h"

static void
buffer_test(bool serialize, bool lreg)
{
    struct buffer_set bs = {0};

    enum buffer_set_opts opts =
        (serialize ? BUFSET_OPT_SERIALIZE : 0) |
        (lreg ? BUFSET_OPT_LREG : 0);

    int rc = buffer_set_init(NULL, 0, 0, opts);
    NIOVA_ASSERT(rc == -EINVAL);

    // page size is set on first call to set_init()
    NIOVA_ASSERT(buffer_page_size() == 4096);

    rc = buffer_set_init(&bs, 0, 0, opts);
    NIOVA_ASSERT(rc == -EINVAL);

    // init and destroy 'bs'
    rc = buffer_set_init(&bs, 1, 1, opts);

    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(buffer_set_navail(&bs) == 1);

    struct buffer_item *bi = buffer_set_allocate_item(&bs);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(buffer_set_navail(&bs) == 0);

    rc = buffer_set_destroy(NULL);
    NIOVA_ASSERT(rc == -EINVAL);
    rc = buffer_set_destroy(&bs);
    NIOVA_ASSERT(rc == -EBUSY);

    buffer_set_release_item(bi);
    NIOVA_ASSERT(buffer_set_navail(&bs) == 1);
    rc = buffer_set_destroy(&bs);
    NIOVA_ASSERT(rc == 0);

    // reserved ops
    const size_t n = 10;
    rc = buffer_set_init(&bs, n, 1, opts);

    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(buffer_set_navail(&bs) == n);

    rc = buffer_set_pending_alloc(&bs, n + 1);
    NIOVA_ASSERT(rc == -ENOMEM); // this will never succeed

    rc = buffer_set_pending_alloc(&bs, n - 1);
    NIOVA_ASSERT(rc == 0);

    rc = buffer_set_pending_alloc(&bs, 1 + (n - (n - 1)));
    NIOVA_ASSERT(rc == -ENOBUFS); // ok once 2 or buffers are available

    struct buffer_item *bi_array[n - 1];

    for (int i = 0; i < (n - 1); i++)
    {
        bi_array[i] = buffer_set_allocate_item_from_pending(&bs);
        NIOVA_ASSERT(bi_array[i]);
    }
    // should be one left..
    NIOVA_ASSERT(buffer_set_navail(&bs) == 1);

    rc = buffer_set_destroy(&bs);
    NIOVA_ASSERT(rc == -EBUSY);

    for (int i = 0; i < (n - 1); i++)
        buffer_set_release_item(bi_array[i]);

    rc = buffer_set_destroy(&bs);
    NIOVA_ASSERT(rc == 0);
}

static void
buffer_user_cache_test_cb(struct buffer_item *bi, void *arg)
{
    NIOVA_ASSERT(bi && bi->bi_user_cached && arg == bi->bi_cache_revoke_arg);

    struct buffer_item **item = (struct buffer_item **)arg;

    NIOVA_ASSERT(*item == bi);
    *item = NULL;
}

static void
buffer_user_cache_test(void)
{
    struct buffer_set bs;

#define BUCT_NBUFS 2

    size_t buf_sz = 64;
    struct buffer_item *items[BUCT_NBUFS];

    /* 1. Try the release call w/out setting BUFSET_OPT_USER_CACHE
     */
    int rc = buffer_set_init(&bs, BUCT_NBUFS, buf_sz, 0);
    NIOVA_ASSERT(rc == 0);

    struct buffer_item *bi = buffer_set_allocate_item(&bs);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(!bi->bi_user_cached);

    rc = buffer_set_user_cache_release_item(bi, NULL, NULL);
    NIOVA_ASSERT(rc == -EINVAL);

    rc = buffer_set_user_cache_release_item(bi, buffer_user_cache_test_cb,
                                            NULL);
    NIOVA_ASSERT(rc == -EPERM);

    buffer_set_release_item(bi);

    rc = buffer_set_destroy(&bs);
    NIOVA_ASSERT(rc == 0);

    /* 2. Setup buffer_set w/ BUFSET_OPT_USER_CACHE w/ a single item
     */
    rc = buffer_set_init(&bs, 1, buf_sz, BUFSET_OPT_USER_CACHE);
    NIOVA_ASSERT(rc == 0);

    // 2a. allocate item and perform a typical release
    bi = buffer_set_allocate_item(&bs);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(!bi->bi_user_cached);

    buffer_set_release_item(bi);

    NIOVA_ASSERT(bs.bs_num_user_cached == 0);

    //
    // 2b. allocate item, make it eligible for caching, then explicitly release
    bi = buffer_set_allocate_item(&bs);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(!bi->bi_user_cached);

    rc = buffer_set_user_cache_release_item(bi, buffer_user_cache_test_cb,
                                            &items[0]);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(bi->bi_user_cached);
    NIOVA_ASSERT(bs.bs_num_user_cached == 1);

    buffer_set_release_item(bi);
    items[0] = bi;
    NIOVA_ASSERT(bs.bs_num_user_cached == 0);
    NIOVA_ASSERT(items[0] == bi); // callback should *not* have run
    items[0] = NULL;

    //
    // 2c. Observe item being removed via callback
    bi = buffer_set_allocate_item(&bs);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(!bi->bi_user_cached);
    items[0] = bi;

    rc = buffer_set_user_cache_release_item(bi, buffer_user_cache_test_cb,
                                            &items[0]);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(bi->bi_user_cached);
    NIOVA_ASSERT(bs.bs_num_user_cached == 1);

    // 2c.1 Second allocation will cause the callback to be issued
    struct buffer_item *bi_new = buffer_set_allocate_item(&bs);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(!bi->bi_user_cached);
    NIOVA_ASSERT(items[0] == NULL); // ensure the first item was removed

    items[0] = bi_new;
    // 2c.2 Cache the new item and observe it being released via set_destroy()
    rc = buffer_set_user_cache_release_item(bi_new, buffer_user_cache_test_cb,
                                            &items[0]);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(bi->bi_user_cached);
    NIOVA_ASSERT(bs.bs_num_user_cached == 1);

    rc = buffer_set_destroy(&bs); // destroy w/ cached item
    NIOVA_ASSERT(rc == 0);

    NIOVA_ASSERT(items[0] == NULL); // callback must have run on destroy

    /* 3. Setup buffer_set w/ BUFSET_OPT_USER_CACHE w/ multiple items
     */
    rc = buffer_set_init(&bs, BUCT_NBUFS, buf_sz, BUFSET_OPT_USER_CACHE);
    NIOVA_ASSERT(rc == 0);

    bi = items[0] = buffer_set_allocate_item(&bs);
    // 3a Cache item
    rc = buffer_set_user_cache_release_item(bi, buffer_user_cache_test_cb,
                                            &items[0]);
    NIOVA_ASSERT(rc == 0);
    NIOVA_ASSERT(bi);
    NIOVA_ASSERT(bi->bi_user_cached);
    NIOVA_ASSERT(bs.bs_num_user_cached == 1);

    // 3b. Allocate a second item and ensure that the first was not release
    for (int i = 0; i < 10; i++)
    {
        bi = buffer_set_allocate_item(&bs);
        NIOVA_ASSERT(rc == 0);
        NIOVA_ASSERT(items[0] != NULL); // ensure cached item remains
        NIOVA_ASSERT(items[0]->bi_user_cached);
        buffer_set_release_item(bi);
    }

    bi = items[0];
    buffer_set_release_item(items[0]);
    NIOVA_ASSERT(items[0] == bi); // callback should not have been issued

    buffer_set_destroy(&bs);
    NIOVA_ASSERT(items[0] == bi); // callback should not have been issued
}

int
main(void)
{
    NIOVA_ASSERT(buffer_page_size() == 0);

    buffer_test(false, false);
    buffer_test(true, false);

    buffer_test(false, true);

    buffer_user_cache_test();

    return 0;
}

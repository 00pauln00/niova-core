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

int
main(void)
{
    NIOVA_ASSERT(buffer_page_size() == 0);

    buffer_test(false, false);
    buffer_test(true, false);

    buffer_test(false, true);

    return 0;
}

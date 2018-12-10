/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "metablock.h"
#include "metaroot.h"
#include "generic_metablock.h"
#include "log.h"
#include "lock.h"

static void
spin_lock_test(void)
{
    spinlock_t lock;

    spinlock_init(&lock);
    spinlock_lock(&lock);
    spinlock_unlock(&lock);

    int rc = spinlock_trylock(&lock);
    NIOVA_ASSERT(!rc);

    spinlock_unlock(&lock);
    spinlock_destroy(&lock);

    STDOUT_MSG("spin lock tests OK");
}

int
main(void)
{
    // Simple assert test
    NIOVA_ASSERT(1 == 1);

    // Test the spinlock api
    spin_lock_test();

    struct mb_header_persistent hp;

    STDOUT_MSG("sizeof(struct mb_vblk_entry) = %zd",
            sizeof(struct mb_vblk_entry));

    STDOUT_MSG("sizeof(struct mb_header_persistent) = %zd",
            sizeof(hp));

    struct vblkdev_metaroot_header mrh;

    STDOUT_MSG("sizeof(struct vblkdev_metaroot_header) = %zd",
            sizeof(mrh));

    struct generic_metablock_header_persistent gmh;

    STDOUT_MSG("sizeof(struct generic_metablock_header) = %zd",
               sizeof(gmh));

    return 0;
}

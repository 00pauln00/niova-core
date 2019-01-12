/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE 1 // for O_DIRECT
#include <libaio.h>

#include "common.h"
#include "log.h"
#include "lock.h"

#include "metablock.h"
#include "metaroot.h"
#include "generic_metablock.h"
#include "superblock.h"
#include "niosd_io.h"
#include "local_registry.h"

static void
spin_lock_test(void)
{
    spinlock_t lock;

    spinlock_init(&lock);

    STDOUT_MSG("lock init value=%d", lock);

    spinlock_lock(&lock);

    STDOUT_MSG("lock locked value=%d", lock);
    spinlock_unlock(&lock);

    STDOUT_MSG("lock unlocked value=%d", lock);

    int rc = spinlock_trylock(&lock);
    NIOVA_ASSERT(!rc);

    STDOUT_MSG("lock trylocked value=%d", lock);

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

    STDOUT_MSG("sizeof(struct mb_vblk_entry) = %zd",
               sizeof(struct mb_vblk_entry));

    struct mb_header_persistent hp;

    STDOUT_MSG("sizeof(struct mb_header_persistent) = %zd",
            sizeof(hp));

    struct sb_header_persistent sbp;

    STDOUT_MSG("sizeof(struct sb_header_persistent) = %zd",
               sizeof(sbp));

    struct vblkdev_metaroot_header mrh;

    STDOUT_MSG("sizeof(struct vblkdev_metaroot_header) = %zd",
            sizeof(mrh));

    struct generic_metablock_header_persistent gmh;

    STDOUT_MSG("sizeof(struct generic_metablock_header) = %zd",
               sizeof(gmh));

    struct niosd_io_request niorq;

    STDOUT_MSG("sizeof(struct niosd_io_request) = %zd", sizeof(niorq));

    lreg_subsystem_init();

    return 0;
}

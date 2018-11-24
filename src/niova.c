/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "log.h"
#include "common.h"
#include "vblkdev_handle.h"

static void
vblkdev_handle_test(void)
{
    vblkdev_id_t vb_id;
    vb_id.vdb_id[0] = 123;
    vb_id.vdb_id[1] = 456;

    struct vblkdev_handle *vbh = vbh_get(vb_id, true);
    vbh_put(vbh);
}

int
main(void)
{
    log_level_set(LL_DEBUG);

    STDOUT_MSG("niovad is here");
    vbh_subsystem_init();

    vblkdev_handle_test();

    vbh_subsystem_destroy();
    STDOUT_MSG("niovad exit");
}

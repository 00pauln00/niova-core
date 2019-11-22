/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "log.h"
#include "common.h"

#include "chunk_handle.h"
#include "vblkdev_handle.h"

static void
chunk_handle_test(struct vblkdev_handle *vbh)
{
    vblkdev_chunk_id_t chunk_id = 0x789;

    struct chunk_handle *ch = ch_get(vbh, chunk_id, true);
    NIOVA_ASSERT(ch);

    ch_put(ch);
}

static void
vblkdev_handle_test(void)
{
    vblkdev_id_t vb_id;
    vb_id.vdb_id[0] = 0x123;
    vb_id.vdb_id[1] = 0x456;

    struct vblkdev_handle *vbh = vbh_get(vb_id, true);
    NIOVA_ASSERT(vbh && !vbh_compare(vbh, (struct vblkdev_handle *)&vb_id));

    chunk_handle_test(vbh);

    vbh_put(vbh);

    vbh = vbh_get(vb_id, false);
    NIOVA_ASSERT(!vbh);
}

int
main(void)
{
    log_level_set(LL_DEBUG);

    STDOUT_MSG("niovad is here");

    vblkdev_handle_test();

    for (;;)
        sleep(1);

    STDOUT_MSG("niovad exit");
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "lock.h"
#include "log.h"
#include "ref_tree_proto.h"

#include "vblkdev_handle.h"
#include "chunk_handle.h"

static int
ch_cmp(const struct chunk_handle *a, const struct chunk_handle *b)
{
    if (a->ch_id < b->ch_id)
	return -1;
    else if (a->ch_id > b->ch_id)
        return 1;

    return 0;
}

REF_TREE_GENERATE(chunk_handle_tree, chunk_handle, ch_tentry, ch_cmp);

static void
ch_init(struct chunk_handle *ch, const struct chunk_handle *init_ch)
{
    ch->ch_vbh = init_ch->ch_vbh;
    ch->ch_id = init_ch->ch_id;
    ch->ch_ref = 0;

    DBG_CHUNK_HNDL(LL_DEBUG, ch, "");
}

struct chunk_handle *
ch_constructor(const struct chunk_handle *init_ch)
{
    struct chunk_handle *ch = niova_malloc(sizeof(struct chunk_handle));
    if (ch)
    {
        ch_init(ch, init_ch);
        // This call takes the vbhTree lock
        vbh_ref_cnt_inc(init_ch->ch_vbh);
    }

    return ch;
}

int
ch_destructor(struct chunk_handle *ch)
{
    struct vblkdev_handle *vbh = ch->ch_vbh;

    NIOVA_ASSERT(vbh);
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "ch=%p", ch);
    DBG_CHUNK_HNDL(LL_DEBUG, ch, "vbh=%p", vbh);

    NIOVA_ASSERT(!ch->ch_ref);

    vbh_put(vbh);

    niova_free(ch);

    return 0;
}

void
ch_put(struct chunk_handle *ch)
{
    struct vblkdev_handle *vbh = ch->ch_vbh;

    NIOVA_ASSERT(vbh);
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "ch=%p", ch);
    DBG_CHUNK_HNDL(LL_DEBUG, ch, "vbh=%p", vbh);

    RT_PUT(chunk_handle_tree, &vbh->vbh_chunk_handle_tree, ch);
}

struct chunk_handle *
ch_get(struct vblkdev_handle *vbh, const vblkdev_chunk_id_t ch_id,
       const bool add)
{
    struct chunk_handle ch_lookup = {.ch_vbh = vbh, .ch_id = ch_id};

    struct chunk_handle *ch =
        RT_GET(chunk_handle_tree, &vbh->vbh_chunk_handle_tree,
               (struct chunk_handle *)&ch_lookup, add);

    if (ch)
        DBG_CHUNK_HNDL(LL_DEBUG, ch, "");

    return ch;
}

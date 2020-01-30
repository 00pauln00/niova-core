/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "alloc.h"
#include "lock.h"
#include "log.h"
#include "ref_tree_proto.h"

#include "vblkdev_handle.h"
#include "chunk_handle.h"

REGISTRY_ENTRY_FILE_GENERATE;

/**
 * ch_cmp - private comparison function for ref tree.
 */
static int
ch_cmp(const struct chunk_handle *a, const struct chunk_handle *b)
{
    if (a->ch_id < b->ch_id)
	return -1;
    else if (a->ch_id > b->ch_id)
        return 1;

    return 0;
}

/* Generate ref tree functions from the template macro.
 */
REF_TREE_GENERATE(chunk_handle_tree, chunk_handle, ch_tentry, ch_cmp);

/**
 * ch_init - initialize a new chunk handle from a copy.
 * @ch:  the new chunk handle being initialized.
 * @init_ch:  initialization source handle.
 */
static void
ch_init(struct chunk_handle *ch, const struct chunk_handle *init_ch)
{
    ch->ch_vbh = init_ch->ch_vbh;
    ch->ch_id = init_ch->ch_id;
    ch->ch_ref = 0;

    DBG_CHUNK_HNDL(LL_DEBUG, ch, "");
}

/**
 * ch_constructor - creates a new chunk handle based on the ID inside the
 *    provided init_ch pointer.
 * @init_ch:  initialization object which contains the chunk ID and the pointer
 *    to the owning vblkdev handle.
 * NOTE: this function is public so that it may be used by vblkdev_handle.c
 */
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

/**
 * ch_destructor - destroys a chunk handle and releases its reference from the
 *    owning vblkdev handle.
 * @ch:  the chunk handle to be released.
 * NOTE: this function is public so that it may be used by vblkdev_handle.c
 */
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

/**
 * ch_put - return a referenced chunk handle.  This call may cause the chunk
 *    handle and its owning vblkdev handle to be released.
 * @ch:  the chunk handle being returned.
 */
void
ch_put(struct chunk_handle *ch)
{
    struct vblkdev_handle *vbh = ch->ch_vbh;

    NIOVA_ASSERT(vbh);
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "ch=%p", ch);
    DBG_CHUNK_HNDL(LL_DEBUG, ch, "vbh=%p", vbh);

    RT_PUT(chunk_handle_tree, &vbh->vbh_chunk_handle_tree, ch);
}

/**
 * ch_get - return a referenced chunk handle object for the given vbh and
 *    chunk id.
 * @vbh:  vblkdev handle to which this chunk belongs.
 * @ch_id:  the id of the chunk.
 * @add:  create the chunk entry if it does not yet exist.
 */
struct chunk_handle *
ch_get(struct vblkdev_handle *vbh, const vblkdev_chunk_id_t ch_id,
       const bool add)
{
    struct chunk_handle ch_lookup = {.ch_vbh = vbh, .ch_id = ch_id};

    struct chunk_handle *ch =
        RT_GET(chunk_handle_tree, &vbh->vbh_chunk_handle_tree,
               (struct chunk_handle *)&ch_lookup, add, NULL);

    if (ch)
        DBG_CHUNK_HNDL(LL_DEBUG, ch, "");

    return ch;
}

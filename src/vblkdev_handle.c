/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "lock.h"
#include "log.h"

#include "chunk_handle.h"
#include "ref_tree_proto.h"
#include "vblkdev_handle.h"

REF_TREE_HEAD(vblkdev_handle_tree, vblkdev_handle);

static int
vbh_cmp(const struct vblkdev_handle *a, const struct vblkdev_handle *b)
{
    int i;
    for (i = 0; i < VBLKDEV_ID_WORDS; i++)
    {
        if (a->vbh_id.vdb_id[i] < b->vbh_id.vdb_id[i])
            return -1;
        else if (a->vbh_id.vdb_id[i] > b->vbh_id.vdb_id[i])
            return 1;
    }
    return 0;
}

int vbh_compare(const struct vblkdev_handle *a, const struct vblkdev_handle *b)
{
    return vbh_cmp(a, b);
}

REF_TREE_GENERATE(vblkdev_handle_tree, vblkdev_handle, vbh_tentry, vbh_cmp);

struct vblkdev_handle_tree vbhTree;
ssize_t                    vbhNumHandles;
bool                       vbhInitialized = false;

#define VBH_LOCK   spinlock_lock(&vbhTree.lock)
#define VBH_UNLOCK spinlock_unlock(&vbhTree.lock)

/**
 * This function must be called when at least one ref is already held.
 */
void
vbh_ref_cnt_inc(struct vblkdev_handle *vbh)
{
    NIOVA_ASSERT(vbh->vbh_tentry.rbe_ref_cnt > 0);

    VBH_LOCK;
    vbh->vbh_tentry.rbe_ref_cnt++;
    VBH_UNLOCK;
}

static void
vbh_num_handles_inc_locked(void)
{
    NIOVA_ASSERT(vbhNumHandles >= 0)
    vbhNumHandles++;
}

static void
vbh_num_handles_dec_locked(void)
{
    NIOVA_ASSERT(vbhNumHandles > 0)
    vbhNumHandles--;
}

static void
vbh_init(struct vblkdev_handle *vbh, const vblkdev_id_t vbh_id)
{
    vbh->vbh_id = vbh_id;
    vbh->vbh_ref = 0;
    spinlock_init(&vbh->vbh_lock);
    REF_TREE_INIT(&vbh->vbh_chunk_handle_tree, ch_constructor, ch_destructor);

    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");
}

static struct vblkdev_handle *
vbh_constructor(const struct vblkdev_handle *init_vbh)
{
    struct vblkdev_handle *vbh = niova_malloc(sizeof(struct vblkdev_handle));
    if (vbh)
    {
        vbh_init(vbh, init_vbh->vbh_id);
        vbh_num_handles_inc_locked();
    }

    return vbh;
}

static int
vbh_destructor(struct vblkdev_handle *vbh)
{
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");

    vbh_num_handles_dec_locked();

    NIOVA_ASSERT(!vbh->vbh_ref);
    NIOVA_ASSERT(RB_EMPTY(&vbh->vbh_chunk_handle_tree.rt_head));
    spinlock_destroy(&vbh->vbh_lock);
    niova_free(vbh);

    return 0;
}

void
vbh_put(struct vblkdev_handle *vbh)
{
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");

    RT_PUT(vblkdev_handle_tree, &vbhTree, vbh);
}

struct vblkdev_handle *
vbh_get(const vblkdev_id_t vbh_id, const bool add)
{
    struct vblkdev_handle *vbh =
        RT_GET(vblkdev_handle_tree, &vbhTree,
               (struct vblkdev_handle *)&vbh_id, add);

    if (vbh)
        DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");

    return vbh;
}

void
vbh_subsystem_init(void)
{
    NIOVA_ASSERT(!vbhInitialized);
    REF_TREE_INIT(&vbhTree, vbh_constructor, vbh_destructor);

    vbhInitialized = true;

    LOG_MSG(LL_DEBUG, "done");
}

void
vbh_subsystem_destroy(void)
{
    NIOVA_ASSERT(vbhInitialized);
    NIOVA_ASSERT(!vbhNumHandles);

    vbhInitialized = false;

    LOG_MSG(LL_DEBUG, "done");
}

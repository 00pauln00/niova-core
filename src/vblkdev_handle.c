/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "lock.h"
#include "log.h"

#include "alloc.h"
#include "chunk_handle.h"
#include "ref_tree_proto.h"
#include "vblkdev_handle.h"

REGISTRY_ENTRY_FILE_GENERATE;

/**
 * vbh_cmp - private ref tree comparison function.
 */
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

/* Declare and generate the ref tree aspects.
 */
REF_TREE_HEAD(vblkdev_handle_tree, vblkdev_handle);
REF_TREE_GENERATE(vblkdev_handle_tree, vblkdev_handle, vbh_tentry, vbh_cmp);

/* vbhTree is the global ref tree head for all vblkdev handles.
 */
static struct vblkdev_handle_tree vbhTree;

/* vbhNumHandles tracks the number of allocated handles in the system.
 */
static ssize_t vbhNumHandles;
static bool vbhInitialized = false;

#define VBH_LOCK   pthread_mutex_lock(&vbhTree.mutex)
#define VBH_UNLOCK pthread_mutex_unlock(&vbhTree.mutex)

/**
 * vbh_compare - export the compare function for external users.
 */
int vbh_compare(const struct vblkdev_handle *a, const struct vblkdev_handle *b)
{
    return vbh_cmp(a, b);
}

/**
 * vbh_ref_cnt_inc - increment the reference count of an already referenced
 *    vbh.
 * @vbh:  The handle which is to be incremented.
 */
void
vbh_ref_cnt_inc(struct vblkdev_handle *vbh)
{
    NIOVA_ASSERT(VBH_TO_REF_CNT(vbh) > 0);

    VBH_LOCK;
    VBH_TO_REF_CNT(vbh)++;
    VBH_UNLOCK;
}

/**
 * vbh_num_handles_inc_locked - increment the number of active vblkdev_handles.
 */
static void
vbh_num_handles_inc_locked(void)
{
    NIOVA_ASSERT(vbhNumHandles >= 0)
    vbhNumHandles++;
}

/**
 * vbh_num_handles_dec_locked - decrement the number of active vblkdev_handles.
 */
static void
vbh_num_handles_dec_locked(void)
{
    NIOVA_ASSERT(vbhNumHandles > 0)
    vbhNumHandles--;
}

/**
 * vbh_init - called from vbh_constructor to initialize a vblkdev_handle.
 * @vbh_id:  The vblkdev_handle ID to be assigned to this handle.
 */
static void
vbh_init(struct vblkdev_handle *vbh, const vblkdev_id_t vbh_id)
{
    vbh->vbh_id = vbh_id;
    spinlock_init(&vbh->vbh_lock);
    REF_TREE_INIT(&vbh->vbh_chunk_handle_tree, ch_constructor, ch_destructor);

    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");
}

/**
 * vbh_constructor - private constructor which allocates and initializes
 *    memory for a vblkdev_handle.
 * @init_vbh:  initialiazation source data.
 */
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

/**
 * vbh_destructor - private destructor function called by the ref tree when the
 *    vblkdev_handle's ref count hits zero.
 * @vbh:  the handle to destroy.
 */
static int
vbh_destructor(struct vblkdev_handle *vbh)
{
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");

    vbh_num_handles_dec_locked();

    NIOVA_ASSERT(!VBH_TO_REF_CNT(vbh));
    NIOVA_ASSERT(RB_EMPTY(&vbh->vbh_chunk_handle_tree.rt_head));
    spinlock_destroy(&vbh->vbh_lock);
    niova_free(vbh);

    return 0;
}

/**
 * vbh_put - return a referenced vblkdev_handle.  This may cause the handle to
 *    be released.
 * @vbh:  the handle to return.
 */
void
vbh_put(struct vblkdev_handle *vbh)
{
    DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");

    RT_PUT(vblkdev_handle_tree, &vbhTree, vbh);
}

/**
 * vbh_get - return a referenced vblkdev_handle to the caller.
 * @vbh_id:  the vblkdev ID of this handle.
 * @add:  conditionally creates the entry if it does not exist.
 */
struct vblkdev_handle *
vbh_get(const vblkdev_id_t vbh_id, const bool add)
{
    struct vblkdev_handle *vbh =
        RT_GET(vblkdev_handle_tree, &vbhTree,
               (struct vblkdev_handle *)&vbh_id, add, NULL);

    if (vbh)
        DBG_VBLKDEV_HNDL(LL_DEBUG, vbh, "");

    return vbh;
}

/**
 * vbh_subsystem_init - called at startup time to initialize the global
 *    variables in this file.
 */
void
vbh_subsystem_init(void)
{
    NIOVA_ASSERT(!vbhInitialized);
    REF_TREE_INIT(&vbhTree, vbh_constructor, vbh_destructor);

    vbhInitialized = true;

    LOG_MSG(LL_DEBUG, "hello");
}

/**
 * vbh_subsystem_destroy - called at shutdown and runs some simple checks.
 */
void
vbh_subsystem_destroy(void)
{
    NIOVA_ASSERT(vbhInitialized);
    NIOVA_ASSERT(!vbhNumHandles);

    vbhInitialized = false;

    LOG_MSG(LL_DEBUG, "goodbye");
}

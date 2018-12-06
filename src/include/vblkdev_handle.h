/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef VBLKDEV_HANDLE_H
#define VBLKDEV_HANDLE_H 1

#include "common.h"
#include "log.h"
#include "lock.h"
#include "tree.h"

RB_HEAD(chunk_handle_tree, chunk_handle);
RB_PROTOTYPE(chunk_handle_tree, chunk_handle, ch_tenry, ch_tree_cmp);

RB_HEAD(vblkdev_handle_tree, vblkdev_handle);
RB_PROTOTYPE(vblkdev_handle_tree, vblkdev_handle, vbh_tenry, vbh_tree_cmp);

/**
 * -- struct chunk_handle --
 * Virtual block device chunk handle structure.
 * @ch_id:  the chunk ID.
 * @ch_tenry:  tree link.
 * @ch_ref:  reference count for this handle.  NOTE:  the reference count is
 *    managed by the owning vblkdev_handle's lock.
 */
struct chunk_handle
{
    vblkdev_chunk_id_t            ch_id;
    RB_ENTRY_PACKED(chunk_handle) ch_tenry;
    int                           ch_ref;
    bool                          ch_has_dirty_dpblks;
};

#define DBG_VBLKDEV_CHUNK_HNDL(log_level, vbch, fmt, ...)               \
    LOG_MSG(log_level, "vbch@%p %zx ref=%d "fmt, (vbch),                \
            (vbch)->ch_id, (vbh)->ch_ref,  ##__VA_ARGS__)

/**
 * -- struct vblkdev_handle --
 * Virtual block device handle is held within a global tree and maintains
 * handles for chunks which belong to the device.
 * @vbh_id: ID of this virtual block device.
 * @vbh_tenry:  tree link.
 * @vbh_chunk_handle_tree:  tree of chunks.
 * @vbh_lock:  lock which manages the tree.
 * @vbh_ref:  ref count which includes the number of chunks attached to the
 *    tree.  NOTE: this ref is managed by the lock in vblkdev_handle.c
 */
struct vblkdev_handle
{
    vblkdev_id_t                    vbh_id; // Must be first entry
    RB_ENTRY_PACKED(vblkdev_handle) vbh_tenry;
    struct chunk_handle_tree        vbh_chunk_handle_tree;
    spinlock_t                      vbh_lock;
    int                             vbh_ref;
};

#define DBG_VBLKDEV_HNDL(log_level, vbh, fmt, ...)                      \
    LOG_MSG(log_level, "vbh@%p %zx:%zx ref=%d "fmt, (vbh),              \
            (vbh)->vbh_id.vdb_id[0], (vbh)->vbh_id.vdb_id[1], (vbh)->vbh_ref, \
            ##__VA_ARGS__)

void
vbh_subsystem_init(void);

void
vbh_subsystem_destroy(void);

struct vblkdev_handle *
vbh_get(const vblkdev_id_t, const bool);

void
vbh_put(struct vblkdev_handle *);

static inline int
vbh_cmp(const struct vblkdev_handle *a,
        const struct vblkdev_handle *b)
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

static inline int
ch_cmp(const struct chunk_handle *a,
       const struct chunk_handle *b)
{
    if (a->ch_id < b->ch_id)
        return -1;
    else if (a->ch_id > b->ch_id)
        return 1;

    return 0;
}

#endif //VBLKDEV_HANDLE_H

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
#include "ctor.h"

#include "tree.h"
#include "ref_tree_proto.h"

/* Declare the chunk handle tree here for use by vblkdev_handle
 */
REF_TREE_HEAD(chunk_handle_tree, chunk_handle);

#define DBG_VBLKDEV_CHUNK_HNDL(log_level, vbch, fmt, ...) \
    LOG_MSG(log_level, "vbch@%p %zx ref=%d "fmt, (vbch),  \
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
    vblkdev_id_t             vbh_id;       // Must be first entry
    REF_TREE_ENTRY(vblkdev_handle) vbh_tentry;
    struct chunk_handle_tree vbh_chunk_handle_tree;
    spinlock_t               vbh_lock;
};

#define VBH_TO_REF_CNT(vbh) (vbh)->vbh_tentry.rte_ref_cnt

#define DBG_VBLKDEV_HNDL(log_level, vbh, fmt, ...)         \
    LOG_MSG(log_level, "vbh@%p %zx:%zx ref=%d "fmt, (vbh), \
        (vbh)->vbh_id.vdb_id[0], (vbh)->vbh_id.vdb_id[1],  \
            VBH_TO_REF_CNT((vbh)), ##__VA_ARGS__)

struct vblkdev_handle *
vbh_get(const vblkdev_id_t, const bool);

void
vbh_put(struct vblkdev_handle *);

int
vbh_compare(const struct vblkdev_handle *, const struct vblkdev_handle *);

void
vbh_ref_cnt_inc(struct vblkdev_handle *);

void
vbh_subsystem_init(void)
__attribute__ ((constructor (VBLKDEV_HANDLE_CTOR_PRIORITY)));

void
vbh_subsystem_destroy(void)
__attribute__ ((destructor (VBLKDEV_HANDLE_CTOR_PRIORITY)));

#endif //VBLKDEV_HANDLE_H

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef CHUNK_HANDLE_H
#define CHUNK_HANDLE_H 1

#include "common.h"
#include "log.h"

#include "ref_tree_proto.h"

struct vblkdev_handle;

/**
 * -- struct chunk_handle --
 * Virtual block device chunk handle structure.
 * @ch_vbh:  backpointer to owning vblkdev handle.
 * @ch_id:  the chunk ID.
 * @ch_tenry:  tree link.
 * @ch_ref:  reference count for this handle.  NOTE:  the reference count is
 *    managed by the owning vblkdev_handle's lock.
 */
struct chunk_handle
{
    struct vblkdev_handle       *ch_vbh;
    vblkdev_chunk_id_t           ch_id;
    REF_TREE_ENTRY(chunk_handle) ch_tentry;
    int                          ch_ref;
    bool                         ch_has_dirty_dpblks;
    //doubly linked list of metablock handles
    //pointers to metablk-content-index-blocks (2)
    //  - vblk caching
    //  - promoted tier physical data
};

#define DBG_CHUNK_HNDL(log_level, ch, fmt, ...)                        \
    log_msg(log_level, "ch@%p %zx ref:%d "fmt, (ch),                   \
            (ch)->ch_id, (ch)->ch_ref,  ##__VA_ARGS__)

struct chunk_handle *
ch_constructor(const struct chunk_handle *);

int
ch_destructor(struct chunk_handle *);

void
ch_put(struct chunk_handle *);

struct chunk_handle *
ch_get(struct vblkdev_handle *, const vblkdev_chunk_id_t,
       const bool);

#endif // CHUNK_HANDLE_H

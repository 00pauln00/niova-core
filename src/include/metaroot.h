/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef METAROOT_H
#define METAROOT_H 1

#define METAROOT_NUM_ROWS    128
#define METAROOT_NUM_COLUMNS 2

#include "common.h"
#include "metablock.h"

/**
 * -- struct vblkdev_metaroot_entry --
 * Metaroot entries are the starting points of metablock chains.
 * @vdme_chunk:  the chunk (and vblkdev id) of the chain.
 * @vdme_first_mb:  reference to the starting metablock.
 */
struct vblkdev_metaroot_entry
{
    struct vblkdev_chunk       vdme_chunk;
    struct mb_chain_link_entry vdme_first_mb;
} PACKED;

/**
 * -- struct vblkdev_metaroot_header --
 * Metaroot physical block header.
 * @vdmh_magic:  magic number for metaroot block headers.
 * @vdmh_hash:  hash of the entire physical block.
 * @vdmh_seqno:  the sequence number of the metaroot block.
 * @vdmh_row:  row in which this block resides.
 * @vdmh_column:  column in which this block resides.
 * @vdmh_num_entries:  number of metaroot entries contained in the block.
 * @vdmh_self_pblk:  the physical block id of this block.
 */
struct vblkdev_metaroot_header
{
    mb_magic_t     vdmh_magic;
    struct mb_hash vdmh_hash;
    // <-- START: vdmh_hash coverage -->
    uint64_t       vdmh_seqno;
    uint32_t       vdmh_row;
    uint32_t       vdmh_column;
    uint32_t       vdmh_num_entries;
    pblk_id_t      vdmh_self_pblk;
    // <-- vdmh_hash coverage extends to the entire pblk.. >
} PACKED;

#endif //METAROOT_H

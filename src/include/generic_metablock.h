/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef GENERIC_METABLOCK_H
#define GENERIC_METABLOCK_H 1

#include "common.h"
#include "metablock.h"

typedef uint32_t gme_magic_t;

/**
 * -- struct generic_metablock_entry --
 * Generic metablock structure is for logically combining vblkdev writes into
 * a single commit operation.
 * @gme_magic:  special value for sanity checking generic metablock contents.
 * @gme_num_vblk_entries:  the number of virtual block entries in the dpblk.
 * @gme_chunk:  vblkdev id and the chunk number of the write.
 * @gme_dpblk_entry:  data physical block entry (contains txn id as well).
 * @gme_vblk_entries:  array of virtual block entries.
 * // .. vblk checksums //
 */
struct generic_metablock_entry
{
    gme_magic_t           gme_magic;
    uint32_t              gme_num_vblk_entries;
    struct vblkdev_chunk  gme_chunk;
    struct mb_dpblk_entry gme_dpblk_entry;
    struct mb_vblk_entry  gme_vblk_entries[];
    // .. checksums follow:  SUM of gme_vblk_entries:mvbe_nblks
} PACKED;

/**
 * -- struct generic_metablock_header --
 * Header for the generic metablock.
 * @gmh_magic:  special number for pblk identification.
 * @gmh_hash:  hash value for the pblk's contents.
 * @gmh_num_dpblk_entries:  number of data pblks represented in this gpblk.
 * @gmh_type:  type of the gpblk.
 * @gmh_version:  version number of the gpblk.
 * @gmh_self_pblk_id:  physical block id.
 */
struct generic_metablock_header
{
    mb_magic_t     gmh_magic;
    struct mb_hash gmh_hash;
    // <-- START: gmh_hash coverage -->
    uint32_t       gmh_num_dpblk_entries;
    mb_type_t      gmh_type;
    mb_version_t   gmh_version;
    pblk_id_t      gmh_self_pblk_id;
    // <-- END: gmh_hash coverage continues to the rest of the pblk -->
} PACKED;

/**
 * -- struct generic_metablock_header_persistent --
 * Persistent representation of the generic metablock header.
 * @gmhp_chain_link:  chain link referring to the next generic metablock.
 * @gmhp_gmb:  generic metablock header contents.
 */
struct generic_metablock_header_persistent
{
    union
    {
        struct mb_header_chain_link gmhp_chain_link;
        unsigned char               gmhp_data_front[MB_HDR_CHAIN_LINK_IO_SIZE];
    };
    union
    {
        struct generic_metablock_header gmhp_gmb;
        unsigned char                   gmhp_data_back[MB_HDR_DATA_IO_SIZE];
    };
} PACKED;

static inline void
gmb_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct generic_metablock_header_persistent) ==
                        MB_HEADER_SIZE_BYTES);
    COMPILE_TIME_ASSERT(offsetof(struct generic_metablock_header_persistent,
                                 gmhp_gmb) == MB_HDR_CHAIN_LINK_IO_SIZE);
}

#endif // GENERIC_METABLOCK

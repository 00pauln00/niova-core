/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef NIOVA_METABLOCK_H
#define NIOVA_METABLOCK_H 1

#include <stdio.h>
#include "common.h"
#include "ec.h"

#define VBLK_ENTRY_TYPE_BITS 8

/**
 * -- struct mb_dpblk_entry --
 * Metablock data physical block entry.
 * @mdpb_txnid:  txn id in which this data block was written
 * @mdpbe_ec_ndata:  erasure coding:  number of data blocks
 * @mdpbe_ec_nparity:  erasure coding:  number of parity blocks
 * @mdpbe_ec_pos:  position of the this dpblk in the ec group
 * @mdpbe_pblk_id:  physical block identifier of the owning device
 */
struct mb_dpblk_entry
{
    uint64_t  mdpbe_txnid:NIOVA_TXN_BITS,
              mdpbe_ec_ndata:EC_DATA_BITS,
              mdpbe_ec_nparity:EC_PARITY_BITS,
              mdpbe_ec_pos:EC_POS_BITS;
    pblk_id_t mdpbe_pblk_id;
} PACKED;

#define VBLK_ENTRY_PAD_SIZE                                               \
    ((sizeof(uint64_t) * NBBY) -                                          \
     (VBLK_BITS + VBLK_RUN_LEN_BITS + VBLK_PBLK_IDX + MB_DPBLK_IDX_BITS + \
      VBLK_ENTRY_TYPE_BITS))

/**
 * -- struct mb_vblk_entry --
 * Metablock virtual block entry.
 * @mvbe_blk:  Virtual block number within the owning chunk.
 * @mvbe_nblks:  Number of consecutive blocks represented in the entry.
 * @mvbe_dpblk_idx:  Index into the physical block where the contents reside.
 * @mvbe_dpblk_info_idx:  Index into the metablock's dpblk info array.
 * @mvbe_type:  The virtual block type.
 */
struct mb_vblk_entry
{
    uint64_t mvbe_blk:VBLK_BITS,
             mvbe_nblks:VBLK_RUN_LEN_BITS,
             mvbe_dpblk_idx:VBLK_PBLK_IDX,
             mvbe_dpblk_info_idx:MB_DPBLK_IDX_BITS,
             mvbe_type:VBLK_ENTRY_TYPE_BITS,
             mvbe__pad:VBLK_ENTRY_PAD_SIZE;
} PACKED;

/**
 * -- struct mb_hash --
 * Metablock hash structure used prove content correctness.
 * @mh_type:  Type of the hash.  Hash size is also determined by the type.
 * @mh_bytes / @mh_uint64:  Buffer for the hash contents.
 */
struct mb_hash
{
    uint32_t          mh_type;
    uint32_t          mh__pad;
    union
    {
        unsigned char mh_bytes[MB_CHAIN_LINK_HASH_BYTES];
        uint64_t      mh_uint64[MB_CHAIN_LINK_HASH_UINT64_BYTES];
    };
} PACKED;

/**
 * -- struct mb_chain_link_entry --
 * Metablock chain link entry.
 * @mcle_pblk_id:  physical block id of the referred metablock.
 * @mcle_hash:  hash contents of the metablock.
 */
struct mb_chain_link_entry
{
    pblk_id_t      mcle_pblk_id;
    uint32_t       mcle__pad;
    struct mb_hash mcle_hash;
} PACKED;

/**
 * -- struct mb_cblk_entry --
 * Checksum block entry.  Checksum blocks entries reside in the metablock.
 */
#define mb_cblk_entry mb_chain_link_entry
//typedef struct mb_chain_link_entry struct mb_cblk_entry;

/**
 * -- struct mb_chain_link --
 * Metablock chain link is used to form a chain of metablocks.
 * @mhcl_magic:  magic number used to mark a chain link.
 * @mhcl_hash:  hash of the chain link header contents.
 * @mhcl_self_mb:  chain link entry of 'this' MB.
 * @mhcl_next_mb:  chain link entry of the 'next' MB in the chain.
 * @mhcl_type:  chain link type.
 */
struct mb_header_chain_link
{
    mb_magic_t                 mhcl_magic;
    struct mb_hash             mhcl_hash;
    // <-- START: mhcl_hash coverage -->
    struct mb_chain_link_entry mhcl_self_mb;
    struct mb_chain_link_entry mhcl_next_mb;
    mb_type_t                  mhcl_type; //end of the chain?
    uint32_t                   mhcl__pad0;
    // <-- END: mhcl_hash coverage -->
} PACKED;

/**
 * -- struct mb_header_data --
 * Metablock header data contains the 'meta information' for this MB.
 * @mbh_magic:  magic number for this structure type.
 * @mbh_hash:  hash of this structure's contents.
 * @mbh_version:  metablock version information.
 * @mbh_type:  metablock type.
 * @mbh_vba_crc:  checksum of the virtual blk entry array.
 * @mbh_self_pblk_id:  the pblk number of this MB.
 * @mbh_num_dpblks: number of data physical blocks.
 * @mbh_num_vblks:  number of virtual block entries.
 * @mbh_cblks:  checksum block entries.
 */
struct mb_header_data
{
    mb_magic_t           mbh_magic;
    struct mb_hash       mbh_hash; // must match mbcl_self_mb
    // <-- START: mbh_self_hash coverage -->
    mb_version_t         mbh_version;
    uint32_t             mbh__pad;
    mb_type_t            mbh_type; //general metablock?
    uint32_t             mbh__pad0;
    mb_crc32_t           mbh_vba_crc;
    pblk_id_t            mbh_self_pblk_id;
    uint32_t             mbh_num_dpblks;
    uint32_t             mbh_num_vblks;
    struct mb_cblk_entry mbh_cblks[MB_MAX_CPBLKS];
    // <-- mbh_hash coverage continues to the rest of the pblk ... >
} PACKED;

/**
 * -- struct mb_header_persistent --
 * Metablock persistent header structure.
 * @mbhp_chain_link:  chain link contents reside at the front of the header.
 * @mbhp_mb:  metablock header contents.
 *
 */
struct mb_header_persistent
{
    union
    {
        struct mb_header_chain_link mbhp_chain_link;
        unsigned char               mbhp_data_front[MB_HDR_CHAIN_LINK_IO_SIZE];
    };
    union
    {
        struct mb_header_data       mbhp_mb;
        unsigned char               mbhp_data_back[MB_HDR_DATA_IO_SIZE];
    };
} PACKED;


#if 1
static inline void
mb_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT((NIOVA_TXN_BITS + EC_DATA_BITS + EC_PARITY_BITS +
                         EC_POS_BITS) <= (sizeof(uint64_t) * NBBY));

    COMPILE_TIME_ASSERT(sizeof(struct mb_dpblk_entry) ==
                        MB_DPBLK_ENTRY_SIZE_BYTES);

    COMPILE_TIME_ASSERT(sizeof(struct mb_vblk_entry) ==
                        MB_VBLK_ENTRY_SIZE_BYTES);

    COMPILE_TIME_ASSERT(sizeof(struct mb_header_persistent) ==
                        MB_HEADER_SIZE_BYTES);
}
#endif

#endif

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef NIOVA_METABLOCK_H
#define NIOVA_METABLOCK_H 1

#include "common.h"

#define PBLK_INFO_IDX_BITS 13
#define VBLK_INFO_PAD_BITS 12
#define VBLK_ADDR_TYPE_BITS 8
#define CBLK_CRC_BITS 32

struct mb_vblk_addr
{
    uint64_t mvba_blk:VBLK_ADDR_BITS,
             mvba_nblks:VBLK_ADDR_RUN_LEN_BITS,
             mvba_pblk_idx:VBLK_PBLK_IDX,
             mvba_pblk_info_idx:PBLK_INFO_IDX_BITS,
             mvba_type:VBLK_ADDR_TYPE_BITS,
             mvba__pad:VBLK_INFO_PAD_BITS;
} PACKED;

struct mb_data_pblk
{
    uint64_t mdpb_txnid:NIOVA_TXN_BITS,
             mdpb_ec_type:NIOVA_EC_TYPE_BITS,
             mdpb_ec_pos:NIOVA_EC_POS_BITS,
             mdpb__pad;
    uint32_t mdpb_pblk_id:PBLK_ADDR_BITS,
             mdpb__pad1;
} PACKED;

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

struct mb_chksum_blk
{
    uint64_t       mcb_pblk_id:PBLK_ADDR_BITS,
                   mcb__pad;
    struct mb_hash mcb_hash;
} PACKED;

#define MB_CHKSUM_BLK_SIZE_BYTES (MB_CHAIN_LINK_HASH_BYTES + sizeof(uint64_t))

struct mb_chain_link_hash
{
    uint32_t       mclh_type;
    uint32_t       mclh__pad;
    struct mb_hash mclh_hash;
} PACKED;

struct mb_chain_link
{
    uint64_t                  mcl_pblk_id:PBLK_ADDR_BITS,
                              mcl__pad;
    struct mb_chain_link_hash mcl_link_hash;
} PACKED;

struct mb_header_chain_link
{
    uint64_t             mbcl_magic;
    struct mb_hash       mbcl_hash;
    // <-- START: mbcl_hash coverage -->
    struct mb_chain_link mbcl_self_mb;
    struct mb_chain_link mbcl_next_mb;
    uint32_t             mbcl_type; //end of the chain?
    uint32_t             mbcl__pad0;
    // <-- END: mbcl_hash coverage -->
} PACKED;

struct mb_header_data
{
    uint64_t             mbh_magic;
    struct mb_hash       mbh_self_hash; // must match mbcl_self_mb
    // <-- START: mbh_self_hash coverage -->
    uint32_t             mbh_version;
    uint32_t             mbh__pad;
    uint32_t             mbh_type; //general metablock?
    uint32_t             mbh__pad0;
    uint32_t             mbh_vba_crc;
    uint64_t             mbh_self_pblk_id:PBLK_ADDR_BITS; //needs to go into
                                                          //the hash
    uint32_t             mbh_num_data_pblks;
    uint32_t             mbh_num_vbas;
    struct mb_chksum_blk mbh_cblks[MB_MAX_NUM_CHKSUM_BLKS];
    // <-- mbh_self_hash coverage continues to the entire pblk ... >
} PACKED;

struct mb_header_persistent
{
    union
    {
        struct mb_header_chain_link mbhp_chain_link_header;
        unsigned char    mbhp_chain_link_header_data[MB_HEADER_IO_SIZE_BYTES];
    };
    union
    {
        struct mb_header_data mbhp_mb_header;
        unsigned char         mbhp_mb_header_data[MB_HEADER_IO_SIZE_BYTES];
    };
} PACKED;

static inline void
mb_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct mb_vblk_addr) ==
                        MB_VBLK_ADDR_SIZE_BYTES);

    COMPILE_TIME_ASSERT(sizeof(struct mb_data_pblk) ==
                        MB_DATA_PBLK_SIZE_BYTES);

    COMPILE_TIME_ASSERT(sizeof(struct mb_chksum_blk) ==
                        MB_CHKSUM_BLK_SIZE_BYTES);

    COMPILE_TIME_ASSERT(sizeof(struct mb_header_persistent) ==
                        MB_HEADER_IO_SIZE_BYTES * 2);
}

#endif

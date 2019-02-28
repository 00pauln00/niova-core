/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef SUPERBLOCK_H
#define SUPERBLOCK_H 1

#include "common.h"

#include "metablock.h"

/**
 * Superblock is a metablock type which is stored at the beginning, end, and
 * middle of the NIOVA-OSD device.  The superblock is responsible for basic
 * integrity checks on the device, such as whether the "device" has been
 * truncated or had its leading blocks overwritten.
 */

/**
 * -- struct sb_header_data --
 * Superblock contents.
 * @sbh_magic:  The magic number for a superblock.
 * @sbh_hash:  The hash of the structure's contents (excluding itself and
 *     the magic).
 * @sbh_version:  The SB version.
 * @sbh_niosd_id:  UUID of this "physical" device.
 * @sbh_pblk_id:  Physical block ID at which this SB resides.
 * @sbh_sb_replica_num:  The number of this SB replica.
 * @sbh_dev_phys_sz_in_bytes:  The size of this device - note - that "devices"
 *     should be capable of expanding.
 * @sbh_dev_prov_sz_in_bytes:  The provisioned size of this device which may
 *     be smaller than the physical size.
 * @sbh_num_pblks:  Number of usable physical blocks per the provisioned size.
 */
struct sb_header_data
{
    mb_magic_t       sbh_magic;
    struct mb_hash   sbh_hash;
    struct { // <-- START: sbh_hash coverage -->
        mb_version_t sbh_version;
        niosd_id_t   sbh_niosd_id;
        uint64_t     sbh_pblk_id;
        uint64_t     sbh_sb_replica_num;
        uint64_t     sbh_dev_phys_sz_in_bytes;
        uint64_t     sbh_dev_prov_sz_in_bytes;
        uint64_t     sbh_num_pblks;
    };      // <-- END: sbh_hash coverage -->
};

/**
 * -- struct sb_header_persistent --
 * Superblock persistent header structure.
 * @sbhp_chain_link:  chain link contents reside at the front of the header.
 *
 */
struct sb_header_persistent
{
    union
    {
        struct mb_header_chain_link sbhp_chain_link;
        unsigned char               sbhp_data_front[MB_HDR_CHAIN_LINK_IO_SIZE];
    };
    union
    {
        struct sb_header_data       sbhp_header;
        unsigned char               sbhp_data_back[MB_HDR_DATA_IO_SIZE];
    };
} PACKED;

static inline void //Xxx these need to be unified!
sb_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct sb_header_persistent) ==
                        MB_HEADER_SIZE_BYTES);
}


#endif //SUPERBLOCK_H

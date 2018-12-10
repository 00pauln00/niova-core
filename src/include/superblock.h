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

struct sb_header_data
{
    mb_magic_t     sbh_magic;
    struct mb_hash sbh_hash;
    mb_version_t   sbh_version;
    niosd_id_t     sbh_niosd_id;
    uint64_t       sbh_dev_sz_in_bytes;
    uint64_t       sbh_used_sz_in_bytes;
    uint64_t       sbh_num_pblks;
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

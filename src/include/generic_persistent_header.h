/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef GENERIC_PERSISTENT_HEADER_H
#define GENERIC_PERSISTENT_HEADER_H 1

#include "common.h"

#include "superblock.h"
#include "metablock.h"
#include "generic_metablock.h"

#define GEN_HEADER_SIZE_IN_BYTES 96

struct gen_header
{
    union
    {
        struct
        {
            struct mb_hash gh_hash;
            mb_magic_t     gh_magic;
            mb_version_t   gh_version;
            uint32_t       gh__pad;
            pblk_id_t      gh_self_pblk_id;
        };
        unsigned char gh_data[GEN_HEADER_SIZE_IN_BYTES];
    };
};

/**
 * -- struct mb_header_persistent --
 * Metablock persistent header structure.
 * @gph_[ms]b_chain_link:  chain link contents reside at the front of the
 *    header.
 * @gph_[ms]b:  metablock header contents.
 */
struct gen_persistent_header
{
    union
    {
        union
        {
            struct mb_header_chain_link gph_mb_chain_link;
            struct mb_header_chain_link gph_sb_chain_link;
            struct mb_header_chain_link gph_gmb_chain_link;
        };
        unsigned char gph_data_front[MB_HDR_CHAIN_LINK_IO_SIZE];
    };
    union
    {
        union
        {
            struct mb_header_data           gph_mb;
            struct sb_header_data           gph_sb;
            struct generic_metablock_header gph_gmb;
        };
        unsigned char gph_data_back[MB_HDR_DATA_IO_SIZE];
    };
} PACKED;

static inline void
gph_persistent_header_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct gen_persistent_header) ==
                        GEN_HEADER_SIZE_BYTES);
}

#endif //GENERIC_PERSISTENT_HEADER

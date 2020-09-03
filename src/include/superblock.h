/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef SUPERBLOCK_H
#define SUPERBLOCK_H 1

#include "common.h"

#include "metablock.h"

#define SB_PRIMARY_PBLK_ID 0 //Primary super block resides in the first block
#define SB_MAX_REPLICAS    8 //The number of possible SB replicas

#define SB_MAGIC 0x4106a261c162a601ULL

typedef uint8_t sb_replica_t;

#define SB_REPLICA_ANY ID_ANY_8bit
#define SB_REPLICA_MAX (SB_REPLICA_ANY - 1)

/**
 * Superblock is a metablock type which is stored at the beginning, end, and
 * middle of the NIOVA-OSD device.  The superblock is responsible for basic
 * integrity checks on the device, such as whether the "device" has been
 * truncated or had its leading blocks overwritten.
 */

/**
 * -- struct sb_contents_v0 --
 * Version '0' of the superblock contents.
 *
 */
struct sb_contents_v0
{
    pblk_id_t    sbc_pblk_id[SB_MAX_REPLICAS];
    sb_replica_t sbc_num_replicas;
    sb_replica_t sbc_sb_replica_num;
    sb_replica_t sbc_current_replica; //used only during start up
    uint8_t      sbc__pad[5];
    niosd_id_t   sbc_niosd_id;
    uint64_t     sbc_dev_phys_sz_in_bytes;
    uint64_t     sbc_dev_prov_sz_in_bytes;
    uint64_t     sbc_num_pblks; //including reserved and uber
    uint32_t     sbc_num_reserved_pblks;
    uint32_t     sbc_num_uber_pblks;
    pblk_id_t    sbc_starting_reserved_pblk; //this is typically '0'
    pblk_id_t    sbc_starting_uber_pblk;
};

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
    mb_magic_t     sbh_magic;
    struct mb_hash sbh_hash;
    struct { // <-- START: sbh_hash coverage -->
        mb_version_t sbh_version;
        union
        {
            struct sb_contents_v0 sbh_contents_v0;
        };
    };      // <-- END: sbh_hash coverage -->
};

static inline mb_version_t
sb_2_version(const struct sb_header_data *sb)
{
    return sb ? sb->sbh_version : MB_VERSION_ANY;
}

static inline pblk_id_t
sb_2_pblk_id(const struct sb_header_data *sb, int sb_replica)
{
    if (sb && sb_replica < SB_MAX_REPLICAS)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return sb->sbh_contents_v0.sbc_pblk_id[sb_replica];
        default:
            break;
        }
    }
    return PBLK_ID_ANY;
}

static inline uint64_t
sb_2_niosd_id(const struct sb_header_data *sb, bool upper)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return (upper ?
                    sb->sbh_contents_v0.sbc_niosd_id.nosd_id[0] :
                    sb->sbh_contents_v0.sbc_niosd_id.nosd_id[1]);
        default:
            break;
        }
    }
    return NIOSD_ID_ANY_64bit;
}

static inline sb_replica_t
sb_2_current_replica_num(const struct sb_header_data *sb)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return sb->sbh_contents_v0.sbc_current_replica;
        default:
            break;
        }
    }
    return SB_REPLICA_ANY;
}

static inline void
sb_set_current_replica_num(struct sb_header_data *sb, sb_replica_t val)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            sb->sbh_contents_v0.sbc_current_replica = val;
            break;
        default:
            break;
        }
    }
}

static inline sb_replica_t
sb_2_replica_num(const struct sb_header_data *sb)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return sb->sbh_contents_v0.sbc_sb_replica_num;
        default:
            break;
        }
    }
    return SB_REPLICA_ANY;
}

static inline ssize_t
sb_2_phys_size_bytes(const struct sb_header_data *sb)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return (ssize_t)sb->sbh_contents_v0.sbc_dev_phys_sz_in_bytes;
        default:
            break;
        }
    }
    return -1;
}

static inline ssize_t
sb_2_prov_size_bytes(const struct sb_header_data *sb)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return (ssize_t)sb->sbh_contents_v0.sbc_dev_prov_sz_in_bytes;
        default:
            break;
        }
    }
    return -1;
}

static inline ssize_t
sb_2_num_pblks(const struct sb_header_data *sb)
{
    if (sb)
    {
        switch (sb->sbh_version)
        {
        case 0:
            return (ssize_t)sb->sbh_contents_v0.sbc_num_pblks;
        default:
            break;
        }
    }
    return -1;
}

#define DBG_SB(log_level, sb, fmt, ...)                              \
    log_msg(log_level,                                               \
            "sb@%p vers=%x pblk=%x uuid=%lx:%lx n=%hhx phys-sz=%zd " \
            "prov-sz=%zd npblks=%ld "fmt,                            \
            (sb), sb_2_version((sb)), sb_2_pblk_id((sb), 0),         \
            sb_2_niosd_id((sb), true), sb_2_niosd_id((sb), false),   \
            sb_2_replica_num((sb)), sb_2_phys_size_bytes((sb)),      \
            sb_2_prov_size_bytes((sb)), sb_2_num_pblks((sb)),        \
            ##__VA_ARGS__)

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
        struct sb_header_data sbhp_header;
        unsigned char         sbhp_data_back[MB_HDR_DATA_IO_SIZE];
    };
} PACKED;

static inline void //Xxx these need to be unified!
sb_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct sb_header_persistent) ==
                        MB_HEADER_SIZE_BYTES);
}


#endif //SUPERBLOCK_H

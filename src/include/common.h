/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef NIOVA_COMMON_H
#define NIOVA_COMMON_H 1

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <uuid/uuid.h>

#ifndef  _STDIO_H
#define _STDIO_H 1
#include <stdio.h>
#undef _STDIO_H
#endif

/* No NIOVA includes may be added here!
 */

#ifndef NBBY
#define NBBY 8
#endif

#define IPV4_STRLEN 16 // char buffer size for ipv4 addresses

#define READ_PIPE_IDX  0
#define WRITE_PIPE_IDX 1
#define NUM_PIPE_FD    2

#define TYPE_SZ_BITS(type) (sizeof(type) * NBBY)

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define PACKED __attribute__((packed))

#define L2_CACHELINE_SIZE_BYTES 64
#define CACHE_ALIGN_MEMBER(memb)                                \
    __attribute__((aligned(L2_CACHELINE_SIZE_BYTES))) memb

#define COMPILE_TIME_ASSERT(cond)               \
    ((void)sizeof(char[1 - 2*!(cond)]))

#define CONST_OVERRIDE(type, var, value)         \
    *(type *)&(var) = value;

typedef uint32_t pblk_id_t;
typedef uint64_t mb_magic_t;
typedef uint32_t mb_type_t;
typedef uint32_t mb_version_t;
typedef uint32_t mb_crc32_t;
typedef uint64_t vblkdev_chunk_id_t;
typedef uint64_t txn_id_t;

/* Thread context defines
 */
typedef pthread_t thread_id_t;
typedef void      thread_exec_ctx_t;
typedef bool      thread_exec_ctx_bool_t;
typedef uint64_t  thread_exec_ctx_u64_t;

#define ID_ANY_8bit  255

#define ID_ANY_64bit -1ULL

#define NIOSD_ID_ANY_64bit ID_ANY_64bit

#define PBLK_ID_ANY -1U
#define PBLK_ID_MAX -2U
#define MB_VERSION_ANY -1U

#define NIOVA_OSD_ID_WORDS 2
#define VBLKDEV_ID_WORDS 2

#define NIOVA_SECTOR_SIZE 512
#define NIOVA_BLOCK_SIZE  4096
#define NIOVA_SECTORS_PER_BLOCK (NIOVA_BLOCK_SIZE / NIOVA_SECTOR_SIZE)

typedef struct niova_osd_id
{
    union
    {
        uint64_t nosd_id[NIOVA_OSD_ID_WORDS];
        uuid_t   nosd_uuid;
    };
} niosd_id_t;

typedef struct vblkdev_id
{
    uint64_t vdb_id[VBLKDEV_ID_WORDS];
} vblkdev_id_t;

struct vblkdev_chunk
{
    vblkdev_id_t       vbdc_dev;
    vblkdev_chunk_id_t vbdc_chunk;
};

#define NIOVA_MB_CHAIN_LINK_MAGIC  0xfefefefe0c0c0c0c
#define NIOVA_MB_HEADER_DATA_MAGIC 0xf0f0f0f0033d3d3f
/**
 * The maximum single device size that is currently supported.
 */
#define NIOVA_MAX_DEVICE_SIZE_BITS 47 //128 TiB

/**
 * Number of bits used to represent a transaction number.
 */
#define NIOVA_TXN_BITS 48

/**
 * Erasure coding type and position maximums.
 */
#define NIOVA_EC_TYPE_BITS 5
#define NIOVA_EC_POS_BITS  5

/**
 * Metablock chain link hash sizes.
 */
#define MB_CHAIN_LINK_HASH_BITS         256 // Support sha256
#define MB_CHAIN_LINK_HASH_BYTES        (MB_CHAIN_LINK_HASH_BITS / NBBY)
#define MB_CHAIN_LINK_HASH_UINT64_BYTES \
    (MB_CHAIN_LINK_HASH_BYTES / sizeof(uint64_t))

/**
 * Metablock header size.  This header includes two components:  chain link,
 * and header data.
 */
#define MB_HEADER_SIZE_BYTES 4096

/**
 * The Metablock header I/O size used both for the chain link and the
 * header contents.
 */
#define MB_HDR_CHAIN_LINK_IO_SIZE 1024
#define MB_HDR_DATA_IO_SIZE (MB_HEADER_SIZE_BYTES - MB_HDR_CHAIN_LINK_IO_SIZE)

/**
 * Physical block size is currently fixed at 128KiB.  Physical blocks (aka
 * 'pblks') are used for every persistent allocation type (data, metablocks,
 * checksum blocks, etc.).
 */
#define PBLK_SIZE_BITS  17 // 128KiB
#define PBLK_SIZE_BYTES (1ULL << PBLK_SIZE_BITS)

/**
 * Number of total physical blocks on a single device.
 */
#define PBLK_ADDR_BITS (sizeof(pblk_id_t) * NBBY)

/**
 * Virtual block device chunk size.  The 'chunk' is a logical division or
 * partitioning of the virtual block device and serves as a unit of
 * distribution, residency, and management.
 */
#define VBLKDEV_CHUNK_SIZE_BITS  33 // 8GiB
#define VBLKDEV_CHUNK_SIZE_BYTES (1ULL << VBLKDEV_CHUNK_SIZE_BITS)

/**
 * Virtual block address size is 4KiB.
 */
#define VBLK_SIZE_BITS  12 // 4KiB
#define VBLK_SIZE_BYTES (1ULL << VBLK_SIZE_BITS)

/**
 * Virtual block address identifier bits represents the address space size
 * of a chunk in vblk address size.  In other words, the number of virtual
 * blocks which can reside in a chunk.
 */
#define VBLK_BITS (VBLKDEV_CHUNK_SIZE_BITS - VBLK_SIZE_BITS)

/**
 * Virtual block address run length bits is used to represent coalesced virtual
 * blocks which are residing in the same pblk.
 */
#define VBLK_RUN_LEN_BITS (PBLK_SIZE_BITS - VBLK_SIZE_BITS)

/**
 * Virtual block index into a physical block.
 */
#define VBLK_PBLK_IDX VBLK_RUN_LEN_BITS

/**
 * Metablock virtual address entry size in bytes.
 */
#define MB_VBLK_ENTRY_SIZE_BYTES 8
/**
 * Metablock physical address entry size in bytes
 */
#define MB_DPBLK_ENTRY_SIZE_BYTES 12

/**
 * Metablock checksum bytes per physical data block.
 */
#define MB_CHKSUM_BYTES_PER_DPBLK 128

/**
 * Metablock:  data physical blocks per checksum physical block.
 */
#define MB_DPBLKS_PER_CPBLK (PBLK_SIZE_BYTES / MB_CHKSUM_BYTES_PER_DPBLK)

#define MB_DPBLKS_PER_CPBLK_HARDCODED 1024

/**
 * Max number of data pblks which can be housed in a metablock.
 */
#define MB_MAX_DPBLKS \
    ((PBLK_SIZE_BYTES - MB_HEADER_SIZE_BYTES) / \
     (MB_VBLK_ENTRY_SIZE_BYTES + MB_DPBLK_ENTRY_SIZE_BYTES))

#define MB_MAX_DPBLKS_HARDCODED 6348
/**
 * Metablock data physical block bits needed to index MB_MAX_DPBLKS
 */
#define MB_DPBLK_IDX_BITS 13
/**
 * Maximum number of checksum physical blocks which can be referenced by a
 * metablock.
 */
#define MB_MAX_CPBLKS                                   \
    ((MB_MAX_DPBLKS / MB_DPBLKS_PER_CPBLK) +            \
     (MB_MAX_DPBLKS % MB_DPBLKS_PER_CPBLK ? 1 : 0))

#define MB_MAX_CPBLKS_HARDCODED 7

static inline void
common_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(MB_MAX_DPBLKS == MB_MAX_DPBLKS_HARDCODED);
    COMPILE_TIME_ASSERT((1U << MB_DPBLK_IDX_BITS) >= MB_MAX_DPBLKS);
    COMPILE_TIME_ASSERT(MB_MAX_CPBLKS == MB_MAX_CPBLKS_HARDCODED);
    COMPILE_TIME_ASSERT(MB_DPBLKS_PER_CPBLK == MB_DPBLKS_PER_CPBLK_HARDCODED);

    COMPILE_TIME_ASSERT(sizeof(uuid_t) ==
                        (sizeof(uint64_t) * NIOVA_OSD_ID_WORDS));
}

static inline unsigned long long
highest_power_of_two_from_val(unsigned long long val)
{
    if (!val)
        return 0;

    int pos = TYPE_SZ_BITS(unsigned long long) - __builtin_clzll(val) - 1;

    return 1ULL << pos;
}

static inline int
number_of_ones_in_val(unsigned long long val)
{
    return __builtin_popcount(val);
}

#endif //NIOVA_COMMON_H

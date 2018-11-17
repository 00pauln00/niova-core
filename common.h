/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef NIOVA_COMMON_H
#define NIOVA_COMMON_H 1

#include <sys/types.h>

#ifndef NBBY
#define NBBY 8
#endif

#define PACKED __attribute__((packed))

#define COMPILE_TIME_ASSERT(cond) \
    ((void)sizeof(char[1 - 2*!(condition)]))

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
#define MB_CHAIN_LINK_HASH_BYTES        (MB_CHAIN_LINK_BITS / NBBY)
#define MB_CHAIN_LINK_HASH_UINT64_BYTES (MB_CHAIN_LINK_BITS / sizeof(uint64_t))

/**
 * The Metablock header I/O size used both for the chain link and the
 * header contents.
 */
#define MB_HEADER_IO_SIZE_BYTES 1024

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
#define PBLK_ADDR_BITS (NIOVA_MAX_DEVICE_SIZE_BITS - PBLK_SIZE_BITS)

/**
 * Virtual block device chunk size.  The 'chunk' is a logical division or
 * partitioning of the virtual block device and serves as a unit of
 * distribution, residency, and management.
 */
#define VBLKDEV_CHUNK_SIZE_BITS  33 // 8GiB
#define VBLKDEV_CHUNK_SIZE_BYTES (1ULL << CHUNK_SIZE_BITS)

/**
 * Virtual block address size is 4KiB.
 */
#define VBLK_ADDR_SIZE_BITS  12 // 4KiB
#define VBLK_ADDR_SIZE_BYTES (1ULL << VBLK_SIZE_BITS)

/**
 * Virtual block address identifier bits represents the address space size
 * of a chunk in vblk address size.  In other words, the number of virtual
 * blocks which can reside in a chunk.
 */
#define VBLK_ADDR_BITS (CHUNK_SIZE_BITS - VBLK_ADDR_SIZE_BITS)

/**
 * Virtual block address run length bits is used to represent coalesced virtual
 * blocks which are residing in the same pblk.
 */
#define VBLK_ADDR_RUN_LEN_BITS (PBLK_SIZE_BITS - VBLK_ADDR_SIZE_BITS)

/**
 * Virtual block index into a physical block.
 */
#define VBLK_PBLK_IDX VBLK_ADDR_RUN_LEN_BITS

/**
 * Metablock header size.  This header includes two components:  chain link,
 * and header data.
 */
#define MB_HEADER_SIZE_BYTES 4096

/**
 * Metablock virtual address entry size in bytes.
 */
#define MB_VBLK_ADDR_ENTRY_SIZE_BYTES 8
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
#define MB_DPBLKS_PER_CPBLK (PBLK_SIZE_BYTES / MB_CHKSUM_BYTES_PER_PBLK)

#define MB_DPBLKS_PER_CPBLK_HARDCODED 1024

/**
 * Max number of data pblks which can be housed in a metablock.
 */
#define MB_MAX_DPBLKS \
    ((PBLK_SIZE_BYTES - MB_HEADER_SIZE_BYTES) / \
     (MB_VBLK_ADDR_ENTRY_SIZE_BYTES + MB_DPBLK_ENTRY_SIZE_BYTES))

#define MB_MAX_DPBLKS_HARDCODED 6348

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
    COMPILE_TIME_ASSERT(MB_MAX_CPBLKS == MB_MAX_CPBLKS_HARDCODED);
    COMPILE_TIME_ASSERT(MB_DPBLKS_PER_CPBLK == MB_DPBLKS_PER_CPBLK_HARDCODED);
}

#endif //NIOVA_COMMON_H

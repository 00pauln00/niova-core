#ifndef NIOVA_COMMON_H
#define NIOVA_COMMON_H 1

#include <sys/types.h>

#ifndef NBBY
#define NBBY 8
#endif

#define MB_CHAIN_LINK_BITS         256 // Support sha256
#define MB_CHAIN_LINK_BYTES        (MB_CHAIN_LINK_BITS / NBBY)
#define MB_CHAIN_LINK_UINT64_BYTES (MB_CHAIN_LINK_BITS / sizeof(uint64_t))

#define MB_HEADER_IO_SIZE_BYTES 512

#define PBLK_BLK_SIZE_BITS  17
#define PBLK_BLK_SIZE_BYTES (1ULL << PBLK_BLK_SIZE_BITS)

#define CHUNK_SIZE_BITS  33
#define CHUNK_SIZE_BYTES (1ULL << CHUNK_SIZE_BITS)

#define VBLK_ADDR_SIZE_BITS  12
#define VBLK_ADDR_SIZE_BYTES (1ULL << VBLK_SIZE_BITS)

#define VBLK_ADDR_BITS         (CHUNK_SIZE_BITS - VBLK_ADDR_SIZE_BITS)
#define VBLK_ADDR_RUN_LEN_BITS (PBLK_BLK_SIZE_BITS - VBLK_ADDR_SIZE_BITS)
#define VBLK_PBLK_IDX     VBLK_RUN_LEN_BITS

#define NIOVA_MAX_DEVICE_SIZE_BITS 47 //128 TiB
#define PBLK_ADDR_BITS (NIOVA_MAX_DEVICE_SIZE_BITS - PBLK_BLK_SIZE_BITS)

#define NIOVA_TXN_BITS 48

#define NIOVA_EC_TYPE_BITS 5
#define NIOVA_EC_POS_BITS  5

#define MB_HEADER_SIZE_BYTES 4096

#define MB_VBLK_ADDR_SIZE_BYTES 8
#define MB_DATA_PBLK_SIZE_BYTES 12

#define MB_CHKSUM_BYTES_PER_PBLK 128
#define MB_DATA_PBLKS_PER_CHKSUM_PBLK \
    (PBLK_BLK_SIZE_BYTES / MB_CHKSUM_BYTES_PER_PBLK)

#define MB_MAX_NUM_CHKSUM_BLKS_BITS \
    (((1 << MB_MAX_DATA_PBLKS_BITS) / MB_DATA_PBLKS_PER_CHKSUM_PBLK) + \
     (1 << MB_MAX_DATA_PBLKS_BITS) % MB_DATA_PBLKS_PER_CHKSUM_PBLK ? 1 : 0)

#define MB_MAX_NUM_CHKSUM_BLKS (1U << MB_MAX_NUM_CHKSUM_BLKS_BITS)

#define PACKED __attribute__((packed))

#define COMPILE_TIME_ASSERT(cond) \
    ((void)sizeof(char[1 - 2*!(condition)]))

/* Max number of data pblks which can be housed in a metablock.  This comes
 * from the following calculation:
 * ((PBLK_BLK_SIZE_BYTES - MB_HEADER_SIZE_BYTES) /
 *  (MB_VBLK_ADDR_SIZE_BYTES + MB_DATA_PBLK_SIZE_BYTES))
 */
#define MB_MAX_DATA_PBLKS 6348

static inline void
common_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT(MB_MAX_DATA_PBLKS ==
                        ((PBLK_BLK_SIZE_BYTES - MB_HEADER_SIZE_BYTES) /
                         (MB_VBLK_ADDR_SIZE_BYTES + MB_DATA_PBLK_SIZE_BYTES)));
}

#endif //NIOVA_COMMON_H

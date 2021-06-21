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
#include <unistd.h>
#include <uuid/uuid.h>

#ifndef  _STDIO_H
#define _STDIO_H 1
#include <stdio.h>
#undef _STDIO_H
#endif

#ifndef UUID_STR_LEN
#define UUID_STR_LEN 37
#endif

/* No NIOVA includes may be added here!
 */

#ifndef NBBY
#define NBBY 8
#endif

#define IPV4_STRLEN 16 // char buffer size for ipv4 addresses and NULL term

#define READ_PIPE_IDX  0
#define WRITE_PIPE_IDX 1
#define NUM_PIPE_FD    2

#define TYPE_SZ_BITS(type) (sizeof(type) * NBBY)

#define ARRAY_SIZE(arr)                                              \
    (sizeof(arr) / sizeof((arr)[0])                                  \
     + sizeof(typeof(int[1 - 2 *                                     \
                         !!__builtin_types_compatible_p(typeof(arr), \
                                                        typeof(&arr[0]))])) * 0)

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define ABS(v) ((v < 0) ? (-v) : (v))

#define IS_EVEN(val) (val & 1) ? false : true

#define PACKED __attribute__((packed))

#define L2_CACHELINE_SIZE_BYTES 64
#define CACHE_ALIGN_MEMBER(memb) \
    __attribute__((aligned(L2_CACHELINE_SIZE_BYTES))) memb

#define SECTOR_ALIGN_MEMBER(memb) \
    __attribute__((aligned(NIOVA_SECTOR_SIZE))) memb

#define WORD_ALIGN_MEMBER(memb) \
    __attribute__((aligned(8))) memb

#define COMPILE_TIME_ASSERT(cond) \
    ((void)sizeof(char[1 - 2*!(cond)]))

#define CONST_OVERRIDE(type, var, value) \
    *(type *)&(var) = value;

#define NUM_HEX_CHARS(type) \
    sizeof(type) * 2

#define MEMBER_SIZE(type, member)               \
    sizeof(((type *)0)->member)

#define RAFT_PEER_ANY ID_ANY_8bit

typedef uint8_t  raft_peer_t;
typedef int64_t  raft_entry_idx_t;
typedef uint32_t version_t;

/* Thread context defines
 */
typedef pthread_t thread_id_t;
typedef void      thread_exec_ctx_t;
typedef bool      thread_exec_ctx_bool_t;
typedef uint64_t  thread_exec_ctx_u64_t;

#define ID_ANY_8bit  255
#define ID_ANY_16bit 65535
#define ID_ANY_32bit -1U
#define ID_ANY_64bit -1ULL

static inline void
common_compile_time_asserts(void)
{
    COMPILE_TIME_ASSERT((ssize_t)-1 == (int)-1);
    COMPILE_TIME_ASSERT((ssize_t)-ENOENT == (int)-ENOENT);
    COMPILE_TIME_ASSERT((ssize_t)-ENOLCK == (int)-ENOLCK);
}

static inline unsigned long long
highest_set_bit_pos_from_val(unsigned long long val)
{
    if (!val)
        return 0;

    int pos = TYPE_SZ_BITS(unsigned long long) - __builtin_clzll(val);

    return pos;
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
    return __builtin_popcountll(val);
}

static inline int
nconsective_bits_assign(uint64_t *field, unsigned int nbits)
{
    const unsigned int field_size = NBBY * sizeof(uint64_t);

    if (!field || nbits <= 0 || nbits > field_size)
        return -EINVAL;

    if (nbits == field_size)
    {
        int rc = 0;

        if (*field)
            rc = -ENOSPC;
        else
            *field = -1ULL;

        return rc;
    }

    const uint64_t ifield = ~(*field);
    uint64_t mask = (1ULL << nbits) - 1;

    for (unsigned int i = 0; i <= (field_size - nbits); i++)
    {
        uint64_t shifted_mask = mask << i;
        if ((ifield & shifted_mask) == shifted_mask)
        {
            *field |= shifted_mask;
            return i;
        }
    }

    return -ENOSPC;
}

static inline int
nconsective_bits_release(uint64_t *field, unsigned int offset,
                         unsigned int nbits)
{
    const unsigned int field_size = NBBY * sizeof(uint64_t);

    if (!field || nbits <= 0 || nbits > field_size || offset >= field_size ||
        ((nbits + offset) > field_size))
        return -EINVAL;

    if (nbits == field_size)
    {
        int rc = 0;

        if (*field == -1ULL)
            *field = 0;
        else
            rc = -EBADSLT;

        return rc;
    }

    uint64_t mask = ((1ULL << nbits) - 1) << offset;
    if ((*field & mask) == mask)
    {
        *field &= ~mask;
        return 0;
    }

    return -EBADSLT;
}

static inline uint64_t
lowest_bit_unset_and_return(uint64_t *field)
{
    const uint64_t x = *field & ~(*field - 1);
    *field &= (*field - 1);

    return x;
}

static inline uint64_t
lowest_bit_set_and_return(uint64_t *field)
{
    uint64_t inverse = ~(*field);
    uint64_t x = inverse & ~(inverse - 1);

    *field |= x;

    return x;
}

#endif //NIOVA_COMMON_H

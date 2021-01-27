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


#define PACKED __attribute__((packed))

#define L2_CACHELINE_SIZE_BYTES 64
#define CACHE_ALIGN_MEMBER(memb) \
    __attribute__((aligned(L2_CACHELINE_SIZE_BYTES))) memb

#define WORD_ALIGN_MEMBER(memb) \
    __attribute__((aligned(8))) memb

#define COMPILE_TIME_ASSERT(cond) \
    ((void)sizeof(char[1 - 2*!(cond)]))

#define CONST_OVERRIDE(type, var, value) \
    *(type *)&(var) = value;

#define NUM_HEX_CHARS(type) \
    sizeof(type) * 2

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
    return __builtin_popcount(val);
}

#endif //NIOVA_COMMON_H

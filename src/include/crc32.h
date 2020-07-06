/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef NIOVA_CRC32_H
#define NIOVA_CRC32_H 1

#include "common.h"

typedef uint32_t crc32_t;

#if defined(__x86_64__)
extern uint32_t
crc_pcl(const unsigned char *buffer, int len, unsigned int crc_init);
#endif

#define niova_crc crc_pcl

#define NIOVA_CRC_OBJ(obj, type, crc32_memb, extra_contents)            \
({                                                                      \
    const size_t _offset =                                              \
        (offsetof(struct type, crc32_memb) + sizeof(crc32_t));          \
    const unsigned char *_buf = (const unsigned char *)(obj) + _offset; \
    const int _crc_len = sizeof(struct type) - offset + extra_contents; \
                                                                        \
    (obj)->crc32_memb = niova_crc(_buf, _crc_len, 0);                   \
    (obj)->crc32_memb;                                                  \
})

#endif

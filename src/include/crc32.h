/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef NIOVA_CRC32_H
#define NIOVA_CRC32_H 1

#include "common.h"
#include "niova_core_config.h"

typedef uint32_t crc32_t;

#if defined(__x86_64__)
extern uint32_t
crc_pcl(const unsigned char *buffer, int len, unsigned int crc_init);

extern uint16_t
crc_t10dif_pcl(uint16_t crc_init, const unsigned char *buffer, size_t len);

#define niova_crc crc_pcl
#define niova_t10dif_crc crc_t10dif_pcl

#elif defined(__aarch64__)
extern uint32_t
crc32_arm(uint32_t crc, const uint8_t *data, size_t size);

#define niova_crc(buf, len, init) crc32_arm(init, buf, len)
#if HAVE_PMULL64
#define niova_t10dif_crc crc_t10dif_pmull_p64
#else
#define niova_t10dif_crc crc_t10dif_pmull_p8
#endif

#endif

#endif

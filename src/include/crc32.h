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

extern uint16_t
crc_t10dif_pcl(uint16_t crc_init, const unsigned char *buffer, size_t len);
#endif

#define niova_crc crc_pcl
#define niova_t10dif_crc crc_t10dif_pcl

#endif

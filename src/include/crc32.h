/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef NIOVA_CRC32_H
#define NIOVA_CRC32_H 1

#include "common.h"

#if defined(__x86_64__)
extern uint32_t
crc_pcl(const unsigned char *buffer, int len, unsigned int crc_init);
#endif

#endif

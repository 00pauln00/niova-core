/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef NIOVA_RANDOM_H
#define NIOVA_RANDOM_H 1

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <uuid/uuid.h>

#define RANDOM_STATE_BUF_LEN 256

unsigned int
get_random(void);

unsigned int
random_get(void);

int
random_init(unsigned int seed);

unsigned int
random_create_seed_from_uuid(const uuid_t uuid);

unsigned int
random_create_seed_from_uuid_and_tid(const uuid_t uuid);

unsigned char
random_get_u8(void);

#endif

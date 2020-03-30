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

#define RANDOM_STATE_BUF_LEN 256

unsigned int
get_random(void);

unsigned int
random_get(void);

int
random_init(unsigned int seed);


#endif

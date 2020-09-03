/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef _NIOVA_INIT_H_
#define _NIOVA_INIT_H_ 1

#include "common.h"
#include "ctor.h"

typedef void init_ctx_t;
typedef int  init_ctx_int_t;
typedef void destroy_ctx_t;

bool
init_ctx(void);

bool
destroy_ctx(void);

init_ctx_t
init_start(void)
__attribute__ ((constructor (INIT_START_CTOR_PRIORITY)));

init_ctx_t
init_complete(void)
__attribute__ ((constructor (INIT_COMPLETE_CTOR_PRIORITY)));

destroy_ctx_t
destroy_start(void)
__attribute__ ((destructor (INIT_COMPLETE_CTOR_PRIORITY)));

destroy_ctx_t
destroy_complete(void)
__attribute__ ((destructor (INIT_START_CTOR_PRIORITY)));

#endif

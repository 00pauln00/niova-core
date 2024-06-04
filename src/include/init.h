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

init_ctx_t NIOVA_CONSTRUCTOR(INIT_START_CTOR_PRIORITY)
init_start(void);

init_ctx_t NIOVA_CONSTRUCTOR(INIT_COMPLETE_CTOR_PRIORITY)
init_complete(void);

destroy_ctx_t NIOVA_DESTRUCTOR(INIT_COMPLETE_CTOR_PRIORITY)
destroy_start(void);

destroy_ctx_t NIOVA_DESTRUCTOR(INIT_START_CTOR_PRIORITY)
destroy_complete(void);

void
init_ctx_set(void);

void
init_ctx_unset(void);

void
destroy_ctx_set(void);

void
destroy_ctx_unset(void);

#endif

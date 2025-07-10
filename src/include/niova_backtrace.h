/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2021
 */

#ifndef _NIOVA_BACKTRACE_H_
#define _NIOVA_BACKTRACE_H_ 1

#include <stdint.h>

void
niova_backtrace_dump(void);

void
niova_backtrace_dump_pc(uintptr_t pc);

#endif

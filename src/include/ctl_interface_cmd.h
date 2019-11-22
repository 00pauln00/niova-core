/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _CTLI_CMD_H
#define _CTLI_CMD_H 1

#include "util_thread.h"

struct ctli_cmd_handle;

util_thread_ctx_ctli_t
ctlic_process_request(const struct ctli_cmd_handle *cch);

init_ctx_t
ctlic_init(void)
    __attribute__ ((constructor (LCTLI_CMD_SUBSYS_PRIORITY)));

#endif

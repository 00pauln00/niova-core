/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#ifndef _POPEN_CMD_H
#define _POPEN_CMD_H 1

typedef int popen_cmd_t;
typedef int popen_cmd_cb_ctx_t;

/**
 * popen_cmd_out - run the provided cmd and continually pass output
 */
int
popen_cmd_out(const char *cmd, int (*cb)(const char *, size_t, void *),
              void *arg);

#endif // _POPEN_CMD_H

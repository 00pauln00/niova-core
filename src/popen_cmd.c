/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>

#define DEF_BUF_SZ 129

/**
 * popen_cmd_out - run the provided cmd and continually pass output
 */
int
popen_cmd_out(const char *cmd, int (*cb)(const char *, size_t, void *),
              void *arg)
{
    FILE *child = popen(cmd, "r");
    if (!child)
        return -errno;

    char buf[DEF_BUF_SZ];

    while (fgets(buf, DEF_BUF_SZ, child))
    {
        if (cb)
        {
            int rc = cb(buf, strnlen(buf, DEF_BUF_SZ), arg);
            if (rc)
                break;
        }
    }

    int rc = pclose(child);

    return rc < 0 ? -errno : rc;
}

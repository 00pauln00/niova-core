/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <unistd.h>

#include "log.h"
#include "popen_cmd.h"

static int
popen_cb1(const char *inbuf, size_t len, void *arg)
{
    (void)arg;
    fprintf(stdout, "len=%zu >> %s", len, inbuf);

    return 0;
}

static int
popen_cb2(const char *inbuf, size_t len, void *arg)
{
    size_t *cnt = (size_t *)arg;

    *cnt += len;

    return 0;
}

int
main(void)
{
    int rc = popen_cmd_out("ls -al /tmp", NULL, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "popen_cmd_out(): %s", strerror(-rc));

    rc = popen_cmd_out("ls -al /tmp", popen_cb1, NULL);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "popen_cmd_out(): %s", strerror(-rc));

    size_t sz = 0;
    rc = popen_cmd_out("find . 2>&1", popen_cb2, (void *)&sz);
    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "popen_cmd_out(): %s", strerror(-rc));

    SIMPLE_LOG_MSG(LL_WARN, "sz=%zu", sz);

    // This should fail
    rc = popen_cmd_out("touch /proc/foo 2>&1", popen_cb2, (void *)&sz);
    if (rc == 256)
        rc = 0;
    else
        SIMPLE_LOG_MSG(LL_ERROR,
                       "popen_cmd_out(): expected error == 256 (rc=%d)", rc);

    exit(rc ? -1 : 0);
}

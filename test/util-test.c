/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2020
 */
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
//#include <uuid/uuid.h>

#include "util.h"

static void
mk_time_string_test(void)
{
    time_t now = niova_realtime_coarse_clock_get_sec();

    char time_string[MK_TIME_STR_LEN];

    int rc = niova_mk_time_string(now, time_string, MK_TIME_STR_LEN);
    if (rc)
        exit(rc);

    fprintf(stdout, "%s:  %s\n", __func__, time_string);

    rc = niova_mk_time_string(now, time_string, 1);
    if (rc != -ENOSPC)
        fprintf(stdout, "expected rc=-ENOSPC\n");
}

int
main(void)
{
    mk_time_string_test();
    return 0;
}

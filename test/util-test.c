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

static void
niova_string_to_unsigned_long_long_test(void)
{
    unsigned long long val = 0;

    int rc = niova_string_to_unsigned_long_long("", NULL);
    MY_FATAL_IF(rc != -EINVAL, "expected -EINVAL, got %d", rc);

    rc = niova_string_to_unsigned_long_long("foo", NULL);
    MY_FATAL_IF(rc != -EINVAL, "expected -EINVAL, got %d", rc);

    rc = niova_string_to_unsigned_long_long("foo", &val);
    MY_FATAL_IF(rc, "expected 0, got %d val=%llu", rc, val);

    rc = niova_string_to_unsigned_long_long("-1", &val);
    MY_FATAL_IF((rc || val != ULLONG_MAX),
                "expected 0, got %d OR expected val=%llu got %llu",
                rc, ULLONG_MAX, val);

    rc = niova_string_to_unsigned_long_long("-2", &val);
    MY_FATAL_IF((rc || val != (ULLONG_MAX - 1)),
                "expected 0, got %d OR expected val=%llu got %llu",
                rc, ULLONG_MAX - 1, val);

    rc = niova_string_to_unsigned_long_long("1", &val);
    MY_FATAL_IF((rc || val != 1),
                "expected 0, got %d (val expects '1' (%llu))", rc, val);

    // Currently there's no hex support
    rc = niova_string_to_unsigned_long_long("0xdeadbeef", &val);
    MY_FATAL_IF((rc || val != 0), "expected, got %d", rc);

    rc = niova_string_to_unsigned_long_long("18446744073709551615", &val);
    MY_FATAL_IF(rc,
                "expected 0, got %d (val expects 18446744073709551615 (%llu))",
                rc, val);
}

static void
niova_string_to_unsigned_int_test(void)
{
    unsigned int tmp = 0;

    int rc = niova_string_to_unsigned_int("", NULL);
    MY_FATAL_IF(rc != -EINVAL, "expected -EINVAL, got %d", rc);

    rc = niova_string_to_unsigned_int("1", NULL);
    MY_FATAL_IF(rc != -EINVAL, "expected -EINVAL, got %d", rc);

    rc = niova_string_to_unsigned_int(NULL, &tmp);
    MY_FATAL_IF(rc != -EINVAL, "expected -EINVAL, got %d (val=%u)", rc, tmp);

    tmp = 666;
    rc = niova_string_to_unsigned_int("", &tmp);
    MY_FATAL_IF((rc || tmp), "expected 0, got %d (val=%u)", rc, tmp);

    rc = niova_string_to_unsigned_int("18446744073709551615", &tmp);
    MY_FATAL_IF(rc != -EOVERFLOW, "expected -EOVERFLOW, got %d", rc);

    // UINT_MAX + 1
    rc = niova_string_to_unsigned_int("4294967296", &tmp);
    MY_FATAL_IF(rc != -EOVERFLOW, "expected -EOVERFLOW, got %d", rc);

    rc = niova_string_to_unsigned_int("4294967295", &tmp);
    MY_FATAL_IF((rc || tmp != UINT_MAX),
                "expected 0, got %d - val expects %u got val=%u",
                rc, UINT_MAX, tmp);

    rc = niova_string_to_unsigned_int("0", &tmp);
    MY_FATAL_IF((rc || tmp != 0), "expected 0, got %d val=%u", rc, tmp);
}

int
main(void)
{
    mk_time_string_test();
    niova_string_to_unsigned_long_long_test();
    niova_string_to_unsigned_int_test();
    return 0;
}

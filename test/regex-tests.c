/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include "log.h"

#include "regex_defines.h"
#include <regex.h>

struct regex_item
{
    const char *ri_item;
    bool        ri_valid;
};

struct regex_test
{
    const char              *rt_regex;
    const struct regex_item *rt_items;
    const size_t             rt_nitems;
};

static const struct regex_item commaIntegerTests[] = {
    {"0", true},
    {"1", true},
    {"9", true},
    {"10", true},
    {"111", true},
    {"999", true},
    {"00", false},
    {"01", false},
    {"9,", false},
    {"9,0", false},
    {"9,01", false},
    {"9,000", true},
    {"19,001", true},
    {"09,001", false},
    {"90,", false},
    {"900,", false},
    {"9001,", false},
    {"900,1", false},
    {"900,12", false},
    {"900,123", true},
    {"900,123,", false},
    {"000,123", false},
    {"123,456,789,000,123,234,000,000", true},
};

static const struct regex_item ipTests[] = {
    {"127.0.0.1", true},
    {"127.0.0.1000", false},
    {"0.0.0.0", true},
    {"256.1.1.1", false},
};

static const struct regex_item rncuiTests[] = {
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:0:0:0:0", true},
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:122:0:0:1", true},
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:90:0:0:1", true},
    {"1a636bd0-d27d-11ea-8cad-90324b2d1e89:2341523123:32452300123:1:0", true},
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:01:0:0:0", false},
    {"0d6ac28e-d278-11ea-9638-90324b2d1e8:122:0:0:1", false},
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:9b:0:0:1", true},
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:ffee:aadd:a341345e:1", true},
    {"1a636bd0-d27d-11ea-8cad-90324b2d1e89::2341523123:32452300123:1", false},
    {"1a636bd0-d27d-11ea-8cad-90324b2d1e89:2341523123:32452300123:0f:1e",
     false},
};

static const struct regex_item pmdbApplyCmdTests[] = {
    {"0d6ac28e-d278-11ea-9638-90324b2d1e89:0:0:0:0.read", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:0:31:3:0.read", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:0:31:3:0.read.8", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:0:31:3:0.read.08", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:0:31:3:0.read.a", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:ef:0.write:0.1", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:ac53.write:0.1", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:ac53.write:-1.1", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.write.01", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.write:432.123425", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.write.ef1232", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.rw.01", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.read.0fe", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.lookup.0fe", false},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.lookup", true},
    {"79551132-d289-11ea-b67c-90324b2d1e89:111:11:1:0.lookup.1000000001", true},
};

static const struct regex_test regexTests[] = {
    {RNCUI_V0_REGEX_BASE, rncuiTests, ARRAY_SIZE(rncuiTests)},
    {IPADDR_REGEX, ipTests, ARRAY_SIZE(ipTests)},
    {PMDB_TEST_CLIENT_APPLY_CMD_REGEX, pmdbApplyCmdTests,
     ARRAY_SIZE(pmdbApplyCmdTests)},
    {COMMA_DELIMITED_UNSIGNED_INTEGER, commaIntegerTests,
     ARRAY_SIZE(commaIntegerTests)},
};

static int
regex_tester(const struct regex_test *rt)
{
    if (!rt || !rt->rt_regex)
        return -EINVAL;

    regex_t regex;

    int rc = regcomp(&regex, rt->rt_regex, 0);
    if (rc)
    {
        char err_str[64] = {0};
        regerror(rc, &regex, err_str, 63);

        SIMPLE_LOG_MSG(LL_ERROR, "regcomp(): %s (regex=%s)",
                       err_str, rt->rt_regex);

        return rc;
    }

    for (size_t i = 0; i < rt->rt_nitems; i++)
    {
        const struct regex_item *ri = &rt->rt_items[i];

        SIMPLE_LOG_MSG(LL_DEBUG, "str=%s regex=%s (expect-valid=%s)",
                       ri->ri_item, rt->rt_regex,
                       ri->ri_valid ? "true" : "false");

        int local_rc = regexec(&regex, ri->ri_item, 0, NULL, 0);
        if ((local_rc && ri->ri_valid) || (!local_rc && !ri->ri_valid))
        {
            SIMPLE_LOG_MSG(LL_ERROR, "regexec(): %s (expected-valid=%s) rc=%d",
                           ri->ri_item, ri->ri_valid ? "true" : "false",
                           local_rc);
            rc = -EINVAL;
            break;
        }
    }

    regfree(&regex);

    return rc;
}

int main(void)
{
    size_t num_regex_tests = sizeof(regexTests) / sizeof(struct regex_test);

    int rc = 0;

    for (size_t i = 0; i < num_regex_tests; i++)
    {
        const struct regex_test *rt = &regexTests[i];
        SIMPLE_LOG_MSG(LL_DEBUG, "testing %s", rt->rt_regex);

        int local_rc = regex_tester(rt);
        if (local_rc && !rc)
            rc = local_rc;
    }

    return rc;
}

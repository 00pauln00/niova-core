/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <regex.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "log.h"
#include "config_token.h"
#include "file_util.h"

#define CONF_FILE_MAX_FILE_SIZE 1024
#define CONF_FILE_MAX_VALUE_SIZE 256
#define NUM_INPUT_FILES 3

#define MAX_TOKENS_PER_CONF_FILE 6

struct ctt_item
{
    const char        *ctti_conf_name;
    const char        *ctti_test_name;
    enum conf_token_id ctti_tokens[MAX_TOKENS_PER_CONF_FILE];
    const bool         ctti_should_fail;
};

const char * const inputFiles[NUM_INPUT_FILES] =
{
    "test/registry-test-inputs/root-obj-only.cmd",
    "test/registry-test-inputs/UUID.conf",
    "test/registry-test-inputs/IP.conf",
};

#define NUM_TEST_ITEMS 20

static const
struct ctt_item cttTestItems[NUM_TEST_ITEMS] =
{
    {
        .ctti_test_name = "valid ctl-interface command input file",
        .ctti_conf_name = "test/registry-test-inputs/root-obj-only.cmd",
        .ctti_tokens = {CT_ID_GET, CT_ID_OUTFILE},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "invalid ctl-interface command input file",
        .ctti_conf_name = "test/registry-test-inputs/root-obj-only.cmd",
        .ctti_tokens = {CT_ID_GET, CT_ID_HOSTNAME},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "UUID token test",
        .ctti_conf_name = "test/registry-test-inputs/UUID.conf",
        .ctti_tokens = {CT_ID_UUID},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "UUID with extra whitespace",
        .ctti_conf_name = "test/registry-test-inputs/UUID-ws.conf",
        .ctti_tokens = {CT_ID_UUID},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "bad UUID (1): invalid char",
        .ctti_conf_name = "test/registry-test-inputs/bad-UUID1.conf",
        .ctti_tokens = {CT_ID_UUID},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "bad UUID (2): too long",
        .ctti_conf_name = "test/registry-test-inputs/bad-UUID2.conf",
        .ctti_tokens = {CT_ID_UUID},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "bad UUID (3): plus instead of minus",
        .ctti_conf_name = "test/registry-test-inputs/bad-UUID3.conf",
        .ctti_tokens = {CT_ID_UUID},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "contents are UUID, IPADDR token specified",
        .ctti_conf_name = "test/registry-test-inputs/bad-UUID3.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "valid IPADDR",
        .ctti_conf_name = "test/registry-test-inputs/IP.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "valid IPADDR, incorrect token",
        .ctti_conf_name = "test/registry-test-inputs/IP.conf",
        .ctti_tokens = {CT_ID_OUTFILE},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "invalid IPADDR (1): token string missing char",
        .ctti_conf_name = "test/registry-test-inputs/IP-bad1.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "invalid IPADDR (2): token string too long",
        .ctti_conf_name = "test/registry-test-inputs/IP-bad2.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "invalid IPADDR (2): token string too long",
        .ctti_conf_name = "test/registry-test-inputs/IP-bad2.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "invalid IPADDR (3): 1st octet has too many numbers",
        .ctti_conf_name = "test/registry-test-inputs/IP-bad3.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "invalid IPADDR (4): 2nd octet has the value '256'",
        .ctti_conf_name = "test/registry-test-inputs/IP-bad4.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "invalid IPADDR (5): trailing char",
        .ctti_conf_name = "test/registry-test-inputs/IP-bad5.conf",
        .ctti_tokens = {CT_ID_IPADDR},
        .ctti_should_fail = true,
    },
    {
        .ctti_test_name = "IPADD and UUID",
        .ctti_conf_name = "test/registry-test-inputs/uuid+ip.conf",
        .ctti_tokens = {CT_ID_IPADDR, CT_ID_UUID},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "hostname",
        .ctti_conf_name = "test/registry-test-inputs/HOSTNAME.conf",
        .ctti_tokens = {CT_ID_HOSTNAME},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "control service local example input",
        .ctti_conf_name = "test/registry-test-inputs/ctl-svc-example.conf",
        .ctti_tokens = {CT_ID_HOSTNAME, CT_ID_PORT, CT_ID_STORE, CT_ID_UUID},
        .ctti_should_fail = false,
    },
    {
        .ctti_test_name = "apply_to",
        .ctti_conf_name = "test/registry-test-inputs/apply_to.conf",
        .ctti_tokens = {CT_ID_TO, CT_ID_APPLY},
        .ctti_should_fail = false,
    },
};

static int
ctsp_cb(const struct conf_token *ct, const char *val_buf, size_t val_buf_sz,
        void *cb_arg, int error)
{
    (void)cb_arg;

    int rc = 0;

    SIMPLE_LOG_MSG(LL_NOTIFY, "token-name %10s val_sz %02zu err %01d %s",
                   ct->ct_name, val_buf_sz, error, val_buf);

    return rc;
}

int
main(void)
{
    char *buf = calloc(1, CONF_FILE_MAX_FILE_SIZE);
    if (!buf)
        return -errno;

    char *value_buf = calloc(1, CONF_FILE_MAX_VALUE_SIZE);
    if (!value_buf)
        return -errno;

    int rc = 0;
    int nfailures = 0;

    for (int i = 0; i < NUM_TEST_ITEMS; i++)
    {
        ssize_t read_rc =
            file_util_open_and_read(AT_FDCWD, cttTestItems[i].ctti_conf_name,
                                    buf, CONF_FILE_MAX_FILE_SIZE, NULL);
        if (read_rc < 0)
        {
            rc = read_rc;
            goto out;
        }

        struct conf_token_set_parser ctsp = {0};
        conf_token_set_init(&ctsp.ctsp_cts);

        for (int j = 0; j < MAX_TOKENS_PER_CONF_FILE; j++)
            conf_token_set_enable(&ctsp.ctsp_cts,
                                  cttTestItems[i].ctti_tokens[j]);

        conf_token_set_parser_init(&ctsp, buf, read_rc, value_buf,
                                   CONF_FILE_MAX_VALUE_SIZE, ctsp_cb, NULL);

        rc = conf_token_set_parse(&ctsp);

        if (rc && cttTestItems[i].ctti_should_fail)
            rc = 0;
        else if (!rc && cttTestItems[i].ctti_should_fail)
            rc = 1;

        if (rc)
            nfailures++;

        fprintf(stdout, "\t%4s %s\n",
                rc ? "FAIL" : "OK", cttTestItems[i].ctti_test_name);
    }

out:
    free(buf);
    free(value_buf);

    return nfailures;
}

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

const char *inputFiles[NUM_INPUT_FILES] =
{
    "test/registry-test-inputs/root-obj-only.cmd",
    "test/registry-test-inputs/UUID.conf",
    "test/registry-test-inputs/IP.conf",
};

static int
ctsp_cb(const struct conf_token *ct, const char *val_buf, size_t val_buf_sz,
        void *cb_arg, int error)
{
    (void)cb_arg;

    int rc = 0;

    fprintf(stdout, "token-name %10s val_sz %02zu err %01d %s\n",
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
    if (!buf)
        return -errno;

    int rc = 0;

    for (int i = 0; i < NUM_INPUT_FILES && !rc; i++)
    {
        ssize_t read_rc =
            file_util_open_and_read(AT_FDCWD, inputFiles[i], buf,
                                    CONF_FILE_MAX_FILE_SIZE, NULL);
        if (read_rc < 0)
            return read_rc;

        struct conf_token_set_parser ctsp = {0};
        conf_token_set_init(&ctsp.ctsp_cts);
        conf_token_set_enable(&ctsp.ctsp_cts, CT_ID_GET);
        conf_token_set_enable(&ctsp.ctsp_cts, CT_ID_OUTFILE);
        conf_token_set_enable(&ctsp.ctsp_cts, CT_ID_IPADDR);
        conf_token_set_enable(&ctsp.ctsp_cts, CT_ID_UUID);

        conf_token_set_parser_init(&ctsp, buf, read_rc, value_buf,
                                   CONF_FILE_MAX_VALUE_SIZE, NULL, ctsp_cb);

        rc = conf_token_set_parse(&ctsp);
    }

    if (rc)
        SIMPLE_LOG_MSG(LL_ERROR, "conf_token_set_parse(): %s", strerror(-rc));

    free(buf);
    free(value_buf);

    return rc;
}

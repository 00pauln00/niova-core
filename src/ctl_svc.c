/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <regex.h>
#include <sys/types.h>
#include <dirent.h>

#include "ctl_svc.h"
#include "init.h"
#include "env.h"
#include "config_token.h"
#include "regex_defines.h"
#include "log.h"
#include "file_util.h"
#include "alloc.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define CTL_SVC_CONF_FILE_MAX_SIZE 1024UL
#define CTL_SVC_NUM_CONF_TOKENS 4

static const
char *ctlSvcLocalDir = CTL_SVC_DEFAULT_LOCAL_DIR;

static const
enum conf_token_id ctlSvcConfTokens[CTL_SVC_NUM_CONF_TOKENS] =
{
    CT_ID_HOSTNAME, CT_ID_PORT, CT_ID_STORE, CT_ID_UUID,
};

void
ctl_svc_set_local_dir(const struct niova_env_var *nev)
{
    if (nev && nev->nev_string)
        ctlSvcLocalDir = nev->nev_string;
}

static int
ctl_svc_ctsp_cb(const struct conf_token *ct, const char *val_buf,
                size_t val_buf_sz, void *cb_arg, int error)
{
    (void)cb_arg;

    SIMPLE_LOG_MSG(LL_NOTIFY, "token-name %10s val_sz %02zu err %01d %s",
                   ct->ct_name, val_buf_sz, error, val_buf);

    return 0;
}

static void
ctl_svc_process_conf_file(int ctl_svc_dir_fd, const char *input_file,
                          char *file_buf, size_t file_buf_sz, char *value_buf,
                          size_t value_buf_sz)
{
    ssize_t read_rc =
        file_util_open_and_read(ctl_svc_dir_fd, input_file, file_buf,
                                file_buf_sz, NULL);
    if (read_rc < 0)
    {
        LOG_MSG(LL_NOTIFY, "file_util_open_and_read(`%s'): %s",
                input_file, strerror(-read_rc));
        return;
    }

    /* File contents have been read into 'file_buf'.
     */
    struct conf_token_set_parser ctsp = {0};
    conf_token_set_init(&ctsp.ctsp_cts);

    for (int i = 0; i < CTL_SVC_NUM_CONF_TOKENS; i++)
        conf_token_set_enable(&ctsp.ctsp_cts, ctlSvcConfTokens[i]);

    conf_token_set_parser_init(&ctsp, file_buf, read_rc, value_buf,
                               value_buf_sz, ctl_svc_ctsp_cb, NULL);

    int rc = conf_token_set_parse(&ctsp);
    if (rc)
        LOG_MSG(LL_NOTIFY, "conf_token_set_parse(`%s'): %s",
                input_file, strerror(-rc));
}

static init_ctx_int_t
ctl_svc_init_scan_entries(void)
{
    int rc = 0, close_rc = 0;

    const regex_t *ctl_svc_regex = conf_token_2_regex_ptr(CT_ID_UUID);
    if (!ctl_svc_regex)
        return -ENOENT;

    int ctl_svc_dir_fd = open(ctlSvcLocalDir, O_RDONLY | O_DIRECTORY);
    if (ctl_svc_dir_fd < 0)
        return -errno;

    DIR *ctl_svc_dir = fdopendir(ctl_svc_dir_fd);
    if (!ctl_svc_dir)
    {
        rc = -errno;
        close_rc = close(ctl_svc_dir_fd);
        if (close_rc)
            SIMPLE_LOG_MSG(LL_ERROR, "close():  %s", strerror(errno));

        return rc;
    }

    char *file_buf = niova_calloc_can_fail(1UL, CTL_SVC_CONF_FILE_MAX_SIZE);
    if (!file_buf)
    {
        rc = -ENOMEM;
        goto out_close_dir;
    }

    char *value_buf = niova_calloc_can_fail(1UL, CTL_SVC_CONF_FILE_MAX_SIZE);
    if (!value_buf)
    {
        rc = -ENOMEM;
        goto out_free_file_buf;
    }

    for (struct dirent *dent = readdir(ctl_svc_dir);
         dent != NULL;
         dent = readdir(ctl_svc_dir))
    {
        int regex_rc = regexec(ctl_svc_regex, dent->d_name, 0, NULL, 0);

        SIMPLE_LOG_MSG(LL_NOTIFY, "regexec(`%s'): rc=%d",
                       dent->d_name, regex_rc);

        if (!regex_rc)
            ctl_svc_process_conf_file(ctl_svc_dir_fd, dent->d_name,
                                      file_buf, CTL_SVC_CONF_FILE_MAX_SIZE,
                                      value_buf, CTL_SVC_CONF_FILE_MAX_SIZE);
    }

    niova_free(value_buf);

out_free_file_buf:
    niova_free(file_buf);

out_close_dir:
    close_rc = closedir(ctl_svc_dir);
    if (close_rc)
        SIMPLE_LOG_MSG(LL_ERROR, "closedir():  %s", strerror(errno));

    return rc;
}

init_ctx_t
ctl_svc_init(void)
{
    int rc = ctl_svc_init_scan_entries();

    if (rc)
        SIMPLE_LOG_MSG(LL_NOTIFY, "ctl_svc_init_scan_entries(): %s",
                       strerror(-rc));
}

destroy_ctx_t
ctl_svc_destroy(void)
{
    FUNC_ENTRY(LL_NOTIFY);
}

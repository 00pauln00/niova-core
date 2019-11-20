/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "log.h"
#include "ctl_interface_cmd.h"
#include "util_thread.h"
#include "io.h"

enum ctlic_cmd_input_output
{
    CTLIC_CMD_INPUT = 0,
    CTLIC_CMD_OUTPUT,
    CTLIC_CMD_TOTAL,
};

#define CTLIC_BUFFER_SIZE 4096

/* The entire ctl interface is single threaded, executed by the util_thread.
 * This means that only a single set of buffers are needed
 */
static util_thread_ctx_ctli_char_t
ctlicBuffer[CTLIC_CMD_TOTAL][CTLIC_BUFFER_SIZE];

struct ctlic_token
{
    const char  *ct_name;
    size_t       ct_name_len;
};

struct ctlic_file
{
    const char             *cf_file_name;
    char                   *cf_buffer;
    int                     cf_fd;
    ssize_t                 cf_nbytes_written;
};

struct ctlic_request
{
    const struct cic_token *cr_token;
    const char             *cr_value;
    struct ctlic_file       cr_file[CTLIC_CMD_TOTAL];
};

#define CTLIC_NUM_CMDS 2
#define CTLIC_MAX_REQ_NAME_LEN 32

static struct ctlic_token ctlInterfaceCmds[CTLIC_NUM_CMDS] = {
    {
        .ct_name = "GET",
    },
    {
        .ct_name = "OUTFILE",
    },
};

static void
ctlic_request_prepare(struct ctlic_request *cr)
{
    if (cr)
    {
        cr->cr_token = NULL;
        cr->cr_value = NULL;

        for (int i = 0; i < CTLIC_CMD_TOTAL; i++)
        {
            cr->cr_file[i].cf_nbytes_written = 0;
            cr->cr_file[i].cf_file_name = NULL;
            cr->cr_file[i].cf_fd = -1;
            cr->cr_file[i].cf_buffer = ctlicBuffer[i];

            memset(cr->cr_file[i].cf_buffer, 0, CTLIC_BUFFER_SIZE);
        }
    }
}

static void
ctlic_request_done(struct ctlic_request *cr)
{
    if (cr)
    {
        for (int i = 0; i < CTLIC_CMD_TOTAL; i++)
        {
            if (cr->cr_file[i].cf_fd >= 0)
                close(cr->cr_file[i].cf_fd);
        }
    }
}

static int
ctlic_open_and_read_input_file(const char *input_cmd_file,
                               struct ctlic_request *cr)
{
    if (!input_cmd_file || !cr)
        return -EINVAL;

    struct stat stb;

    /* Lookup the file, check the type and file size.
     */
    int rc = stat(input_cmd_file, &stb);
    if (rc < 0)
        return -errno;

    else if (!S_ISREG(stb.st_mode))
        return -ENOTSUP;

    else if (stb.st_size >= CTLIC_BUFFER_SIZE)
        return -E2BIG;

    /* Init and assign buffers.
     */
    ctlic_request_prepare(cr);

    struct ctlic_file *cf_in = &cr->cr_file[CTLIC_CMD_INPUT];

    cf_in->cf_file_name = input_cmd_file;

    /* Open the file
     */
    cf_in->cf_fd = open(input_cmd_file, O_RDONLY);
    if (cf_in->cf_fd < 0)
        return -errno;

    /* Read the file
     */
    cf_in->cf_nbytes_written =
        io_read(cf_in->cf_fd, cf_in->cf_buffer, stb.st_size);

    /* Check for any basic errors
     */
    if (cf_in->cf_nbytes_written < 0)
    {
        rc = (int)cf_in->cf_nbytes_written;
        goto error;
    }
    /* The file's size has shrunk - ignore it
     */
    else if (cf_in->cf_nbytes_written != stb.st_size)
    {
        rc = -EMSGSIZE;
        goto error;
    }

    return 0;

error:
    ctlic_request_done(cr);
    return rc;
}

void
ctlic_process_new_cmd(const char *input_cmd_file)
{
    struct ctlic_request cr;

    int rc = ctlic_open_and_read_input_file(input_cmd_file, &cr);

    if (rc)
        SIMPLE_LOG_MSG(LL_NOTIFY, "ctlic_open_and_read_input_file(`%s'): %s",
                       input_cmd_file, strerror(-rc));

    SIMPLE_LOG_MSG(LL_WARN, "file=%s\ncontents=\n%s",
                   input_cmd_file,
                   (const char *)cr.cr_file[CTLIC_CMD_INPUT].cf_buffer);

    ctlic_request_done(&cr);
}

init_ctx_t
ctlic_init(void)
{
    for (int i = 0; i < CTLIC_NUM_CMDS; i++)
    {
        struct ctlic_token *ctlic = &ctlInterfaceCmds[i];
        if (ctlic->ct_name)
            ctlic->ct_name_len = strnlen(ctlic->ct_name,
                                         CTLIC_MAX_REQ_NAME_LEN);

        NIOVA_ASSERT(ctlic->ct_name_len < CTLIC_MAX_REQ_NAME_LEN);
    }
}

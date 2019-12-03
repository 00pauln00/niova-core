/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <regex.h>
#include <stdio.h>

#include "log.h"
#include "ctl_interface_cmd.h"
#include "ctl_interface.h"
#include "util_thread.h"
#include "io.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define CTLIC_BUFFER_SIZE        4096
#define CTLIC_MAX_TOKENS_PER_REQ 8
#define CTLIC_MAX_VALUE_SIZE     80
#define CTLIC_MAX_VALUE_DEPTH    32 // Max 'tree' depth which can be queried
#define CTLIC_MAX_REQ_NAME_LEN   32
#define CTLIC_MAX_TAB_DEPTH      (CTLIC_MAX_VALUE_DEPTH * 2)
#define CTLIC_MAX_SIBLING_CNT    16384

enum ctlic_cmd_input_output_files
{
    CTLIC_INPUT_FILE = 0,
    CTLIC_OUTPUT_FILE,
    CTLIC_NUM_FILES,
};

/* The entire ctl interface is single threaded, executed by the util_thread.
 * This means that only a single set of buffers are needed
 */
static util_thread_ctx_ctli_char_t
ctlicBuffer[CTLIC_NUM_FILES][CTLIC_BUFFER_SIZE];

enum ctl_cmd_inteface_token
{
    CTLIC_TOKEN_GET = 0,
    CTLIC_TOKEN_OUTFILE,
    CTLIC_NUM_TOKENS,
};

struct ctlic_token
{
    const char                 *ct_name;
    size_t                      ct_name_len;
    enum ctl_cmd_inteface_token ct_token_value;
};

static struct ctlic_token ctlInterfaceCmds[CTLIC_NUM_TOKENS] =
{
    [CTLIC_TOKEN_GET] {
        .ct_name = "GET",
        .ct_token_value = CTLIC_TOKEN_GET,
    },
    [CTLIC_TOKEN_OUTFILE] {
        .ct_name = "OUTFILE",
        .ct_token_value = CTLIC_TOKEN_OUTFILE,
    },
};

struct ctlic_depth_segment
{
    unsigned int cds_free_regex:1,
                 cds_free_regex_value:1,
                 cds_tab_depth:6;
    const char  *cds_str;
    const char  *cds_str_value;
    regex_t      cds_regex;
    regex_t      cds_regex_value;
};

struct ctlic_matched_token
{
    const struct ctlic_token  *cmt_token;
    size_t                     cmt_value_idx;
    char                       cmt_value[CTLIC_MAX_VALUE_SIZE];
    size_t                     cmt_current_depth;
    size_t                     cmt_num_depth_segments;
    struct ctlic_depth_segment cmt_depth_segments[CTLIC_MAX_VALUE_DEPTH];
};

struct ctlic_file
{
    const char *cf_file_name;
    char       *cf_buffer;
    int         cf_fd;
    ssize_t     cf_nbytes_written;
};

struct ctlic_request
{
    size_t                     cr_num_matched_tokens;
    size_t                     cr_current_token;
    size_t                     cr_output_byte_cnt;
    size_t                     cr_current_tab_depth;
    struct ctlic_matched_token cr_matched_token[CTLIC_MAX_TOKENS_PER_REQ];
    struct ctlic_file          cr_file[CTLIC_NUM_FILES];
};

struct ctlic_iterator
{
    struct ctlic_request *citer_cr;
    struct lreg_value     citer_lv;
    size_t                citer_starting_byte_cnt;
    size_t                citer_tab_depth;
    size_t                citer_sibling_num;
    bool                  citer_open_stanza;
};

static void
ctlic_matched_token_init(struct ctlic_matched_token *cmt)
{
    if (cmt)
    {
        cmt->cmt_token = NULL;
        cmt->cmt_value_idx = 0;
        memset(cmt->cmt_value, 0, CTLIC_MAX_VALUE_SIZE);
    }
}

static void
ctlic_request_prepare(struct ctlic_request *cr)
{
    if (cr)
    {
        cr->cr_num_matched_tokens = 0;

        for (int i = 0; i < CTLIC_MAX_TOKENS_PER_REQ; i++)
            ctlic_matched_token_init(&cr->cr_matched_token[i]);

        for (int i = 0; i < CTLIC_NUM_FILES; i++)
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
    if (!cr)
        return;

    for (int i = 0; i < CTLIC_NUM_FILES; i++)
    {
        if (cr->cr_file[i].cf_fd >= 0)
        {
            close(cr->cr_file[i].cf_fd);
            cr->cr_file[i].cf_fd = -1;
        }
    }

    for (size_t i = 0; i < cr->cr_num_matched_tokens; i++)
    {
        struct ctlic_matched_token *cmt = &cr->cr_matched_token[i];

        for (size_t j = 0; j < cmt->cmt_num_depth_segments; j++)
        {
            struct ctlic_depth_segment *cds = &cmt->cmt_depth_segments[j];

            if (cds->cds_free_regex)
                regfree(&cds->cds_regex);
        }
    }
}

static int
ctlic_open_output_file(int out_dirfd, struct ctlic_request *cr)
{
    if (out_dirfd < 0 || !cr || !cr->cr_num_matched_tokens)
        return -EINVAL;

    struct ctlic_file *cf = &cr->cr_file[CTLIC_OUTPUT_FILE];
    if (cf->cf_fd >= 0 || cf->cf_file_name)
        return -EINVAL;

    bool found = false;
    const struct ctlic_matched_token *cmt = NULL;

    /* Set the output file name.
     */
    for (size_t i = 0; i < cr->cr_num_matched_tokens; i++)
    {
        cmt = &cr->cr_matched_token[i];
        if (!cmt->cmt_token) // Something went badly wrong here..
            return -EINVAL;

        if (cmt->cmt_token->ct_token_value == CTLIC_TOKEN_OUTFILE)
        {
            found = true;
            break;
        }
    }

    if (!found || cmt->cmt_num_depth_segments != 1)
        return -EBADMSG;

    cf->cf_file_name = cmt->cmt_depth_segments[0].cds_str;

    cf->cf_fd = openat(out_dirfd, cf->cf_file_name,
                       O_WRONLY | O_CREAT | O_TRUNC, 0644);

    return cf->cf_fd < 0 ? -errno : 0;
}

static int
ctlic_open_and_read_input_file(const struct ctli_cmd_handle *cch,
                               struct ctlic_request *cr)
{
    const char *input_cmd_file = cch ? cch->ctlih_input_file_name : NULL;
    if (!input_cmd_file || !cr)
        return -EINVAL;

    struct stat stb;

    /* Lookup the file, check the type and file size.
     */
    int rc = fstatat(cch->ctlih_input_dirfd, input_cmd_file, &stb,
                     AT_SYMLINK_NOFOLLOW);
    if (rc < 0)
        return -errno;

    else if (!S_ISREG(stb.st_mode))
        return -ENOTSUP;

    else if (stb.st_size >= CTLIC_BUFFER_SIZE)
        return -E2BIG;

    /* Init and assign buffers.
     */
    ctlic_request_prepare(cr);

    struct ctlic_file *cf_in = &cr->cr_file[CTLIC_INPUT_FILE];

    cf_in->cf_file_name = input_cmd_file;

    /* Open the file
     */
    cf_in->cf_fd = openat(cch->ctlih_input_dirfd, input_cmd_file, O_RDONLY);
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

static int
ctlic_prepare_token_values(struct ctlic_matched_token *cmt)
{
    if (!cmt)
        return -EINVAL;

    for (size_t i = 0; i < cmt->cmt_num_depth_segments; i++)
    {
        struct ctlic_depth_segment *cds = &cmt->cmt_depth_segments[i];

        int rc = regcomp(&cds->cds_regex, cds->cds_str, REG_NOSUB);
        if (rc)
        {
            char err_str[64] = {0};
            regerror(rc, &cds->cds_regex, err_str, 63);

            SIMPLE_LOG_MSG(LL_WARN, "regcomp(`%s'): %s",
                           cmt->cmt_depth_segments[i].cds_str, err_str);

            return -EBADMSG;
        }
        else
        {
            cds->cds_free_regex = 1;

            SIMPLE_LOG_MSG(LL_WARN, "%s regcomp():  OK",
                           cmt->cmt_depth_segments[i].cds_str);
        }
    }

    return 0;
}

static int
ctlic_parse_token_value(struct ctlic_matched_token *cmt)
{
    if (!cmt ||
        !cmt->cmt_value_idx ||
        cmt->cmt_num_depth_segments ||
        cmt->cmt_value_idx > CTLIC_MAX_VALUE_SIZE - 1 ||
        cmt->cmt_value[0] != '/')
        return -EINVAL;

    bool escape_char = false;
    bool prev_char_was_solidus = false;

    for (size_t i = 0; i < cmt->cmt_value_idx; i++)
    {
        if (cmt->cmt_value[i] == '\\')
        {
            escape_char = true;
            continue;
        }
        else if (cmt->cmt_value[i] == '/' && !escape_char)
        {
            cmt->cmt_value[i] = '\0';
            prev_char_was_solidus = true;
            escape_char = false;
            continue;
        }
        else if (prev_char_was_solidus)
        {
            if (cmt->cmt_num_depth_segments == CTLIC_MAX_VALUE_DEPTH)
                return -E2BIG;

            struct ctlic_depth_segment *cds =
                &cmt->cmt_depth_segments[cmt->cmt_num_depth_segments++];

            cds->cds_str = &cmt->cmt_value[i];
        }

        prev_char_was_solidus = false;
        escape_char = false;
    }

    return ctlic_prepare_token_values(cmt);
}

static void
ctlic_dump_request_items(const struct ctlic_request *cr)
{
    if (!cr)
        return;

    for (size_t i = 0; i < cr->cr_num_matched_tokens; i++)
    {
        if (cr->cr_matched_token[i].cmt_token)
        {
            SIMPLE_LOG_MSG(LL_WARN, "(%s) %s -> `%s'",
                           cr->cr_file[CTLIC_INPUT_FILE].cf_file_name,
                           cr->cr_matched_token[i].cmt_token->ct_name,
                           cr->cr_matched_token[i].cmt_value);
        }
    }
}

static int
ctlic_parse_request_values(struct ctlic_request *cr)
{
    if (!cr || cr->cr_num_matched_tokens > CTLIC_MAX_TOKENS_PER_REQ)
        return -EINVAL;

    for (size_t i = 0; i < cr->cr_num_matched_tokens; i++)
    {
        int rc = ctlic_parse_token_value(&cr->cr_matched_token[i]);

        if (rc)
            return rc;
    }

    return 0;
}

static int
ctlic_parse_request(struct ctlic_request *cr)
{
    if (!cr)
        return -EINVAL;

    const struct ctlic_file *cf_in = &cr->cr_file[CTLIC_INPUT_FILE];

    if (!cf_in->cf_buffer)
        return -EINVAL;

    for (ssize_t i = 0; i < cf_in->cf_nbytes_written; i++)
    {
        const char c = cf_in->cf_buffer[i];

        struct ctlic_matched_token *cmt =
            &cr->cr_matched_token[cr->cr_num_matched_tokens];

        // First, try to detect a token such as "GET" or "OUTFILE"
        if (!cmt->cmt_token)
        {
            if (isspace(c))
                continue; // Filter out leading whitespace

            else if (!isupper(c)) // Tokens are entirely upper case
                return -EBADMSG;

            bool found = false;

            // Have the first upper case char in a word
            for (int j = 0; j < CTLIC_NUM_TOKENS; j++)
            {
                if ((ctlInterfaceCmds[j].ct_name_len + i) >
                    cf_in->cf_nbytes_written) // Check len prior to strncmp()
                    return -EBADMSG;

                if (!strncmp(ctlInterfaceCmds[j].ct_name, &cf_in->cf_buffer[i],
                             ctlInterfaceCmds[j].ct_name_len))
                {
                    cmt->cmt_token = &ctlInterfaceCmds[j];

                    // Found it, move indexer to the word's end
                    i += ctlInterfaceCmds[j].ct_name_len - 1;
                    found = true;
                }
            }

            if (!found)
                return -EBADMSG;
            else
                continue;
        }
        else // Read chars into cmt_value
        {
            if (!cmt->cmt_value_idx)
            {
                if (isspace(c))
                    continue; // Filter out leading whitespace

                else if (c != '/')
                    return -EBADMSG;
            }
            else if (c == '\n')
            {
                cr->cr_num_matched_tokens++;

                if (cr->cr_num_matched_tokens > CTLIC_MAX_TOKENS_PER_REQ)
                    return -EBADMSG;

                else
                    continue;
            }

            if (cmt->cmt_value_idx == CTLIC_MAX_VALUE_SIZE - 1)
                return -EBADMSG; // Value length check

            cmt->cmt_value[cmt->cmt_value_idx++] = c;
        }
    }

    ctlic_dump_request_items(cr);

    int rc = ctlic_parse_request_values(cr);
    if (rc)
        return rc;

    return 0;
}

static struct ctlic_matched_token *
ctlic_get_current_matched_token(struct ctlic_request *cr)
{
    NIOVA_ASSERT(cr && cr->cr_current_token < CTLIC_MAX_TOKENS_PER_REQ);

    struct ctlic_matched_token *cmt =
        &cr->cr_matched_token[cr->cr_current_token];

    NIOVA_ASSERT(cmt->cmt_token);
    NIOVA_ASSERT(cmt->cmt_num_depth_segments < CTLIC_MAX_VALUE_DEPTH);

    return cmt;
}

#if 0
static int
ctlic_scan_registry_cb_output_writer_null_stanza(struct ctlic_request *cr,
                                                 const int depth,
                                                 const int sibling_number,
                                                 const bool open_stanza,
                                                 enum lreg_node_types type)
{
    if (!cr || depth < 0 || sibling_number < 0 ||
        (type != LREG_NODE_TYPE_ARRAY && type != LREG_NODE_TYPE_OBJECT))
        return -EINVAL;

    const char *out_char = open_stanza ?
        ((type == LREG_NODE_TYPE_OBJECT) ? "{" : "[") :
        ((type == LREG_NODE_TYPE_OBJECT) ? "}" : "]");

    char tab_array[64] = {0};
    for (int i = 0; i < MIN(depth, 63); i++)
        tab_array[i] = '\t';

    int rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                     "%s%s%s%s",
                     sibling_number ? "," : "",
                     //             Final closing "}"
                     (depth || (!depth && !open_stanza)) ? "\n" : "",
                     tab_array, out_char);

    return rc >= 0 ? 0 : -errno;
}
#endif

static int
ctlic_scan_registry_cb_output_writer(struct ctlic_iterator *citer)
{
    if (!citer || !citer->citer_cr)
        return -EINVAL;

    else if (citer->citer_tab_depth > CTLIC_MAX_TAB_DEPTH ||
             citer->citer_sibling_num > CTLIC_MAX_SIBLING_CNT)
        return -E2BIG;

    const char *value_string;
    bool object_or_array = false;
    bool open_stanza = citer->citer_open_stanza;
    struct lreg_value *lv = &citer->citer_lv;
    size_t tab_depth = citer->citer_tab_depth;
    size_t sibling_number = citer->citer_sibling_num;
    struct ctlic_request *cr = citer->citer_cr;
    size_t starting_byte_cnt = citer->citer_starting_byte_cnt;

    switch (LREG_VALUE_TO_REQ_TYPE(lv))
    {
    case LREG_NODE_TYPE_OBJECT: // XXX change me to 'value type'
    case LREG_NODE_TYPE_ANON_OBJECT:
        object_or_array = true;
        value_string = open_stanza ? "{" : "}";
        break;
    case LREG_NODE_TYPE_ARRAY:
        object_or_array = true;
        value_string = open_stanza ? "[" : "]";
        break;
    default:
//XXX fix me - value needs to be type dependent
//    strings values must be surrounded by \" but numerics are not.
        value_string =
            open_stanza ? LREG_VALUE_TO_OUT_STR(lv) : NULL;
        break;
    }

    SIMPLE_LOG_MSG(LL_WARN, "key=`%s' depth=%zu sib-num=%zu open=%d",
                   lv->lrv_key_string, tab_depth, sibling_number, open_stanza);

    char tab_array[CTLIC_MAX_TAB_DEPTH] = {0};
    for (int i = 0; i < MIN(tab_depth, CTLIC_MAX_TAB_DEPTH); i++)
        tab_array[i] = '\t';

    int rc = 0;

    if (open_stanza)
    {
        if (tab_depth > 0)
        {
            switch (LREG_VALUE_TO_REQ_TYPE(lv))
            {
            case LREG_NODE_TYPE_ANON_OBJECT:
                rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                             "%s\n%s%s",
                             sibling_number > 0 ? "," : "",
                             tab_array,
                             value_string);
                break;
            case LREG_NODE_TYPE_ARRAY:
            case LREG_NODE_TYPE_OBJECT:
            case LREG_NODE_TYPE_STRING:
                rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                             object_or_array ?
                             "%s\n%s\"%s\" : %s" :
                             "%s\n%s\"%s\" : \"%s\"",
                             sibling_number > 0 ? "," : "",
                             tab_array,
                             lv->lrv_key_string,
                             value_string);
                break;
            case LREG_NODE_TYPE_BOOL:
                rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                             "%s\n%s\"%s\" : %s",
                             sibling_number > 0 ? "," : "",
                             tab_array,
                             lv->lrv_key_string,
                             LREG_VALUE_TO_BOOL(lv) ?
                             "true" : "false");
                break;
            case LREG_NODE_TYPE_SIGNED_VAL:
                rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                             "%s\n%s\"%s\" : %ld",
                             sibling_number > 0 ? "," : "",
                             tab_array,
                             lv->lrv_key_string,
                             LREG_VALUE_TO_OUT_SIGNED_INT(lv));
                break;
            case LREG_NODE_TYPE_UNSIGNED_VAL:
                rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                             "%s\n%s\"%s\" : %lu",
                             sibling_number > 0 ? "," : "",
                             tab_array,
                             lv->lrv_key_string,
                             LREG_VALUE_TO_OUT_UNSIGNED_INT(lv));
                break;
            case LREG_NODE_TYPE_FLOAT_VAL:
                rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                             "%s\n%s\"%s\" : %f",
                             sibling_number > 0 ? "," : "",
                             tab_array,
                             lv->lrv_key_string,
                             LREG_VALUE_TO_OUT_FLOAT(lv));
                break;
            default:
                break;
            }
        }
        else
        {
            rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd, "{");
        }
    }

    else if (value_string)
    {
            rc = dprintf(cr->cr_file[CTLIC_OUTPUT_FILE].cf_fd,
                         "%s%s%s%s",
                         cr->cr_output_byte_cnt > starting_byte_cnt ?
                         "\n" : "",
                         cr->cr_output_byte_cnt > starting_byte_cnt ?
                         tab_array : "",
                         value_string,
                         // Add a newline if this closes the final stanza
                         !tab_depth ? "\n" : "");
    }

    if (rc > 0)
        cr->cr_output_byte_cnt += rc;

    citer->citer_open_stanza = false;
    citer->citer_starting_byte_cnt = cr->cr_output_byte_cnt;

    return rc >= 0 ? 0 : -errno;
}

static bool // return 'false' to terminate scan
ctlic_scan_registry_cb(struct lreg_node *lrn, void *arg, const int depth)
{
    if (!lrn)
        return false;

    struct ctlic_iterator *parent_citer = arg;

    NIOVA_ASSERT(parent_citer && parent_citer->citer_cr && depth >= 0);

    struct ctlic_request *cr = parent_citer->citer_cr;
    struct ctlic_matched_token *cmt = ctlic_get_current_matched_token(cr);

    struct ctlic_iterator my_citer = {
        .citer_cr = cr,
        .citer_starting_byte_cnt = cr->cr_output_byte_cnt,
        .citer_tab_depth = parent_citer->citer_tab_depth + 1,
        .citer_sibling_num = parent_citer->citer_sibling_num,
        .citer_open_stanza = true,
    };

    if (cmt->cmt_token->ct_token_value == CTLIC_TOKEN_GET)
    {
        /* Do not exceed the depth specified in the GET request.
         */
        if (depth - 1 >= cmt->cmt_num_depth_segments)
            return false;

        /* Subtract '1' from depth since depth '0' is the root ('/')
         */
        struct ctlic_depth_segment *cds = &cmt->cmt_depth_segments[depth - 1];

        struct lreg_value *lv = &my_citer.citer_lv;

// XXx this all needs to be changed so that any member of an object or array
// can be matched
        int rc = lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_GET_NODE_INFO, lrn, lv);
        if (rc)
            return false;

        rc = regexec(&cds->cds_regex, LREG_VALUE_TO_OUT_STR(lv), 0, NULL,
                     REG_NOTBOL | REG_NOTEOL);

//Xxx this log installation should not post an event on the pipe
// since it's the util thread (it doesn't need synchro)
        DBG_LREG_NODE(LL_WARN, lrn,
                      "matched: %s (depth=%d, sib-num=%zd) (cds=%s) nseg=%zu",
                      rc ? "no" : "yes", depth,
                      parent_citer->citer_sibling_num, cds->cds_str,
                      cmt->cmt_num_depth_segments);

        if (rc)
            return true;

        int depth_add = 1;
        if (lrn->lrn_node_type == LREG_NODE_TYPE_ANON_OBJECT)
        {
            ctlic_scan_registry_cb_output_writer(&my_citer);
            depth_add++;
        }

        const unsigned int nkeys = lv->get.lrv_num_keys_out;

        for (unsigned int i = 0; i < nkeys; i++)
        {
            struct ctlic_iterator kv_citer = {
                .citer_cr = cr,
                .citer_starting_byte_cnt = cr->cr_output_byte_cnt,
                .citer_tab_depth = parent_citer->citer_tab_depth + depth_add,
                .citer_sibling_num = i,
                .citer_open_stanza = true,
                .citer_lv = {.lrv_value_idx_in = i},
            };

            struct lreg_value *kv_lv = &kv_citer.citer_lv;

            rc = lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_READ_VAL, lrn, kv_lv);

            DBG_LREG_NODE(LL_WARN, lrn, "rc=%d", rc);
            if (rc)
                return false;

            ctlic_scan_registry_cb_output_writer(&kv_citer);

            if (cmt->cmt_num_depth_segments > depth &&
                (LREG_VALUE_TO_REQ_TYPE(kv_lv) == LREG_NODE_TYPE_OBJECT ||
                 LREG_VALUE_TO_REQ_TYPE(kv_lv) == LREG_NODE_TYPE_ARRAY))
            {
                lreg_node_walk(lrn, ctlic_scan_registry_cb, (void *)&kv_citer,
                               depth + 1, LREG_VALUE_TO_USER_TYPE(kv_lv));
            }

            ctlic_scan_registry_cb_output_writer(&kv_citer);
        }
    }

    if (lrn->lrn_node_type == LREG_NODE_TYPE_ANON_OBJECT)
        ctlic_scan_registry_cb_output_writer(&my_citer);

    parent_citer->citer_sibling_num++;

    return true;
}

static void
ctlic_scan_registry(struct ctlic_request *cr)
{
    if (!cr)
        return;

    struct lreg_node *lrn_root = lreg_root_node_get();
    if (!lrn_root)
        return;

    struct ctlic_iterator citer = {
        .citer_cr = cr,
        .citer_starting_byte_cnt = 0,
        .citer_tab_depth = 0,
        .citer_sibling_num = 0,
        .citer_open_stanza = true,
    };

    int rc =
        lreg_node_exec_lrn_cb(LREG_NODE_CB_OP_GET_NAME, lrn_root,
                              &citer.citer_lv);
    if (rc)
        return;

    /* This should just print the opening "{" with no prepended "`key` =" since
     * the root object is anonymous.  By bookending the token loop (just below)
     * we allow the user to place multiple GET calls into the cmd file to
     * create custom JSON outputs.
     */
    rc = ctlic_scan_registry_cb_output_writer(&citer);
    if (rc)
        return;

    for (cr->cr_current_token = 0;
         cr->cr_current_token < cr->cr_num_matched_tokens;
         cr->cr_current_token++)
    {
        const struct ctlic_matched_token *cmt =
            &cr->cr_matched_token[cr->cr_current_token];

        if (cmt->cmt_token->ct_token_value == CTLIC_TOKEN_GET)
            lreg_node_walk(lrn_root, ctlic_scan_registry_cb, (void *)&citer, 1,
                           LREG_USER_TYPE_ANY);
    }

    ctlic_scan_registry_cb_output_writer(&citer);
}

util_thread_ctx_ctli_t
ctlic_process_request(const struct ctli_cmd_handle *cch)
{
    if (!cch || !cch->ctlih_input_file_name || cch->ctlih_output_dirfd < 0)
        return;

    struct ctlic_request cr = {0};

    int rc = ctlic_open_and_read_input_file(cch, &cr);

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_NOTIFY, "ctlic_open_and_read_input_file(`%s'): %s",
                       cch->ctlih_input_file_name, strerror(-rc));
        return;
    }

    SIMPLE_LOG_MSG(LL_WARN, "file=%s\ncontents=\n%s",
                   cch->ctlih_input_file_name,
                   (const char *)cr.cr_file[CTLIC_INPUT_FILE].cf_buffer);

    rc = ctlic_parse_request(&cr);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "invalid %s:  file=%s\ncontents=\n%s",
                       strerror(-rc), cch->ctlih_input_file_name,
                       (const char *)cr.cr_file[CTLIC_INPUT_FILE].cf_buffer);
        goto done;
    }

    rc = ctlic_open_output_file(cch->ctlih_output_dirfd, &cr);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "ctlic_open_output_file(): %s", strerror(-rc));
        goto done;
    }

    ctlic_scan_registry(&cr);

done:
    ctlic_request_done(&cr);
}

init_ctx_t
ctlic_init(void)
{
    for (int i = 0; i < CTLIC_NUM_TOKENS; i++)
    {
        struct ctlic_token *ctlic = &ctlInterfaceCmds[i];
        if (ctlic->ct_name)
            ctlic->ct_name_len = strnlen(ctlic->ct_name,
                                         CTLIC_MAX_REQ_NAME_LEN);

        NIOVA_ASSERT(ctlic->ct_name_len < CTLIC_MAX_REQ_NAME_LEN);
    }
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <ctype.h>

#include "log.h"
#include "config_token.h"
#include "regex_defines.h"

static
struct conf_token confTokens[CT_ID__MAX] =
{
    [CT_ID_GET] {
        .ct_name = "GET",
        .ct_name_len = 3,
        .ct_val_regex = NULL, //todo
        .ct_id = CT_ID_GET,
    },
    [CT_ID_OUTFILE] {
        .ct_name = "OUTFILE",
        .ct_name_len = 7,
        .ct_val_regex = NULL, //todo
        .ct_id = CT_ID_OUTFILE,
    },
    [CT_ID_HOSTNAME] {
        .ct_name = "HOSTNAME",
        .ct_name_len = 8,
        .ct_val_regex = HOSTNAME_REGEX,
        .ct_id = CT_ID_HOSTNAME,
    },
    [CT_ID_PORT] {
        .ct_name = "PORT",
        .ct_name_len = 4,
        .ct_val_regex = PORT_REGEX,
        .ct_id = CT_ID_PORT,
    },
    [CT_ID_STORE] {
        .ct_name = "STORE",
        .ct_name_len = 5,
        .ct_val_regex = NULL,
        .ct_id = CT_ID_STORE,
    },
    [CT_ID_UUID] {
        .ct_name = "UUID",
        .ct_name_len = 4,
        .ct_val_regex = UUID_REGEX,
        .ct_id = CT_ID_UUID,
    },
    [CT_ID_IPADDR] {
        .ct_name = "IPADDR",
        .ct_name_len = 6,
        .ct_val_regex = IPADDR_REGEX,
        .ct_id = CT_ID_IPADDR,
    },
    [CT_ID_CTL_SVC_FILENAME] {
        .ct_name = "",
        .ct_name_len = 0,
        .ct_val_regex = UUID_REGEX_CTL_SVC_FILE_NAME,
        .ct_id = CT_ID_CTL_SVC_FILENAME,
    },
    [CT_ID_RAFT] {
        .ct_name = "RAFT",
        .ct_name_len = 4,
        .ct_val_regex = UUID_REGEX,
        .ct_id = CT_ID_RAFT,
    },
    [CT_ID_PEER] {
        .ct_name = "PEER",
        .ct_name_len = 4,
        .ct_val_regex = UUID_REGEX,
        .ct_id = CT_ID_PEER,
    },
};

const regex_t *
conf_token_2_regex_ptr(enum conf_token_id token_id)
{
    return (confTokens[token_id].ct_val_regex &&
            confTokens[token_id].ct_regex_allocated) ?
        &confTokens[token_id].ct_regex : NULL;
}

void
conf_token_set_init(struct conf_token_set *cts)
{
    if (!cts)
        return;

    for (int i = 0; i < CT_ID__MAX; i++)
        cts->cts_tokens[i] = NULL;
}

static bool
conf_token_set_args_valid(const struct conf_token_set *cts,
                          enum conf_token_id token_id)
{
    return (cts && token_id > CT_ID__MIN && token_id < CT_ID__MAX) ?
        true : false;
}

void
conf_token_set_enable(struct conf_token_set *cts, enum conf_token_id token_id)
{
    if (conf_token_set_args_valid(cts, token_id))
	cts->cts_tokens[token_id] = &confTokens[token_id];
}

void
conf_token_set_disable(struct conf_token_set *cts, enum conf_token_id token_id)
{
    if (conf_token_set_args_valid(cts, token_id))
	cts->cts_tokens[token_id] = NULL;
}

bool
conf_token_set_token_is_enabled(const struct conf_token_set *cts,
                                enum conf_token_id token_id)
{
    if (conf_token_set_args_valid(cts, token_id))
        return cts->cts_tokens[token_id] ? true : false;

    return false;
}

const struct conf_token *
conf_token_set_get(const struct conf_token_set *cts,
                   enum conf_token_id token_id)
{
    if (conf_token_set_args_valid(cts, token_id))
        return cts->cts_tokens[token_id];

    return NULL;
}

/**
 * conf_token_set_parser_init - initialize token parsing of config info.
 * @ctsp: Set parser 'handle' - this is the structure that manages
 *    the parsing.
 * @cts:  Token set.  The group of tokens, and their attributes, which are
 *    valid for this operation.
 * @input_buf:  The buffer containing the config input.
 * @input_buf_size:  The size of the input buffer.
 * @value_buf:  Output buffer for the 'value' data associated with the current
 *    token.
 * @value_buf_size:  Size of the output buffer.
 * @cb_arg:  Argument for the callback.
 * @ctsp_cb:  Callback function which is called when a token match or matching
 *    error is found.
 */
int
conf_token_set_parser_init(struct conf_token_set_parser *ctsp,
                           const char *input_buf,
                           size_t input_buf_size,
                           char *value_buf,
                           size_t value_buf_size,
                           int (*ctsp_cb)(const struct conf_token *,
                                          const char *, size_t, void *, int),
                           void *cb_arg)
{
    if (ctsp && input_buf && input_buf_size > 0 && value_buf &&
        value_buf_size > 0 && ctsp_cb)
    {
        ctsp->ctsp_input_buf = input_buf;
        ctsp->ctsp_input_buf_size = input_buf_size;
	ctsp->ctsp_value_buf = value_buf;
	ctsp->ctsp_value_buf_size = value_buf_size;
        ctsp->ctsp_cb = ctsp_cb;
        ctsp->ctsp_cb_arg = cb_arg;
        ctsp->ctsp_input_buf_off = 0;

	return 0;
    }

    return -EINVAL;
}

static int
conf_token_regex_compile(struct conf_token *ct)
{
    if (!ct || !ct->ct_val_regex)
        return 0;

    int rc = regcomp(&ct->ct_regex, ct->ct_val_regex, REG_NOSUB);
    if (rc)
    {
        char err_str[64] = {0};
	regerror(rc, &ct->ct_regex, err_str, 63);

        SIMPLE_LOG_MSG(LL_ERROR,
                       "conf-token=`%s' has invalid regex=`%s': %s",
                       ct->ct_name, ct->ct_val_regex, err_str);

        return -EBADMSG;
    }

    ct->ct_regex_allocated = 1;

    return 0;
}

static void
conf_token_regex_destroy(struct conf_token *ct)
{
    if (ct && ct->ct_val_regex && ct->ct_regex_allocated)
    {
        ct->ct_regex_allocated = 0;
        regfree(&ct->ct_regex);
    }
}

static const struct conf_token *
conf_token_set_parse_match_token(const char *input_buf, size_t input_buf_size,
                                 const struct conf_token_set *cts)
{
    if (!input_buf || !input_buf_size || !cts)
        return NULL;

    const struct conf_token *ct = NULL;
    bool found = false;

    for (enum conf_token_id i = CT_ID__MIN; i < CT_ID__MAX && !found; i++)
    {
        ct = cts->cts_tokens[i];

        if (!ct || !ct->ct_name)
            continue;

        // Check len prior to strncmp()
        if (ct->ct_name_len + 1 > input_buf_size)
            return NULL;

        // The token string be immediately followed by a tab or space.
        found = (strncmp(ct->ct_name, input_buf, ct->ct_name_len) ||
                 (input_buf[ct->ct_name_len] != ' ' &&
                  input_buf[ct->ct_name_len] != '\t')) ?
            false : true;
    }

    return found ? ct : NULL;
}

static int
conf_token_value_check_regex(const struct conf_token *ct,
                             const char *value_buf)

{
    if (!ct || !value_buf)
        return -EINVAL;

    SIMPLE_LOG_MSG(LL_TRACE, "value-buf=%s", value_buf);

    return (ct->ct_name && ct->ct_val_regex && ct->ct_regex_allocated) ?
        regexec(&ct->ct_regex, value_buf, 0, NULL, 0) :
        0;
}

int
conf_token_set_parse(struct conf_token_set_parser *ctsp)
{
    if (!ctsp || !ctsp->ctsp_input_buf || !ctsp->ctsp_input_buf_size ||
        !ctsp->ctsp_value_buf || !ctsp->ctsp_value_buf_size || !ctsp->ctsp_cb)
        return -EINVAL;

    const struct conf_token *ct = NULL;
    size_t value_buf_idx = 0;
    bool inside_comment = false;

    for (size_t i = ctsp->ctsp_input_buf_off;
         i < ctsp->ctsp_input_buf_size; i++, ctsp->ctsp_input_buf_off++)
    {
        const char c = ctsp->ctsp_input_buf[i];

        if (!ct) // Try to find a token at the current buffer offset
        {
            NIOVA_ASSERT(!value_buf_idx);

            if (c == '#')
            {
                inside_comment = true;
                continue;
            }
            else if (inside_comment)
            {
                if (c == '\n')
                    inside_comment = false;
                continue;
            }
            else if (isspace(c))
            {
                continue; // Filter out leading whitespace
            }

            ct =
                conf_token_set_parse_match_token(&ctsp->ctsp_input_buf[i],
                                                 ctsp->ctsp_input_buf_size - i,
                                                 &ctsp->ctsp_cts);
            if (!ct)
                return -EBADMSG; // A valid token was not found

            // Found it!  Move indexer to the token's end.
            i += ct->ct_name_len - 1;

            continue;
        }
        else // Read chars into value[]
        {
            if (!value_buf_idx && isspace(c))
                continue; // Filter out leading whitespace

            if (c == '\n') // Token values must be terminated with newlines
            {
                ctsp->ctsp_value_buf[value_buf_idx] = '\0';

                if (conf_token_value_check_regex(ct, ctsp->ctsp_value_buf))
                {
                    SIMPLE_LOG_MSG(LL_NOTIFY,
                                   "value-buf=%s failed regex(`%s')",
                                   ctsp->ctsp_value_buf, ct->ct_val_regex);
                    return -EBADMSG;
                }

                int cb_rc =
                    ctsp->ctsp_cb(ct, ctsp->ctsp_value_buf,
                                  value_buf_idx, ctsp->ctsp_cb_arg, 0);

                if (cb_rc)
                    return cb_rc;

                ct = NULL;
                value_buf_idx = 0;
            }
            else
            {
                if (value_buf_idx == ctsp->ctsp_value_buf_size - 1)
                    return -EBADMSG; // Value length check

                ctsp->ctsp_value_buf[value_buf_idx++] = c;
            }
        }
    }

    return 0;
}

init_ctx_t
conf_token_svc_init(void)
{
    for (enum conf_token_id i = CT_ID__MIN; i < CT_ID__MAX; i++)
    {
        struct conf_token *ct = &confTokens[i];

        int rc = conf_token_regex_compile(ct);
        FATAL_IF((rc), "conf_token_regex_compile(): %s", strerror(-rc));
    }
}

destroy_ctx_t
conf_token_svc_destroy(void)
{
    for (enum conf_token_id i = CT_ID__MIN; i < CT_ID__MAX; i++)
        conf_token_regex_destroy(&confTokens[i]);
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _CONFIG_TOKEN_
#define _CONFIG_TOKEN_ 1

#include <regex.h>

#include "common.h"

enum conf_token_id
{
    CT_ID__MIN = 0,
    CT_ID_APPLY,
    CT_ID_CLIENT_PORT,
    CT_ID_CTL_SVC_FILENAME,
    CT_ID_GET,
    CT_ID_HOSTNAME,
    CT_ID_IPADDR,
    CT_ID_OUTFILE,
    CT_ID_PEER,
    CT_ID_PORT,
    CT_ID_RAFT,
    CT_ID_RESET,  // special 'modify' which restores default value
    CT_ID_STORE,
    CT_ID_UUID,
    CT_ID_WHERE,
    CT_ID__MAX
};

struct conf_token
{
    const char        *ct_name;
    const char        *ct_val_regex;
    regex_t            ct_regex;
    enum conf_token_id ct_id;
    unsigned int       ct_name_len:31;
    unsigned int       ct_regex_allocated:1;
};

struct conf_token_set
{
    const struct conf_token *cts_tokens[CT_ID__MAX];
};

struct conf_token_set_parser
{
    struct conf_token_set ctsp_cts;
    const char           *ctsp_input_buf;
    size_t                ctsp_input_buf_size;
    size_t                ctsp_input_buf_off;
    char                 *ctsp_value_buf;
    size_t                ctsp_value_buf_size;
    int                   ctsp_parse_err;
    int                 (*ctsp_cb)(const struct conf_token *,
                                   const char *, size_t, void *, int);
    void                 *ctsp_cb_arg;
};

const regex_t *
conf_token_2_regex_ptr(enum conf_token_id token_id);

int
conf_token_set_parser_init(struct conf_token_set_parser *ctsp,
                           const char *input_buf,
                           size_t input_buf_size,
                           char *value_buf,
                           size_t value_buf_size,
                           int (*ctsp_cb)(const struct conf_token *,
                                          const char *, size_t, void *, int),
                           void *cb_arg);

void
conf_token_set_init(struct conf_token_set *cts);

void
conf_token_set_enable(struct conf_token_set *cts, enum conf_token_id token_id);

void
conf_token_set_disable(struct conf_token_set *cts, enum conf_token_id token_id);

bool
conf_token_set_token_is_enabled(const struct conf_token_set *cts,
                                enum conf_token_id token_id);

int
conf_token_set_parse(struct conf_token_set_parser *ctsp);

init_ctx_t
conf_token_svc_init(void)
    __attribute__ ((constructor (CONFIG_TOKEN_CTOR_PRIORITY)));

destroy_ctx_t
conf_token_svc_destroy(void)
    __attribute__ ((destructor (CONFIG_TOKEN_CTOR_PRIORITY)));


#endif

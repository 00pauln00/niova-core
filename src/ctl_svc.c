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
#include "ref_tree_proto.h"

#define CTL_SVC_CONF_FILE_MAX_SIZE 1024UL
#define CTL_SVC_NUM_CONF_TOKENS 9

REGISTRY_ENTRY_FILE_GENERATE;

static struct ctl_svc_node_tree ctlSvcNodeTree;
static const char *ctlSvcLocalDir = CTL_SVC_DEFAULT_LOCAL_DIR;


#define CTL_SVR_NODE_TOKEN_HNDLR(name)                                  \
    static int                                                          \
    ctl_svc_node_token_hndlr_ ##name (struct ctl_svc_node *,            \
                                      const struct conf_token *,        \
                                      const char *, size_t)

REF_TREE_HEAD(ctl_svc_node_tree, ctl_svc_node);

REF_TREE_GENERATE(ctl_svc_node_tree, ctl_svc_node, csn_rtentry,
                  ctl_svc_node_cmp);

struct ctl_svc_node_type_suffix
{
    const char            *csnts_file_suffix;
    unsigned int           csnts_file_suffix_len;
    enum ctl_svc_node_type csnts_type;
};

/**
 * ctlSvcNodeTypes is a table of ctl-svc file extensions.  Files placed in the
 *     ctl-svc directory MUST have an extension which matches an entry.
 *     To add a new entry to this table, first add the appropriate entry to
       'enum ctl_svc_node_type' (found in ctl_svc.h)
 */
static const
struct ctl_svc_node_type_suffix ctlSvcNodeTypes[CTL_SVC_NODE_TYPE_MAX] =
{
    [CTL_SVC_NODE_TYPE_NIOSD] {
        .csnts_file_suffix = "niosd",
        .csnts_file_suffix_len = 5,
        .csnts_type = CTL_SVC_NODE_TYPE_NIOSD,
    },
    [CTL_SVC_NODE_TYPE_RAFT] {
        .csnts_file_suffix = "raft",
        .csnts_file_suffix_len = 4,
        .csnts_type = CTL_SVC_NODE_TYPE_RAFT,
    },
    [CTL_SVC_NODE_TYPE_RAFT_PEER] {
        .csnts_file_suffix = "peer",
        .csnts_file_suffix_len = 5,
        .csnts_type = CTL_SVC_NODE_TYPE_RAFT_PEER,
    },
    [CTL_SVC_NODE_TYPE_ANY] {
        .csnts_file_suffix = "", // This is effectively an invalid entry
        .csnts_file_suffix_len = 0,
        .csnts_type = CTL_SVC_NODE_TYPE_ANY,
    },
};

/**
 * ctlSvcNodeTypeTokens is a 2D array which holds the set of allowable tokens
 *    for a given ctl-svc file type.  Note that the 2D array is of type
 *    'unsigned int' where as the assignments are derived from 2 different
 *    enumerated types.
 */
static const unsigned int
ctlSvcNodeTypeTokens[CTL_SVC_NODE_TYPE_MAX][CT_ID__MAX] =
{
    [CTL_SVC_NODE_TYPE_NIOSD] = {
        CT_ID_HOSTNAME,
        CT_ID_PORT,
        CT_ID_STORE,
        CT_ID_UUID,
        CT_ID_IPADDR,
    },
    [CTL_SVC_NODE_TYPE_RAFT] = {
        CT_ID_RAFT,
        CT_ID_UUID,
        CT_ID_PEER,
    },
    [CTL_SVC_NODE_TYPE_RAFT_PEER] = {
        CT_ID_RAFT,
        CT_ID_UUID,
        CT_ID_IPADDR,
        CT_ID_STORE,
        CT_ID_PORT,
        CT_ID_HOSTNAME,
    },
};

/**
 * Macro'fied forward declarations for ctlSvcNodeTokenFuncTable.
 */
CTL_SVR_NODE_TOKEN_HNDLR(HOSTNAME);
CTL_SVR_NODE_TOKEN_HNDLR(IPADDR);
CTL_SVR_NODE_TOKEN_HNDLR(PEER);
CTL_SVR_NODE_TOKEN_HNDLR(PORT);
CTL_SVR_NODE_TOKEN_HNDLR(RAFT);
CTL_SVR_NODE_TOKEN_HNDLR(STORE);
CTL_SVR_NODE_TOKEN_HNDLR(UUID);

/**
 * ctlSvcNodeTokenFuncTable - is an array of function pointers used for
 *   applying token values to the ctl_svc_node.  To enable a token not already
 *   identified here, a handler must be written and its forward declaration
 *   should be added directly above.
 */
static int
(*ctlSvcNodeTokenFuncTable[CT_ID__MAX])(struct ctl_svc_node *,
                                        const struct conf_token *,
                                        const char *, size_t) =
{
    [CT_ID_HOSTNAME] = ctl_svc_node_token_hndlr_HOSTNAME,
    [CT_ID_IPADDR]   = ctl_svc_node_token_hndlr_IPADDR,
    [CT_ID_PEER]     = ctl_svc_node_token_hndlr_PEER,
    [CT_ID_PORT]     = ctl_svc_node_token_hndlr_PORT,
    [CT_ID_RAFT]     = ctl_svc_node_token_hndlr_RAFT,
    [CT_ID_STORE]    = ctl_svc_node_token_hndlr_STORE,
    [CT_ID_UUID]     = ctl_svc_node_token_hndlr_UUID,
};

/**
 * ctl_svc_setup_token_parser - configures the provided conf_token_set_parser
 *    with the tokens specified in the ctlSvcNodeTypeTokens array.
 */
static int
ctl_svc_setup_token_parser(struct conf_token_set_parser *ctsp,
                           enum ctl_svc_node_type csn_type)
{
    if (csn_type >= CTL_SVC_NODE_TYPE_ANY)
        return -EOPNOTSUPP;

    conf_token_set_init(&ctsp->ctsp_cts);

    int i = 0;
    while (ctlSvcNodeTypeTokens[csn_type][i] != CT_ID__MIN &&
           i < CT_ID__MAX)
        conf_token_set_enable(&ctsp->ctsp_cts,
                              ctlSvcNodeTypeTokens[csn_type][i++]);

    return 0;
}

/**
 * ctl_svr_node_type_accepts_token - helper function which returns 'true' if
 *    the ctl_svc_node_type is configured to allow tokens of type 'token_id'.
 */
static bool
ctl_svr_node_type_accepts_token(enum ctl_svc_node_type csn_type,
                                enum conf_token_id token_id)
{
    if (csn_type < CTL_SVC_NODE_TYPE_MAX)
    {
        for (int i = 0;
             i < CT_ID__MAX &&
                 ctlSvcNodeTypeTokens[csn_type][i] != CT_ID__MIN; i++)
            if (token_id == ctlSvcNodeTypeTokens[csn_type][i])
                return true;
    }
    return false;
}

/**
 * ctl_svc_raft_node_add_peer - helper function which inserts the provided
 *    raft peer UUID into the ctl_svc_node.  This function checks the type of
 *    the csn, the number of raft peers already present, and the validity of
 *    UUID string.
 */
static int
ctl_svc_raft_node_add_peer(struct ctl_svc_node *csn, const char *uuid_str)
{
    if (!csn || !uuid_str)
        return -EINVAL;

    else if (csn->csn_type != CTL_SVC_NODE_TYPE_RAFT)
        return -EOPNOTSUPP;

    else if (csn->csn_raft.csnr_num_members == CTL_SVC_MAX_RAFT_PEERS)
        return -ENOSPC;

    NIOVA_ASSERT(csn->csn_raft.csnr_num_members <= CTL_SVC_MAX_RAFT_PEERS);

    uuid_t tmp_uuid;

    if (uuid_parse(uuid_str, tmp_uuid))
        return -EBADMSG;

    raft_peer_t npeers = csn->csn_raft.csnr_num_members++;

    uuid_copy(csn->csn_raft.csnr_members[npeers].csrm_peer, tmp_uuid);

    return 0;
}

static enum ctl_svc_node_type
ctl_svc_detect_node_type(const char *input_file)
{
    size_t name_len = strnlen(input_file, PATH_MAX);
    if (name_len < UUID_STR_LEN - 1)
        return CTL_SVC_NODE_TYPE_ANY;

    size_t i;
    for (i = name_len; i > 0; i--)
        if (input_file[i - 1] == '.')
            break;

    if (i > 0)
    {
        for (enum ctl_svc_node_type j = CTL_SVC_NODE_TYPE_NIOSD;
             j < CTL_SVC_NODE_TYPE_MAX; j++)
        {
            if (!strncmp(ctlSvcNodeTypes[j].csnts_file_suffix, &input_file[i],
                         ctlSvcNodeTypes[j].csnts_file_suffix_len))
                return ctlSvcNodeTypes[j].csnts_type;
        }
    }

    return CTL_SVC_NODE_TYPE_ANY;
}

/**
 * ctl_svc_process_conf_file_parse_input_file_name - converts the input file
 *    name into binary representation for the file's type (based on the
 *    file extension) and the UUID file name prefix.
 * @csn:  the ctl-svc node structure to be written.
 * @input_file_name:  name of the input file.
 */
static enum ctl_svc_node_type
ctl_svc_parse_input_file_name(struct ctl_svc_node *csn,
                              const char *input_file_name)
{
    enum ctl_svc_node_type csn_type =
        ctl_svc_detect_node_type(input_file_name);

    if (csn_type >= CTL_SVC_NODE_TYPE_ANY)
        return csn_type;

    csn->csn_type = csn_type;

    int rc = uuid_parse(input_file_name, csn->csn_uuid);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "uuid_parse() returns %d", rc);
        return CTL_SVC_NODE_TYPE_ANY;
    }

    DBG_CTL_SVC_NODE(LL_WARN, csn, "");

    return csn_type;
}

static int
ctl_svc_node_token_hndlr_HOSTNAME(struct ctl_svc_node *csn,
                                  const struct conf_token *ct,
                                  const char *val_buf, size_t val_buf_sz)
{
    if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT)
        return -EINVAL;

    else if (val_buf_sz >= HOST_NAME_MAX)
        return -ENAMETOOLONG;

    strncpy(csn->csn_peer.csnp_hostname, val_buf, val_buf_sz);

    DBG_CTL_SVC_NODE(LL_DEBUG, csn, "%s", csn->csn_peer.csnp_hostname);

    return 0;
}

static int
ctl_svc_node_token_hndlr_PORT(struct ctl_svc_node *csn,
                              const struct conf_token *ct,
                              const char *val_buf, size_t val_buf_sz)
{
    if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT)
        return -EINVAL;

    else if (val_buf_sz > PORT_REGEX_MAX_LEN)
        return -ENAMETOOLONG;

    const long int port = strtol(val_buf, NULL, 10);
    if ((port < 0 || port == 0 || port > 65536))
        return -ERANGE;

    csn->csn_peer.csnp_port = port;

    DBG_CTL_SVC_NODE(LL_DEBUG, csn, "%u", csn->csn_peer.csnp_port);

    return 0;
}

static int
ctl_svc_node_token_hndlr_UUID(struct ctl_svc_node *csn,
                              const struct conf_token *ct,
                              const char *val_buf, size_t val_buf_sz)
{
    if (val_buf_sz > UUID_STR_LEN)
        return -ENAMETOOLONG;

    int rc = ctl_svc_node_check_string(csn, val_buf);

    DBG_CTL_SVC_NODE((rc ? LL_WARN : LL_DEBUG), csn, "rc=%d (%s)",
                     rc, val_buf);

    return rc;
}

static int
ctl_svc_node_token_hndlr_RAFT(struct ctl_svc_node *csn,
                              const struct conf_token *ct,
                              const char *val_buf, size_t val_buf_sz)
{
    if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT)
        return ctl_svc_node_token_hndlr_UUID(csn, ct, val_buf, val_buf_sz);

    else if (csn->csn_type != CTL_SVC_NODE_TYPE_RAFT_PEER)
        return -EOPNOTSUPP;

    else if (val_buf_sz > UUID_STR_LEN)
        return -ENAMETOOLONG;

    uuid_t tmp_uuid;
    int rc = -EBADMSG;

    if (!uuid_parse(val_buf, tmp_uuid))
    {
        uuid_copy(csn->csn_peer.csnp_raft_info.csnrp_member.csrm_peer,
                  tmp_uuid);
        rc = 0;
    }

    DBG_CTL_SVC_NODE((rc ? LL_WARN : LL_DEBUG), csn, "rc=%d (%s)",
                     rc, val_buf);

    return rc;
}

static int
ctl_svc_node_token_hndlr_PEER(struct ctl_svc_node *csn,
                              const struct conf_token *ct,
                              const char *val_buf, size_t val_buf_sz)
{
    if (val_buf_sz > UUID_STR_LEN)
        return -ENAMETOOLONG;

    int rc = ctl_svc_raft_node_add_peer(csn, val_buf);
    if (rc)
        DBG_CTL_SVC_NODE(LL_WARN, csn, "ctl_svc_raft_node_add_peer(): %s",
                         strerror(-rc));

    return rc;
}

static int
ctl_svc_node_token_hndlr_STORE(struct ctl_svc_node *csn,
                               const struct conf_token *ct,
                               const char *val_buf, size_t val_buf_sz)
{
    if (val_buf_sz > PATH_MAX)
        return -ENAMETOOLONG;

    else if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT)
        return -EOPNOTSUPP;

    else if (csn->csn_peer.csnp_store != NULL) // Don't leak memory
        return -EALREADY;

    csn->csn_peer.csnp_store = niova_malloc(val_buf_sz + 1);
    if (!csn->csn_peer.csnp_store)
        return -ENOMEM;

    strncpy(csn->csn_peer.csnp_store, val_buf, val_buf_sz);
    csn->csn_peer.csnp_store[val_buf_sz] = '\0';

    DBG_CTL_SVC_NODE(LL_NOTIFY, csn, "STORE=%s", csn->csn_peer.csnp_store);

    return 0;
}

static int
ctl_svc_node_token_hndlr_IPADDR(struct ctl_svc_node *csn,
                                const struct conf_token *ct,
                                const char *val_buf, size_t val_buf_sz)
{
    if (val_buf_sz >= IPV4_STRLEN) // IPV4_STRLEN includes NULL terminator
        return -ENAMETOOLONG;

    else if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT)
        return -EOPNOTSUPP;

    strncpy(csn->csn_peer.csnp_ipv4, val_buf, val_buf_sz);
    csn->csn_peer.csnp_ipv4[val_buf_sz] = '\0';

    DBG_CTL_SVC_NODE(LL_NOTIFY, csn, "IPADDR=%s", val_buf);

    return 0;
}

static int
ctl_svc_apply_token_value_to_node(struct ctl_svc_node *csn,
                                  const struct conf_token *ct,
                                  const char *val_buf, size_t val_buf_sz)
{
    if (!csn || !ct || !val_buf || !val_buf_sz)
    {
        return -EINVAL;
    }
    else if (!ctl_svr_node_type_accepts_token(csn->csn_type, ct->ct_id))
    {
        DBG_CTL_SVC_NODE(LL_ERROR, csn, "does not accept token %s",
                         ct->ct_name);
        return -ENOENT;
    }

    return ctlSvcNodeTokenFuncTable[ct->ct_id] ?
        ctlSvcNodeTokenFuncTable[ct->ct_id](csn, ct, val_buf, val_buf_sz) : 0;
}

/**
 * ctl_svc_ctsp_cb - callback function for the config_token parser.  Here
 *    the ctl_svc_node is configured according to the token and token value
 *    provided by the parser.
 */
static int
ctl_svc_ctsp_cb(const struct conf_token *ct, const char *val_buf,
                size_t val_buf_sz, void *cb_arg, int error)
{
    struct ctl_svc_node *csn = cb_arg;

    if (csn)
        DBG_CTL_SVC_NODE(LL_NOTIFY, csn,
                         "token-name %s val_sz %02zu err %01d %s",
                         ct->ct_name, val_buf_sz, error, val_buf);

    return ctl_svc_apply_token_value_to_node(csn, ct, val_buf, val_buf_sz);
}

/**
 * ctl_svc_process_conf_file - Function which reads and parses the contents of
 *    the input_file.  It first checks the file suffix to find the right
 *    parsing handler.
 * @ctl_svc_dir_fd:  dir fd of the parent directory - used for openat().
 * @input_file:  Name of the input file.  The file must have a supported
 *    suffix.
 * @file_buf:  Buffer for the contents of the file. This is typically small,
 *    like 4k.
 * @file_buf_sz:   Size of the file buffer.
 * @value_buf:  Value for the individual line entries in the file.
 * @value_buf_sz:  Value buffer size.
 */
static int
ctl_svc_process_conf_file(int ctl_svc_dir_fd, const char *input_file,
                          char *file_buf, size_t file_buf_sz, char *value_buf,
                          size_t value_buf_sz)
{
    struct conf_token_set_parser ctsp = {0};
    struct ctl_svc_node csn = {0};

    if (ctl_svc_parse_input_file_name(&csn, input_file) >=
        CTL_SVC_NODE_TYPE_ANY)
    {
        LOG_MSG(LL_NOTIFY, "Conf file parse failed (%s)", input_file);
        return -EOPNOTSUPP;
    }

    int rc = ctl_svc_setup_token_parser(&ctsp, csn.csn_type);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "Unsupported ctl_svc file type (%s)", input_file);
        return rc;
    }

    // Read the file contents into 'file_buf'.
    ssize_t read_rc =
        file_util_open_and_read(ctl_svc_dir_fd, input_file, file_buf,
                                file_buf_sz, NULL);
    if (read_rc < 0)
    {
        LOG_MSG(LL_NOTIFY, "file_util_open_and_read(`%s'): %s",
                input_file, strerror(-read_rc));
        return rc;
    }

    conf_token_set_parser_init(&ctsp, file_buf, read_rc, value_buf,
                               value_buf_sz, ctl_svc_ctsp_cb, &csn);

    rc = conf_token_set_parse(&ctsp);
    if (rc)
        LOG_MSG(LL_NOTIFY, "conf_token_set_parse(`%s'): %s",
                input_file, strerror(-rc));

// How do we know if this call created the object and therefore should leave
// a ref intact?  This may be a more general issue..  Note that the STORE
    // token may have caused us to allocate memory which would need to be freed.
//    RT_GET_ADD();

    return rc;
}

/**
 * ctl_svc_init_scan_entries - Called from initialization context to iterate
 *    the children in the directory path found in 'ctlSvcLocalDir'.  Entries
 *    which match the regex CT_ID_CTL_SVC_FILENAME are passed on to the
 *    parsing logic in this file.
 */
static init_ctx_int_t
ctl_svc_init_scan_entries(void)
{
    int rc = 0, close_rc = 0;

    const regex_t *ctl_svc_regex =
        conf_token_2_regex_ptr(CT_ID_CTL_SVC_FILENAME);
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

        if (regex_rc)
            continue;

        int rc = ctl_svc_process_conf_file(ctl_svc_dir_fd, dent->d_name,
                                           file_buf,
                                           CTL_SVC_CONF_FILE_MAX_SIZE,
                                           value_buf,
                                           CTL_SVC_CONF_FILE_MAX_SIZE);
        if (rc)
            LOG_MSG(LL_WARN, "Processing failed for ctl-svc-file %s: %s",
                    dent->d_name, strerror(-rc));
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

static struct ctl_svc_node *
ctl_svc_node_construct(const struct ctl_svc_node *in)
{
    if (!in)
        return NULL;

    struct ctl_svc_node *csn = niova_malloc(sizeof(struct ctl_svc_node));
    if (!csn)
        return NULL;

    *csn = *in;

    DBG_CTL_SVC_NODE(LL_DEBUG, csn, "");

    return csn;
}

static int
ctl_svc_node_destruct(struct ctl_svc_node *destroy)
{
    if (!destroy)
        return -EINVAL;

    DBG_CTL_SVC_NODE(LL_DEBUG, destroy, "");

    if (ctl_svc_node_is_peer(destroy) && destroy->csn_peer.csnp_store)
    {
        niova_free(destroy->csn_peer.csnp_store);
        destroy->csn_peer.csnp_store = NULL;
    }

    niova_free(destroy);

    return 0;
}

void
ctl_svc_set_local_dir(const struct niova_env_var *nev)
{
    if (nev && nev->nev_string)
        ctlSvcLocalDir = nev->nev_string;
}

init_ctx_t
ctl_svc_init(void)
{
    REF_TREE_INIT(&ctlSvcNodeTree, ctl_svc_node_construct,
                  ctl_svc_node_destruct);

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

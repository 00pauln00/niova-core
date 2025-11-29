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

#include "niova/alloc.h"
#include "niova/config_token.h"
#include "niova/ctl_svc.h"
#include "niova/env.h"
#include "niova/file_util.h"
#include "niova/init.h"
#include "niova/log.h"
#include "niova/tcp_mgr.h"
#include "niova/ref_tree_proto.h"
#include "niova/regex_defines.h"
#include "niova/registry.h"
#include "niova/util_thread.h"

#define CTL_SVC_CONF_FILE_MAX_SIZE 1024UL
#define CTL_SVC_NUM_CONF_TOKENS 9

#define CTL_SVC_INVALID_LINE_MARKER " <---\n"
#define CTL_SVC_INVALID_LINE_MARKER_LEN 8

REGISTRY_ENTRY_FILE_GENERATE;

/* UUID and TYPE must be first, otherwise the RAFT node type will break.
 */
enum ctl_svc_node_keys
{
    CTL_SVC_NODE_UUID,              //string
    CTL_SVC_NODE_TYPE,              //string
    CTL_SVC_NODE_HOSTNAME,          //string
    CTL_SVC_NODE_IPADDR,            //string
    CTL_SVC_NODE_PORT,              //string
    CTL_SVC_NODE_CLIENT_PORT,       //unsigned
    CTL_SVC_NODE_NET_SEND_ENABLED, //bool
    CTL_SVC_NODE_NET_RECV_ENABLED, //bool
    CTL_SVC_NODE_STORE,             //string (pathname)
    CTL_SVC_NODE__MAX,
    CTL_SVC_NODE__MIN = 0,
    CTL_SVC_NODE__MAX_RAFT = CTL_SVC_NODE_HOSTNAME,
};

LREG_ROOT_ENTRY_GENERATE(ctl_svc_nodes, LREG_USER_TYPE_CTL_SVC_NODE);

REF_TREE_HEAD(ctl_svc_node_tree, ctl_svc_node);

REF_TREE_GENERATE(ctl_svc_node_tree, ctl_svc_node, csn_rtentry,
                  ctl_svc_node_cmp);

static struct ctl_svc_node_tree ctlSvcNodeTree;
static const char *ctlSvcLocalDir = CTL_SVC_DEFAULT_LOCAL_DIR;

#define CTL_SVR_NODE_TOKEN_HNDLR(name)                           \
    static int                                                   \
    ctl_svc_node_token_hndlr_ ##name (struct ctl_svc_node *,     \
                                      const struct conf_token *, \
                                      const char *, size_t)

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
    [CTL_SVC_NODE_TYPE_NIOSD] = {
        .csnts_file_suffix = "niosd",
        .csnts_file_suffix_len = 5,
        .csnts_type = CTL_SVC_NODE_TYPE_NIOSD,
    },
    [CTL_SVC_NODE_TYPE_RAFT] = {
        .csnts_file_suffix = "raft",
        .csnts_file_suffix_len = 4,
        .csnts_type = CTL_SVC_NODE_TYPE_RAFT,
    },
    [CTL_SVC_NODE_TYPE_RAFT_PEER] = {
        .csnts_file_suffix = "peer",
        .csnts_file_suffix_len = 4,
        .csnts_type = CTL_SVC_NODE_TYPE_RAFT_PEER,
    },
    [CTL_SVC_NODE_TYPE_RAFT_CLIENT] = {
        .csnts_file_suffix = "raft_client",
        .csnts_file_suffix_len = 11,
        .csnts_type = CTL_SVC_NODE_TYPE_RAFT_CLIENT,
    },
    [CTL_SVC_NODE_TYPE_ANY] = {
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
        CT_ID_CLIENT_PORT,
        CT_ID_HOSTNAME,
    },
    [CTL_SVC_NODE_TYPE_RAFT_CLIENT] = {
        CT_ID_RAFT,
        CT_ID_UUID,
        CT_ID_IPADDR,
        CT_ID_CLIENT_PORT,
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
    [CT_ID_CLIENT_PORT] = ctl_svc_node_token_hndlr_PORT,
    [CT_ID_HOSTNAME]    = ctl_svc_node_token_hndlr_HOSTNAME,
    [CT_ID_IPADDR]      = ctl_svc_node_token_hndlr_IPADDR,
    [CT_ID_PEER]        = ctl_svc_node_token_hndlr_PEER,
    [CT_ID_PORT]        = ctl_svc_node_token_hndlr_PORT,
    [CT_ID_RAFT]        = ctl_svc_node_token_hndlr_RAFT,
    [CT_ID_STORE]       = ctl_svc_node_token_hndlr_STORE,
    [CT_ID_UUID]        = ctl_svc_node_token_hndlr_UUID,
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

    //DBG_CTL_SVC_NODE(LL_TRACE, csn, "npeer=%hhu %s", npeers, uuid_str);

    return 0;
}

static enum ctl_svc_node_type
ctl_svc_detect_node_type(const char *input_file_ext)
{
    if (input_file_ext)
    {
        for (enum ctl_svc_node_type j = CTL_SVC_NODE_TYPE_NIOSD;
             j < CTL_SVC_NODE_TYPE_MAX; j++)
            if (!strncmp(ctlSvcNodeTypes[j].csnts_file_suffix, input_file_ext,
                         ctlSvcNodeTypes[j].csnts_file_suffix_len))
                return ctlSvcNodeTypes[j].csnts_type;
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
static int
ctl_svc_parse_input_file_name(struct ctl_svc_node *csn,
                              const char *input_file_name)
{
    if (!csn || !input_file_name)
        return -EINVAL;

    else if (strnlen(input_file_name, NAME_MAX) < UUID_STR_LEN ||
             input_file_name[UUID_STR_LEN - 1] != '.')
        return -EBADMSG;

#if 0
    /* Note that using size of 'NAME_MAX' is just to prevent recent versions
     * of GCC from throwing '-Wformat-truncation' errors.  Otherwise, a char
     * buffer of len UUID_STR_LEN would be fine.
     */
    char tmp_fname[NAME_MAX + 1] = {0};
    strncpy(tmp_fname, input_file_name, NAME_MAX);
#else
    char tmp_fname[UUID_STR_LEN];
    memcpy((void *)tmp_fname, (void *)input_file_name, UUID_STR_LEN);
    tmp_fname[UUID_STR_LEN - 1] = '\0';
#endif

    /* First, parse the UUID to check for validity.
     */
    int rc = uuid_parse(tmp_fname, csn->csn_uuid);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "uuid_parse() returns %d", rc);
        return CTL_SVC_NODE_TYPE_ANY;
    }

    /* Next, check the file extension.
     */
    enum ctl_svc_node_type csn_type =
        ctl_svc_detect_node_type(&input_file_name[UUID_STR_LEN]);

    if (csn_type >= CTL_SVC_NODE_TYPE_ANY)
        return csn_type;

    csn->csn_type = csn_type;

    return csn_type;
}

static int
ctl_svc_node_token_hndlr_HOSTNAME(struct ctl_svc_node *csn,
                                  const struct conf_token *ct,
                                  const char *val_buf, size_t val_buf_sz)
{
    (void)ct;
    if (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT)
        return -EINVAL;

    else if (val_buf_sz >= HOST_NAME_MAX)
        return -ENAMETOOLONG;

    strncpy(csn->csn_peer.csnp_hostname, val_buf, val_buf_sz);

    DBG_CTL_SVC_NODE(LL_DEBUG, csn, "%s", csn->csn_peer.csnp_hostname);

    return 0;
}

/**
 * ctl_svc_node_token_hndlr_PORT - this function services both
 *    CT_ID_CLIENT_PORT and CT_ID_PORT.
 */
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
    if ((port < 0 || port == 0 || port >= 65536))
        return -ERANGE;

    if (ct->ct_id == CT_ID_CLIENT_PORT)
        csn->csn_peer.csnp_client_port = port;
    else
        csn->csn_peer.csnp_port = port;

    DBG_CTL_SVC_NODE(LL_DEBUG, csn, "%s %ld", ct->ct_name, port);

    return 0;
}

static int
ctl_svc_node_token_hndlr_UUID(struct ctl_svc_node *csn,
                              const struct conf_token *ct,
                              const char *val_buf, size_t val_buf_sz)
{
    (void)ct;

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

    else if (csn->csn_type != CTL_SVC_NODE_TYPE_RAFT_PEER &&
             csn->csn_type != CTL_SVC_NODE_TYPE_RAFT_CLIENT)
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
    (void)ct;

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
    (void)ct;

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
    (void)ct;

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

static void
ctl_svc_node_release_internal_members(struct ctl_svc_node *destroy)
{
    if (ctl_svc_node_is_peer(destroy) && destroy->csn_peer.csnp_store)
    {
        niova_free(destroy->csn_peer.csnp_store);
        destroy->csn_peer.csnp_store = NULL;
    }
}

static util_thread_ctx_reg_int_t
ctl_svc_lreg_cb(enum lreg_node_cb_ops op, struct lreg_node *lrn,
                struct lreg_value *lrv)
{
    if (!lrn || (!lrv && (op == LREG_NODE_CB_OP_GET_NODE_INFO ||
                          op == LREG_NODE_CB_OP_READ_VAL ||
                          op == LREG_NODE_CB_OP_WRITE_VAL)))
        return -EINVAL;

    struct ctl_svc_node *csn = OFFSET_CAST(ctl_svc_node, csn_lrn, lrn);

    DBG_CTL_SVC_NODE(LL_DEBUG, csn, "");

    bool tmp_bool;
    int tmp_rc;

    switch (op)
    {
    case LREG_NODE_CB_OP_INSTALL_NODE: /* fall through */
    case LREG_NODE_CB_OP_DESTROY_NODE: /* fall through */
    case LREG_NODE_CB_OP_INSTALL_QUEUED_NODE:
        break; // These may require implementation later..

    case LREG_NODE_CB_OP_GET_NODE_INFO:
        lrv->get.lrv_num_keys_out = ctl_svc_node_is_raft(csn) ?
            CTL_SVC_NODE__MAX_RAFT : CTL_SVC_NODE__MAX;
        strncpy(lrv->lrv_key_string, ctl_svc_node_to_string(csn),
                LREG_VALUE_STRING_MAX);
        break;

    case LREG_NODE_CB_OP_READ_VAL:
        switch (lrv->lrv_value_idx_in)
        {
        case CTL_SVC_NODE_UUID:
            lreg_value_fill_string_uuid(lrv, "uuid", csn->csn_uuid);
            break;
        case CTL_SVC_NODE_TYPE:
            lreg_value_fill_string(lrv, "type",
                                   ctl_svc_node_to_string(csn));
            break;
        case CTL_SVC_NODE_HOSTNAME:
            lreg_value_fill_string(lrv, "hostname",
                                   csn->csn_peer.csnp_hostname);
            break;
        case CTL_SVC_NODE_IPADDR:
            lreg_value_fill_string(lrv, "ip_address",
                                   csn->csn_peer.csnp_ipv4);
            break;
        case CTL_SVC_NODE_PORT:
            lreg_value_fill_unsigned(lrv, "server_port",
                                     csn->csn_peer.csnp_port);
            break;
        case CTL_SVC_NODE_CLIENT_PORT:
            lreg_value_fill_unsigned(lrv, "client_port",
                                     csn->csn_peer.csnp_client_port);
            break;
        case CTL_SVC_NODE_NET_SEND_ENABLED:
            lreg_value_fill_bool(
                lrv, "net_send_enabled",
                net_ctl_can_send(&csn->csn_peer.csnp_net_ctl));
            break;
        case CTL_SVC_NODE_NET_RECV_ENABLED:
            lreg_value_fill_bool(
                lrv, "net_recv_enabled",
                net_ctl_can_recv(&csn->csn_peer.csnp_net_ctl));
            break;
        case CTL_SVC_NODE_STORE:
            lreg_value_fill_string(
                lrv, "data_store",
                (csn->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER ?
                 csn->csn_peer.csnp_store : "none"));
            break;
        default:
            return -EOPNOTSUPP;
        }
        break;

    case LREG_NODE_CB_OP_WRITE_VAL:
        if (lrv->put.lrv_value_type_in != LREG_VAL_TYPE_STRING)
            return -EINVAL;

        tmp_rc = niova_string_to_bool(LREG_VALUE_TO_IN_STR(lrv), &tmp_bool);
        if (tmp_rc)
            return tmp_rc;

        switch (lrv->lrv_value_idx_in)
        {
        case CTL_SVC_NODE_NET_SEND_ENABLED:
            net_ctl_send_recv(&csn->csn_peer.csnp_net_ctl,
                              (tmp_bool ?
                               NET_CTL_ENABLE_SEND : NET_CTL_DISABLE_SEND));
            break;
        case CTL_SVC_NODE_NET_RECV_ENABLED:
            net_ctl_send_recv(&csn->csn_peer.csnp_net_ctl,
                              (tmp_bool ?
                               NET_CTL_ENABLE_RECV : NET_CTL_DISABLE_RECV));
            break;
        default:
            return -EOPNOTSUPP;
        };

        break;

    default:
        break;
    }

    return 0;
}

static int
ctl_svc_node_tree_add(const struct ctl_svc_node *csn_from_caller_stack,
                      int *rt_ret)
{
    NIOVA_ASSERT(csn_from_caller_stack && rt_ret);

    struct ctl_svc_node *new_csn =
        RT_GET_ADD(ctl_svc_node_tree, &ctlSvcNodeTree, csn_from_caller_stack,
                   rt_ret);

    if (!new_csn)
        return -ENOMEM;

    DBG_CTL_SVC_NODE(LL_NOTIFY, new_csn, "");

    RT_PUT(ctl_svc_node_tree, &ctlSvcNodeTree, new_csn);

    return 0;
}

/**
 * ctl_svc_read_and_prep_conf_file - this is a helper function for
 *    ctl_svc_process_conf_file which reads in the specified input file and
 *    performs post-read tasks such as detecting trailing null characters
 *    and ensuring that the file ends with a newline.
 */
static ssize_t
ctl_svc_read_and_prep_conf_file(int ctl_svc_dir_fd, const char *input_file,
                                char *file_buf, const size_t file_buf_sz)
{
    // Read the file contents into 'file_buf'.
    ssize_t read_rc =
        file_util_open_and_read(ctl_svc_dir_fd, input_file, file_buf,
                                file_buf_sz, NULL);
    if (read_rc < 0)
    {
        LOG_MSG(LL_NOTIFY, "file_util_open_and_read(`%s'): %s",
                input_file, strerror(-read_rc));
        return read_rc;
    }

    const ssize_t null_cnt_end_of_buf =
        niova_count_nulls_from_end_of_buffer(file_buf, read_rc);

    NIOVA_ASSERT(read_rc >= null_cnt_end_of_buf);

    if (null_cnt_end_of_buf)
    {
        read_rc -= null_cnt_end_of_buf;

        LOG_MSG(LL_NOTIFY, "null_cnt_end_of_buf=%zu new-read_rc=%zd",
                null_cnt_end_of_buf, read_rc);
    }

    // Workaround for files which do not have a newline at the end.
    if (read_rc && file_buf[read_rc - 1] != '\n' &&
        read_rc < (ssize_t)file_buf_sz)
    {
        file_buf[read_rc] = '\n';
        read_rc += 1;

        LOG_MSG(LL_NOTIFY,
                "setting newline @pos=%zd"
                "\n-----------------------------------------------------\n"
                "%s-----------------------------------------------------",
                read_rc, file_buf);
    }

    return read_rc;
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
 * @last_line_number:  Used to track parsing errors.
 */
static int
ctl_svc_process_conf_file(int ctl_svc_dir_fd, const char *input_file,
                          char *file_buf, size_t file_buf_sz, char *value_buf,
                          size_t value_buf_sz,
                          unsigned int *last_line_number)
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
    const ssize_t read_rc =
        ctl_svc_read_and_prep_conf_file(ctl_svc_dir_fd, input_file, file_buf,
                                        file_buf_sz);

    if (read_rc < 0)
    {
        rc = read_rc;
        return rc;
    }

    NIOVA_ASSERT(read_rc <= (ssize_t)file_buf_sz);

    conf_token_set_parser_init(&ctsp, file_buf, read_rc, value_buf,
                               value_buf_sz, ctl_svc_ctsp_cb, &csn);

    rc = conf_token_set_parse(&ctsp);
    if (rc)
    {
        LOG_MSG(LL_NOTIFY, "conf_token_set_parse(`%s'): %s",
                input_file, strerror(-rc));

        if (last_line_number)
            *last_line_number = ctsp.ctsp_parse_line_num;

        return rc;
    }

    int rt_ret = 0;
    rc = ctl_svc_node_tree_add(&csn, &rt_ret);

    /* RT_GET_ADD() may place 2 different errors into 'rt_ret' while still
     * returning a valid entry.  These are EEXIST and EALREADY.
     * Both mean that an entry of the same id was already in the tree.
     * The difference is that EALREADY is returned when the object
     * destructor was already run.
     * Why does that matter here?  Our stack-allocated 'csn' may have heap
     * memory allocated to some of it members and these pointers were
     * inherited by a newly contstructed object which was immediately
     * released by RT_GET_ADD() due to a conflict.  It's important that
     * we don't double free the pointers within the stack csn structure and
     * it's also important that we do release these allocations if the
     * destructor was not run.
     */
    if (rt_ret == -EEXIST)
        ctl_svc_node_release_internal_members(&csn);

    return rc;
}

static init_ctx_t
ctl_svc_init_scan_entries_dump_invalid_file(const char *file_name,
                                            const char *file_buf,
                                            const size_t len,
                                            const unsigned int invalid_line,
                                            const int error)
{
    if (!file_name || !file_buf || !len)
        return;

    bool dump_whole_file = true;
    size_t my_len = len + CTL_SVC_INVALID_LINE_MARKER_LEN;

    char *stage_buffer = niova_calloc_can_fail(1UL, my_len);

    if (stage_buffer)
    {
        bool inside_comment = false;
        unsigned int current_line = 1;
        size_t idx = 0;
        size_t valid_chars_on_line = 0;

        for (size_t i = 0; i < len; i++)
        {
            if (file_buf[i] == '\n')
            {
                if (current_line++ == invalid_line)
                {
                    dump_whole_file = false;
                    snprintf(&stage_buffer[idx],
                             CTL_SVC_INVALID_LINE_MARKER_LEN + (len - i),
                             CTL_SVC_INVALID_LINE_MARKER "%s",
                             &file_buf[i + 1]);
                    break;
                }
                stage_buffer[idx++] = file_buf[i];
                valid_chars_on_line = 0;

                if (inside_comment)
                    inside_comment = false;
            }
            else
            {
                stage_buffer[idx++] = file_buf[i];
                valid_chars_on_line++;
            }
        }
    }

    LOG_MSG(
        LL_WARN,
        "Processing failed for ctl-svc-file %s (line=%u): %s"
        "\n-------------------------------------------------------------\n"
        "%s-------------------------------------------------------------",
        file_name, invalid_line, strerror(-error),
        dump_whole_file ? file_buf : stage_buffer);

    if (stage_buffer)
        niova_free(stage_buffer);
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

    char *file_buf = niova_malloc_can_fail(CTL_SVC_CONF_FILE_MAX_SIZE);
    if (!file_buf)
    {
        rc = -ENOMEM;
        goto out_close_dir;
    }

    char *value_buf = niova_malloc_can_fail(CTL_SVC_CONF_FILE_MAX_SIZE);
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

        unsigned int line_number = 0;

        memset(file_buf, 0, CTL_SVC_CONF_FILE_MAX_SIZE);
        memset(value_buf, 0, CTL_SVC_CONF_FILE_MAX_SIZE);

        int rc = ctl_svc_process_conf_file(ctl_svc_dir_fd, dent->d_name,
                                           file_buf,
                                           CTL_SVC_CONF_FILE_MAX_SIZE,
                                           value_buf,
                                           CTL_SVC_CONF_FILE_MAX_SIZE,
                                           &line_number);
        if (rc)
        {
            if (line_number)
                ctl_svc_init_scan_entries_dump_invalid_file(
                    dent->d_name, file_buf, CTL_SVC_CONF_FILE_MAX_SIZE,
                    line_number, rc);
            else
                LOG_MSG(LL_WARN,
                        "Processing failed for ctl-svc-file %s: %s",
                        dent->d_name, strerror(-rc));
        }
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

int
ctl_svc_scan(void)
{
    int rc = ctl_svc_init_scan_entries();

    if (rc)
        SIMPLE_LOG_MSG(LL_NOTIFY, "ctl_svc_init_scan_entries() again: %s",
                       strerror(-rc));
    return rc;
}

static struct ctl_svc_node *
ctl_svc_node_construct(const struct ctl_svc_node *in, void *arg)
{
    (void)arg;

    if (!in)
        return NULL;

    struct ctl_svc_node *csn = niova_calloc((size_t)1,
                                            sizeof(struct ctl_svc_node));
    if (!csn)
        return NULL;

    *csn = *in;
    if (ctl_svc_node_is_peer(csn))
        csn->csn_peer.csnp_net_data.tmc_status = TMCS_NEEDS_SETUP;

    lreg_node_init(&csn->csn_lrn, LREG_USER_TYPE_CTL_SVC_NODE,
                   ctl_svc_lreg_cb, NULL, LREG_INIT_OPT_NONE);

    int rc = lreg_node_install(&csn->csn_lrn,
                               LREG_ROOT_ENTRY_PTR(ctl_svc_nodes));

    DBG_CTL_SVC_NODE((rc ? LL_FATAL : LL_DEBUG), csn, "%s", strerror(-rc));

    return csn;
}

static int
ctl_svc_node_destruct(struct ctl_svc_node *destroy, void *arg)
{
    (void)arg;

    if (!destroy)
        return -EINVAL;

    DBG_CTL_SVC_NODE(LL_DEBUG, destroy, "");

    ctl_svc_node_release_internal_members(destroy);

    niova_free(destroy);

    return 0;
}

int
ctl_svc_node_init(struct ctl_svc_node *csn,
                         const uuid_t raft_uuid,
                         const uuid_t node_uuid,
                         enum ctl_svc_node_type node_type)
{
    if (!csn)
        return -EINVAL;

    csn->csn_type = node_type;
    uuid_copy(csn->csn_peer.csnp_raft_info.csnrp_member.csrm_peer, raft_uuid);
    uuid_copy(csn->csn_uuid, node_uuid);

    return 0;
}

int
ctl_svc_node_add(struct ctl_svc_node *csn,
                 struct ctl_svc_node **ret_csn)
{

    if (!csn)
        return -EINVAL;

    // Add the node to the tree
    int rc, rt_ret = 0;
    rc = ctl_svc_node_tree_add(csn, &rt_ret);

    if (rc)
    {
        SIMPLE_LOG_MSG(LL_WARN, "ctl_svc_node_tree_add failed: %d, rt_ret: %d",
                       rc, rt_ret);
        return rc;
    }

    //XXX return ret_csn from ctl_svc_node_tree_add itself
    return ctl_svc_node_lookup(csn->csn_uuid, ret_csn);
}

int
ctl_svc_node_lookup(const uuid_t lookup_uuid, struct ctl_svc_node **ret_csn)
{
    uuid_t my_uuid;
    uuid_copy(my_uuid, lookup_uuid);

    struct ctl_svc_node *csn =
        RT_LOOKUP(ctl_svc_node_tree, &ctlSvcNodeTree,
                  (const struct ctl_svc_node *)&my_uuid);

    if (!csn)
        return -ENOENT;
    else
        *ret_csn = csn;

    return 0;
}

int
ctl_svc_node_lookup_by_string(const char *uuid_str,
                              struct ctl_svc_node **ret_csn)
{
    if (!uuid_str || !ret_csn)
        return -EINVAL;

    uuid_t lookup_uuid;
    if (uuid_parse(uuid_str, lookup_uuid))
        return -EBADMSG;

    return ctl_svc_node_lookup(lookup_uuid, ret_csn);
}

void
ctl_svc_node_get(struct ctl_svc_node *csn)
{
    DBG_CTL_SVC_NODE(LL_TRACE, csn, "");
    REF_TREE_REF_GET_ELEM(&ctlSvcNodeTree, csn, csn_rtentry);
}

void
ctl_svc_node_put(struct ctl_svc_node *csn)
{
    DBG_CTL_SVC_NODE(LL_TRACE, csn, "");
    RT_PUT(ctl_svc_node_tree, &ctlSvcNodeTree, csn);
}

void
ctl_svc_set_local_dir(const struct niova_env_var *nev)
{
    if (nev && nev->nev_string)
        ctlSvcLocalDir = nev->nev_string;
}

const char *
ctl_svc_get_local_dir(void)
{
    return ctlSvcLocalDir;
}

static destroy_ctx_t
ctl_svc_nodes_release(void)
{
    struct ctl_svc_node *csn =
        REF_TREE_MIN(ctl_svc_node_tree, &ctlSvcNodeTree, ctl_svc_node,
                     csn_rtentry);

    if (csn)
    {
        for (; csn != NULL;
             csn = REF_TREE_MIN(ctl_svc_node_tree, &ctlSvcNodeTree,
                                ctl_svc_node, csn_rtentry))
            for (int i = 0; i < REF_TREE_INITIAL_REF_CNT(&ctlSvcNodeTree); i++)
                RT_PUT(ctl_svc_node_tree, &ctlSvcNodeTree, csn);

        csn = REF_TREE_MIN(ctl_svc_node_tree, &ctlSvcNodeTree, ctl_svc_node,
                           csn_rtentry);
        if (csn)
            DBG_CTL_SVC_NODE(LL_WARN, csn, "ctl_svc_node(s) still exist");
    }
}

static init_ctx_t NIOVA_CONSTRUCTOR(CTL_SVC_CTOR_PRIORITY)
ctl_svc_init(void)
{
    LREG_ROOT_ENTRY_INSTALL(ctl_svc_nodes);

    /* Use an initial ref count of "2" so that entries don't require additional
     * dependencies to remain in the tree after creation.
     */
    REF_TREE_INIT_ALT_REF(&ctlSvcNodeTree, ctl_svc_node_construct,
                          ctl_svc_node_destruct, 2, NULL);

    int rc = ctl_svc_init_scan_entries();

    if (rc)
        SIMPLE_LOG_MSG(LL_NOTIFY, "ctl_svc_init_scan_entries(): %s",
                       strerror(-rc));
}

static destroy_ctx_t NIOVA_DESTRUCTOR(CTL_SVC_CTOR_PRIORITY)
ctl_svc_destroy(void)
{
    FUNC_ENTRY(LL_NOTIFY);

    ctl_svc_nodes_release();
}

void
ctl_svc_nodes_apply(enum ctl_svc_node_type type,
                    int (*cb)(struct ctl_svc_node *, void *), void *data)
{
    struct ctl_svc_node *csn;

    niova_mutex_lock(&ctlSvcNodeTree.mutex);
    RT_FOREACH_LOCKED(csn, ctl_svc_node_tree, &ctlSvcNodeTree)
    {
        if (csn->csn_type != type)
            continue;

        if (cb(csn, data))
            break;
    }
    niova_mutex_unlock(&ctlSvcNodeTree.mutex);
}

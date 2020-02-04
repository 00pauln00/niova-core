/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _NIOVA_CTL_SVC_H_
#define _NIOVA_CTL_SVC_H_ 1

#include <limits.h>
#include <uuid/uuid.h>

#include "common.h"
#include "init.h"
#include "ctor.h"
#include "env.h"
#include "ref_tree_proto.h"

#define CTL_SVC_DEFAULT_LOCAL_DIR "/etc/niova/ctl-svc/local"
#define CTL_SVC_MAX_RAFT_PEERS 11

enum ctl_svc_node_type
{
    CTL_SVC_NODE_TYPE_NIOSD,
    CTL_SVC_NODE_TYPE_RAFT,
    CTL_SVC_NODE_TYPE_RAFT_PEER,
    CTL_SVC_NODE_TYPE_ANY,
    CTL_SVC_NODE_TYPE_MAX,
};

struct ctl_svc_node_niosd
{
    uint64_t csnn__pad;
};

struct ctl_svc_raft_member
{
    uuid_t csrm_peer;
};

struct ctl_svc_node_raft_peer
{
    uint16_t                   csnrp_is_leader:1;
    uint16_t                   csnrp__pad[3];
    struct ctl_svc_raft_member csnrp_member;
};

/**
 * NOTE:  the UUID for the peer is stored in the ctl_svc_node structure.
 */
struct ctl_svc_node_peer
{
    char         csnp_hostname[HOST_NAME_MAX];
    char         csnp_ipv4[IPV4_STRLEN];
    char        *csnp_store;
    uint16_t     csnp_port;
    uint16_t     csnp_client_port;
    uint16_t     csnp__pad[2];
    union {
        struct ctl_svc_node_raft_peer csnp_raft_info;
        struct ctl_svc_node_niosd     csnp_niosd_info;
    };
};

struct ctl_svc_node_raft
{
    raft_peer_t                csnr_num_members;
    struct ctl_svc_raft_member csnr_members[CTL_SVC_MAX_RAFT_PEERS];
};

struct ctl_svc_node
{
    uuid_t                       csn_uuid;
    REF_TREE_ENTRY(ctl_svc_node) csn_rtentry;
    enum ctl_svc_node_type       csn_type;
    union
    {
        struct ctl_svc_node_peer csn_peer;
        struct ctl_svc_node_raft csn_raft;
    };
};

static inline bool
ctl_svc_node_is_peer(const struct ctl_svc_node *csn)
{
    return (csn &&
            (csn->csn_type == CTL_SVC_NODE_TYPE_NIOSD ||
             csn->csn_type == CTL_SVC_NODE_TYPE_RAFT_PEER)) ? true : false;
}

static inline bool
ctl_svc_node_is_raft(const struct ctl_svc_node *csn)
{
    return (csn && csn->csn_type == CTL_SVC_NODE_TYPE_RAFT) ? true : false;
}

static inline const char *
ctl_svc_node_peer_2_store(const struct ctl_svc_node *csn)
{
    return (csn && ctl_svc_node_is_peer(csn)) ?
        csn->csn_peer.csnp_store : NULL;
}

static inline const struct ctl_svc_node_raft *
ctl_svc_node_raft_2_raft(const struct ctl_svc_node *csn)
{
    return (csn && ctl_svc_node_is_raft(csn)) ? &csn->csn_raft : NULL;
}

static inline raft_peer_t
ctl_svc_node_raft_2_num_members(const struct ctl_svc_node *csn)
{
    return (csn && ctl_svc_node_is_raft(csn)) ?
        csn->csn_raft.csnr_num_members : RAFT_PEER_ANY;
}

static inline const uint16_t
ctl_svc_node_peer_2_port(const struct ctl_svc_node *csn)
{
    return (csn && ctl_svc_node_is_peer(csn)) ?
        csn->csn_peer.csnp_port : 0;
}

static inline const uint16_t
ctl_svc_node_peer_2_client_port(const struct ctl_svc_node *csn)
{
    return (csn && ctl_svc_node_is_peer(csn)) ?
        csn->csn_peer.csnp_client_port : 0;
}

static inline const char *
ctl_svc_node_peer_2_ipaddr(const struct ctl_svc_node *csn)
{
    return (csn && ctl_svc_node_is_peer(csn)) ?
        csn->csn_peer.csnp_ipv4 : NULL;
}

static inline int
ctl_svc_node_cmp(const struct ctl_svc_node *a, const struct ctl_svc_node *b)
{
    return uuid_compare(a->csn_uuid, b->csn_uuid);
}

/**
 * ctl_svc_node_check_string - compares the provided UUID string with the
 *    binary UUID stored in the node.
 * Returns:  0 if the check passed.
 */
static inline int
ctl_svc_node_check_string(const struct ctl_svc_node *a, const char *b_str)
{
    uuid_t b;

    if (uuid_parse(b_str, b))
        return -EINVAL;

    return uuid_compare(a->csn_uuid, b) ? 1 : 0;
}

static inline char
ctl_svc_node_type(const struct ctl_svc_node *csn)
{
    if (csn)
    {
        switch (csn->csn_type)
        {
        case CTL_SVC_NODE_TYPE_NIOSD:
            return 'N';
        case CTL_SVC_NODE_TYPE_RAFT:
            return 'R';
        case CTL_SVC_NODE_TYPE_RAFT_PEER:
            return 'r';
        default:
            break;
        }
    }
    return 'u';
}

#define DBG_CTL_SVC_NODE(log_level, csn, fmt, ...)                      \
{                                                                       \
    char __uuid_str[UUID_STR_LEN];                                      \
    uuid_unparse((csn)->csn_uuid, __uuid_str);                          \
    LOG_MSG(log_level, "csn@%p %c %s ref=%d store=%s "fmt,              \
            (csn), ctl_svc_node_type((csn)), __uuid_str,                \
            (csn)->csn_rtentry.rbe_ref_cnt,                             \
            (ctl_svc_node_is_peer((csn)) ?                              \
             (csn)->csn_peer.csnp_store : NULL),                        \
            ##__VA_ARGS__);                                             \
}

struct niova_env_var;

void
ctl_svc_set_local_dir(const struct niova_env_var *nev);

int
ctl_svc_node_lookup(const uuid_t lookup_uuid, struct ctl_svc_node **ret_csn);

int
ctl_svc_node_lookup_by_string(const char *uuid_str,
                              struct ctl_svc_node **ret_csn);

void
ctl_svc_node_put(struct ctl_svc_node *csn);

init_ctx_t
ctl_svc_init(void)
    __attribute__ ((constructor (CTL_SVC_CTOR_PRIORITY)));

destroy_ctx_t
ctl_svc_destroy(void)
    __attribute__ ((destructor (CTL_SVC_CTOR_PRIORITY)));

#endif

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


struct ctl_svc_node
{
    uuid_t                       csn_uuid;
    char                         csn_hostname[HOST_NAME_MAX];
    char                         csn_ipv4[IPV4_STRLEN];
    unsigned int                 csn_port_num;
    REF_TREE_ENTRY(ctl_svc_node) csn_rtentry;
};

struct niova_env_var;

void
ctl_svc_set_local_dir(const struct niova_env_var *nev);

init_ctx_t
ctl_svc_init(void)
    __attribute__ ((constructor (CTL_SVC_CTOR_PRIORITY)));

destroy_ctx_t
ctl_svc_destroy(void)
    __attribute__ ((destructor (CTL_SVC_CTOR_PRIORITY)));

#endif

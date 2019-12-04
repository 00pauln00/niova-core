/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _NIOSD_IO_STATS
#define _NIOSD_IO_STATS 1

#include "ctor.h"

struct niosd_device;
struct niosd_io_ctx;
struct niosd_io_request;

void
nioctx_stats_ingest_from_niorq(struct niosd_io_request *niorq);

void
nioctx_stats_dump(const struct niosd_io_ctx *nioctx);

void
nioctx_stats_init(struct niosd_io_ctx *nioctx);

void
niosd_io_stats_init(struct niosd_device *ndev);

init_ctx_t
niosd_io_stats_subsys_init(void)
     __attribute__ ((constructor (NIOSD_IO_CTX_STATS_CTOR_PRIORITY)));

destroy_ctx_t
niosd_io_stats_subsys_destroy(void)
    __attribute__ ((destructor (NIOSD_IO_CTX_STATS_CTOR_PRIORITY)));

#endif

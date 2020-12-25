/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#ifndef _CTL_INTERFACE_
#define _CTL_INTERFACE_ 1

#include "common.h"
#include "ctor.h"
#include "registry.h"

struct ctli_cmd_handle
{
    enum lreg_user_types ctlih_reg_user_type;
    int                  ctlih_output_dirfd;
    int                  ctlih_input_dirfd;
    const char          *ctlih_input_file_name;
};

const char *
lctli_get_inotify_path(void);

int
lctli_init_subdir_rescan(enum lreg_user_types reg_type);

#endif //_CTL_INTERFACE_

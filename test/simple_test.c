/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */

#include "common.h"
#include "metablock.h"

#include <stdio.h>

int
main(void)
{
    struct mb_header_persistent hp;

    fprintf(stderr, "sizeof(struct mb_vblk_entry) = %zd\n",
            sizeof(struct mb_vblk_entry));

    fprintf(stdout, "sizeof(struct mb_header_persistent) = %zd\n",
            sizeof(hp));

    fprintf(stdout, "simple test OK\n");
    return 0;
}

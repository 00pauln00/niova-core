/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#ifndef _CTOR_H
#define _CTOR_H 1

/* -- constructor_priorities --
 * Startup (and shutdown) order for NIOVA subsystems.
 */
enum constructor_priorities {
    INIT_START_CTOR_PRIORITY = 101,
    ENV_VAR_SUBSYS_CTOR_PRIORITY,
    WATCHDOG_SUBSYS_CTOR_PRIORITY,
    LREG_SUBSYS_CTOR_PRIORITY,
    LOG_SUBSYS_CTOR_PRIORITY,
    LCTLI_SUBSYS_PRIORITY,
    VBLKDEV_HANDLE_CTOR_PRIORITY,
    INIT_COMPLETE_CTOR_PRIORITY,
};

#endif

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _REGEX_DEFINES
#define _REGEX_DEFINES 1

#define UUID_REGEX_BASE                                                 \
    "[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}"

#define UUID_REGEX_CTL_SVC_FILE_NAME                                    \
    "^"UUID_REGEX_BASE"\\.raft\\|peer$"

#define UUID_REGEX                              \
    "^"UUID_REGEX_BASE"$"

#define IPADDR_REGEX                                                    \
    "^\\(\\([0-9]\\|[1-9][0-9]\\|1[0-9]\\{2\\}\\|2[0-4][0-9]\\|25[0-5]\\)\\.\\)\\{3\\}\\([0-9]\\|[1-9][0-9]\\|1[0-9]\\{2\\}\\|2[0-4][0-9]\\|25[0-5]\\)$"

#define HOSTNAME_REGEX                                                  \
    "^\\(\\([a-zA-Z0-9]\\|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9]\\)\\.\\)*\\([A-Za-z0-9]\\|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]\\)$"

#define PORT_REGEX_MAX_LEN 5
#define PORT_REGEX "^[0-9]\\{1,5\\}$"

#define CTL_SVC_REGEX_STR UUID_REGEX_CTL_SVC_FILE_NAME

#endif

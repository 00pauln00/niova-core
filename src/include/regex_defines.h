/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#ifndef _REGEX_DEFINES
#define _REGEX_DEFINES 1

#define UUID_REGEX_BASE \
    "[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}"

#define UUID_REGEX_CTL_SVC_FILE_NAME \
    "^"UUID_REGEX_BASE"\\.raft$\\|peer$\\|raft_client$"

#define UUID_REGEX \
    "^"UUID_REGEX_BASE"$"

// Match '-u UUID' on the process's command line
#define UUID_REGEX_PROC_CMDLINE \
    "-u[ \t]\\+"UUID_REGEX_BASE

#define IPADDR_REGEX \
    "^\\(\\([0-9]\\|[1-9][0-9]\\|1[0-9]\\{2\\}\\|2[0-4][0-9]\\|25[0-5]\\)\\.\\)\\{3\\}\\([0-9]\\|[1-9][0-9]\\|1[0-9]\\{2\\}\\|2[0-4][0-9]\\|25[0-5]\\)$"

#define HOSTNAME_REGEX \
    "^\\(\\([a-zA-Z0-9]\\|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9]\\)\\.\\)*\\([A-Za-z0-9]\\|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]\\)$"

#define PORT_REGEX_MAX_LEN 5
#define PORT_REGEX "^[0-9]\\{1,5\\}$"

#define CTL_SVC_REGEX_STR UUID_REGEX_CTL_SVC_FILE_NAME

#define RNCUI_V0_REGEX_BASE \
    UUID_REGEX_BASE"\\(:\\([0-9a-f]\\{1\\}\\|[1-9a-f][0-9a-f]\\+\\)\\)\\{4\\}"

#define POS_INT "[1-9]\\{1\\}\\|[1-9][0-9]\\+"
#define ZERO_or_POS_INT "[0-9]\\{1\\}\\|[1-9][0-9]\\+"

#define PMDB_TEST_CLIENT_APPLY_CMD_REGEX \
    "^"RNCUI_V0_REGEX_BASE"\\(\\.lookup\\|\\.read\\|\\.write:\\("ZERO_or_POS_INT"\\)\\)\\($\\|.\\("POS_INT"\\)$\\)"

#define COMMA_DELIMITED_UNSIGNED_INTEGER \
    "^\\([0-9]\\|[1-9][0-9]\\{1,2\\}\\)\\(,[0-9]\\{3\\}\\)*$"

#endif

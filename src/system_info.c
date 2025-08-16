/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2019
 */

#include <sys/types.h>
#include <unistd.h>
#include <sys/utsname.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <uuid/uuid.h>
#include <regex.h>
#include <limits.h>

#include "common.h"

#include "ctl_interface.h"
#include "env.h"
#include "init.h"
#include "file_util.h"
#include "log.h"
#include "regex_defines.h"
#include "registry.h"
#include "system_info.h"
#include "util_thread.h"

REGISTRY_ENTRY_FILE_GENERATE;

enum system_info_keys
{
    SYS_INFO_KEY_CTIME,
    SYS_INFO_KEY_STARTUP_CTIME,
    SYS_INFO_KEY_PID,
    SYS_INFO_KEY_UUID,
    SYS_INFO_KEY_CTL_INTERFACE_PATH,
    SYS_INFO_KEY_PROCESS_CMDLINE,
    SYS_INFO_KEY_HOSTNAME,
    SYS_INFO_KEY_UTS_SYSNAME,
    SYS_INFO_KEY_UTS_RELEASE,
    SYS_INFO_KEY_UTS_VERSION,
    SYS_INFO_KEY_UTS_MACHINE_HW,
    SYS_INFO_KEY_RUSAGE_USER_CPU,
    SYS_INFO_KEY_RUSAGE_SYS_CPU,
    SYS_INFO_KEY_RUSAGE_MAX_RSS,
    SYS_INFO_KEY_RUSAGE_MIN_FAULT,
    SYS_INFO_KEY_RUSAGE_MAJ_FAULT,
    SYS_INFO_KEY_RUSAGE_IN_BLOCK,
    SYS_INFO_KEY_RUSAGE_OUT_BLOCK,
    SYS_INFO_KEY_RUSAGE_VOL_CTXSW,
    SYS_INFO_KEY_RUSAGE_INVOL_CTXSW,
    SYS_INFO_KEY__MAX,
    SYS_INFO_KEY__MIN = SYS_INFO_KEY_CTIME,
};

#define SYS_INFO_PROCESS_CMDLINE_LEN 4096

static char systemInfoProcessCmdLine[SYS_INFO_PROCESS_CMDLINE_LEN];
static uuid_t systemInfoUuid;
static struct timespec systemInfoStartTime;

void
system_info_get_uuid(uuid_t sys_info_uuid_copy)
{
    uuid_copy(sys_info_uuid_copy, systemInfoUuid);
}

bool
system_info_uuid_is_present(void)
{
    return uuid_is_null(systemInfoUuid) ? false : true;
}

static util_thread_ctx_reg_int_t
system_info_multi_facet_cb(enum lreg_node_cb_ops op, struct lreg_value *lv,
                           void *arg);

LREG_ROOT_ENTRY_GENERATE_OBJECT(system_info, LREG_USER_TYPE_SYS_INFO,
                                SYS_INFO_KEY__MAX, system_info_multi_facet_cb,
                                NULL, LREG_INIT_OPT_NONE);

#define RUSAGE_CACHE_TIME_MS 10

static util_thread_ctx_reg_int_t
system_info_get(struct rusage *r, struct utsname *u)
{
    if (!u || !r)
        return -EINVAL;

    int rc = getrusage(RUSAGE_SELF, r);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_WARN, "getrusage(): %s", strerror(-rc));

        return -rc;
    }

    rc = uname(u);
    if (rc)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_WARN, "uname(): %s", strerror(-rc));

        return -rc;
    }

    return 0;
}

static util_thread_ctx_reg_int_t
system_info_multi_facet_cb(enum lreg_node_cb_ops op, struct lreg_value *lv,
                           void *arg)
{
    if (!lv || arg /* arg is not used here */)
        return -EINVAL;
    else if (lv->lrv_value_idx_in >= SYS_INFO_KEY__MAX)
        return -ERANGE;
    else if (op == LREG_NODE_CB_OP_WRITE_VAL)
        return -EPERM;
    else if (op != LREG_NODE_CB_OP_READ_VAL)
        return -EOPNOTSUPP;

    static struct timespec time_of_previous_call;
    static struct rusage rusage_current;
    static struct utsname uts_current;

    struct timespec now;

    niova_realtime_coarse_clock(&now);

    if ((timespec_2_msec(&now) - timespec_2_msec(&time_of_previous_call)) >
        RUSAGE_CACHE_TIME_MS)
    {
        int rc = system_info_get(&rusage_current, &uts_current);
        if (rc)
            return rc;
    }

    switch (lv->lrv_value_idx_in)
    {
    case SYS_INFO_KEY_CTIME:
        lreg_value_fill_string_time(lv, "current_time", now.tv_sec);
        break;
    case SYS_INFO_KEY_STARTUP_CTIME:
        lreg_value_fill_string_time(lv, "start_time",
                                    systemInfoStartTime.tv_sec);
        break;
    case SYS_INFO_KEY_UUID:
        lreg_value_fill_string_uuid(lv, "uuid", systemInfoUuid);
        break;
    case SYS_INFO_KEY_CTL_INTERFACE_PATH:
        lreg_value_fill_string(lv, "ctl_interface_path",
                               lctli_get_inotify_path());
        break;
    case SYS_INFO_KEY_PID:
        lreg_value_fill_unsigned(lv, "pid", getpid());
        break;
    case SYS_INFO_KEY_PROCESS_CMDLINE:
        lreg_value_fill_string(lv, "command_line", systemInfoProcessCmdLine);
        break;
    case SYS_INFO_KEY_HOSTNAME:
        lreg_value_fill_string(lv, "uts.nodename", uts_current.nodename);
        break;
    case SYS_INFO_KEY_UTS_SYSNAME:
        lreg_value_fill_string(lv, "uts.sysname", uts_current.sysname);
        break;
    case SYS_INFO_KEY_UTS_RELEASE:
        lreg_value_fill_string(lv, "uts.release", uts_current.release);
        break;
    case SYS_INFO_KEY_UTS_VERSION:
        lreg_value_fill_string(lv, "uts.version", uts_current.version);
        break;
    case SYS_INFO_KEY_UTS_MACHINE_HW:
        lreg_value_fill_string(lv, "uts.machine", uts_current.machine);
        break;
    case SYS_INFO_KEY_RUSAGE_USER_CPU:
        lreg_value_fill_float(lv, "rusage.user_cpu_time_used",
                              timeval_2_float(&rusage_current.ru_utime));
        break;
    case SYS_INFO_KEY_RUSAGE_SYS_CPU:
        lreg_value_fill_float(lv, "rusage.system_cpu_time_used",
                              timeval_2_float(&rusage_current.ru_stime));
        break;
    case SYS_INFO_KEY_RUSAGE_MAX_RSS:
        lreg_value_fill_signed(lv, "rusage.max_rss",
                               rusage_current.ru_maxrss);
        break;
    case SYS_INFO_KEY_RUSAGE_MIN_FAULT:
        lreg_value_fill_signed(lv, "rusage.min_fault",
                               rusage_current.ru_minflt);
        break;
    case SYS_INFO_KEY_RUSAGE_MAJ_FAULT:
        lreg_value_fill_signed(lv, "rusage.maj_fault",
                               rusage_current.ru_majflt);
        break;
    case SYS_INFO_KEY_RUSAGE_IN_BLOCK:
        lreg_value_fill_signed(lv, "rusage.in_block",
                               rusage_current.ru_inblock);
        break;
    case SYS_INFO_KEY_RUSAGE_OUT_BLOCK:
        lreg_value_fill_signed(lv, "rusage.out_block",
                               rusage_current.ru_oublock);
        break;
    case SYS_INFO_KEY_RUSAGE_VOL_CTXSW:
        lreg_value_fill_signed(lv, "rusage.vol_ctsw",
                               rusage_current.ru_nvcsw);
        break;
    case SYS_INFO_KEY_RUSAGE_INVOL_CTXSW:
        lreg_value_fill_signed(lv, "rusage.invol_ctsw",
                               rusage_current.ru_nivcsw);
        break;
    default:
        break;
    }

    time_of_previous_call = now;

    return 0;
}

int
system_info_apply_uuid_by_str(const char *uuid_str)
{
    if (!uuid_str)
        return -EINVAL;

    else if (!uuid_is_null(systemInfoUuid))
        return -EALREADY;

    char my_uuid_str[UUID_STR_LEN] = {0};
    if (strnlen(uuid_str, UUID_STR_LEN) < (UUID_STR_LEN - 1))
        return -EINVAL;

    strncpy(my_uuid_str, uuid_str, UUID_STR_LEN - 1);

    uuid_t tmp;

    if (uuid_parse(my_uuid_str, tmp))
        return -EINVAL;

    uuid_copy(systemInfoUuid, tmp);

    return 0;
}

env_cb_ctx_t
system_info_apply_uuid_env_cb(const struct niova_env_var *ev)
{
    if (!ev)
        return;

    int rc = system_info_apply_uuid_by_str(ev->nev_string);
    if (rc)
        SIMPLE_LOG_MSG(LL_WARN, "system_info_apply_uuid_by_str('%s'): %s",
                       ev->nev_name, strerror(-rc));
}

static int
system_info_auto_detect_uuid(void)
{
    char proc_cmdline_path[PATH_MAX];
    char buf[SYS_INFO_PROCESS_CMDLINE_LEN] = {0};

    int rc = snprintf(proc_cmdline_path, PATH_MAX, "/proc/%u/cmdline",
                      getpid());
    if (rc >= PATH_MAX)
        return -ENAMETOOLONG;

    ssize_t rrc =
        file_util_open_and_read(AT_FDCWD, proc_cmdline_path, buf,
                                SYS_INFO_PROCESS_CMDLINE_LEN, NULL, NULL);
    if (rrc < 0) // Xxx this may happen if our buffer is too small
    {
        SIMPLE_LOG_MSG(LL_ERROR, "file_util_open_and_read(): %s",
                       strerror((int)-rrc));
        return -rrc;
    }

    niova_string_convert_null_to_space(buf, (size_t)rrc);

    // Set the cmd line here
    strncpy(systemInfoProcessCmdLine, buf, SYS_INFO_PROCESS_CMDLINE_LEN);

    regex_t regex;

    rc = regcomp(&regex, UUID_REGEX_PROC_CMDLINE, 0);
    if (rc)
    {
        char err_str[64] = {0};
        regerror(rc, &regex, err_str, 63);

        SIMPLE_LOG_MSG(LL_ERROR, "regcomp(): %s", err_str);

        return -EINVAL;
    }

    regmatch_t pmatch[1] = {0};
    rc = regexec(&regex, buf, 1, pmatch, 0);
    if (rc || pmatch[0].rm_so == -1 || pmatch[0].rm_eo <= pmatch[0].rm_so)
    {
        LOG_MSG(LL_NOTIFY, "regexec(): %d so:eo=%d:%d ",
                rc, pmatch[0].rm_so, pmatch[0].rm_eo);
    }
    else
    {
        const int match_len = pmatch[0].rm_eo - pmatch[0].rm_so;
        const char *match = &buf[pmatch[0].rm_so];

        int pos; // See the UUID_REGEX_PROC_CMDLINE for where '2' comes from

        // -u  <UUID>
        for (pos = 2; pos < match_len; pos++)
            if (!isspace(match[pos]))
                break;

        LOG_MSG(LL_NOTIFY, "uuid=%s", &match[pos]);

        rc = (pos < match_len) ?
            system_info_apply_uuid_by_str(&match[pos]) : -EFBIG;
    }

    regfree(&regex);

    return rc;
}

static init_ctx_t NIOVA_CONSTRUCTOR(SYSTEM_INFO_CTOR_PRIORITY)
system_info_subsystem_init(void)
{
    FUNC_ENTRY(LL_DEBUG);
    LREG_ROOT_OBJECT_ENTRY_INSTALL(system_info);

    niova_realtime_coarse_clock(&systemInfoStartTime);

    int rc = system_info_auto_detect_uuid();
    if (rc)
        SIMPLE_LOG_MSG(LL_NOTIFY, "system_info_auto_detect_system_uuid(): %s",
                       strerror(-rc));
}

static destroy_ctx_t NIOVA_DESTRUCTOR(SYSTEM_INFO_CTOR_PRIORITY)
system_info_subsystem_destroy(void)
{
    FUNC_ENTRY(LL_DEBUG);
}

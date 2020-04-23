/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2020
 */
#include <sys/inotify.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <limits.h>
#include <regex.h>

#define _GNU_SOURCE
#include <pthread.h>

#include "ctl_interface.h"
#include "ctl_interface_cmd.h"
#include "env.h"
#include "file_util.h"
#include "log.h"
#include "registry.h"
#include "system_info.h"
#include "thread.h"
#include "util_thread.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define DEFAULT_INOTIFY_PATH "/tmp/.niova"

#define INOTIFY_POLLING_MSEC 1000
#define INOTIFY_MAX_POLL_FDS 1
#define INOTIFY_BUFFER_SIZE 4096

#define LCTLI_MAX 1
#define LCTLI_DEFAULT_IDX 0

typedef void lctli_inotify_thread_t;
typedef int  lctli_inotify_thread_int_t;

struct ctl_interface
{
    const char        lctli_path[PATH_MAX];
    bool              lctli_init;
    int               lctli_inotify_fd;
    int               lctli_inotify_watch_fd;
    int               lctli_input_dirfd;
    int               lctli_output_dirfd;
    struct thread_ctl lctli_thr_ctl;
};

enum lctli_subdirs
{
    LCTLI_SUBDIR_INPUT,
    LCTLI_SUBDIR_OUTPUT,
    LCTLI_SUBDIR_MAX,
};

const char *lctliSubdirs[LCTLI_SUBDIR_MAX] =
{
    [LCTLI_SUBDIR_INPUT] = "input",
    [LCTLI_SUBDIR_OUTPUT] = "output"
};

static struct ctl_interface localCtlIf[LCTLI_MAX];
static pthread_mutex_t lctlMutex = PTHREAD_MUTEX_INITIALIZER;
static int numLocalCtlIfs;

const char *
lctli_get_inotify_path(void)
{
    return localCtlIf[LCTLI_DEFAULT_IDX].lctli_path;
}

static struct ctl_interface *
lctli_new(void)
{
    struct ctl_interface *new_lctli = NULL;

    pthread_mutex_lock(&lctlMutex);

    if (numLocalCtlIfs < LCTLI_MAX)
        new_lctli = &localCtlIf[numLocalCtlIfs++];

    pthread_mutex_unlock(&lctlMutex);

    return new_lctli;
}

// how do we deal with start-time config parameters which should be applied
// before the service completes startup?
// one example could be the number of memory buffers, etc.
// the method which returns the number of memory buffers, should itself
// install an lreg object.  In the case of a preconfigured option, this thread
// would install a temporary node which would be found at the time the initial
// query of the respective value is made.

// need a max number of start-time config options that can be present

static util_thread_ctx_t
lctli_inotify_thread_poll_parse_buffer(struct ctl_interface *lctli,
                                       char *buf, const ssize_t len)
{
    const struct inotify_event *event;

    for (char *ptr = buf; ptr < (buf + len);
         ptr += sizeof(struct inotify_event) + event->len)
    {
        event = (const struct inotify_event *)ptr;

        LOG_MSG(LL_DEBUG, "event@%p mask=%x name=%s %s ",
                event, event->mask, event->name,
                (event->mask & IN_ISDIR) ? "[dir]" : "[file]");

        if (!(event->mask & IN_ISDIR))
        {
            struct ctli_cmd_handle cch = {
                .ctlih_input_dirfd = lctli->lctli_input_dirfd,
                .ctlih_output_dirfd = lctli->lctli_output_dirfd,
                .ctlih_input_file_name = event->name
            };

            ctlic_process_request(&cch);
        }
#if 0
        if (!(event->mask & IN_ISDIR) &&
            (event->mask & IN_CLOSE_WRITE ||
             event->mask & IN_ATTRIB      ||
             event->mask & IN_MOVED_TO))
        {
        }
#endif
    }
}

static util_thread_ctx_t
lctli_inotify_thread_poll_handle_event(struct ctl_interface *lctli)
{
    char buf[INOTIFY_BUFFER_SIZE]
        __attribute__ ((aligned(__alignof__(struct inotify_event))));

    for (;;)
    {
        ssize_t len = read(lctli->lctli_inotify_fd, &buf, INOTIFY_BUFFER_SIZE);

        if (len < 0 && errno != EAGAIN)
        {
            len = errno;
            FATAL_MSG("read(): %s", strerror(len));
        }
        else if (len <= 0)
        {
            break;
        }
        else
        {
            lctli_inotify_thread_poll_parse_buffer(lctli, buf, len);
        }
    }
}

static util_thread_ctx_t
lctli_epoll_mgr_cb(const struct epoll_handle *eph)
{
    NIOVA_ASSERT(eph);

    struct ctl_interface *lctli = eph->eph_arg;

    if (eph->eph_fd != lctli->lctli_inotify_fd)
    {
        LOG_MSG(LL_ERROR, "invalid fd=%d (expected %d)",
                eph->eph_fd, lctli->lctli_inotify_fd);

        return;
    }

    return lctli_inotify_thread_poll_handle_event(lctli);
}

static int
lctli_prepare(struct ctl_interface *lctli)
{
    if (!lctli)
        return -EINVAL;

    else if (lctli->lctli_init)
        return -EALREADY;

    int rc = file_util_pathname_build(lctli->lctli_path);
    if (rc)
        return rc;

    for (int i = 0; i < LCTLI_SUBDIR_MAX; i++)
    {
        char subdir_path[PATH_MAX];

        int rc = snprintf(subdir_path, PATH_MAX, "%s/%s",
                          lctli->lctli_path, lctliSubdirs[i]);

        if (rc >= PATH_MAX)
            return -ENAMETOOLONG;

        rc = file_util_pathname_build(subdir_path);
        if (rc)
            return rc;

        if (i == LCTLI_SUBDIR_INPUT)
        {
            lctli->lctli_input_dirfd =
                open(subdir_path, O_DIRECTORY | O_RDONLY);

            if (lctli->lctli_input_dirfd < 0)
                return -errno;
        }
        else if (i == LCTLI_SUBDIR_OUTPUT)
        {
            lctli->lctli_output_dirfd =
                open(subdir_path, O_DIRECTORY | O_RDONLY);

            if (lctli->lctli_output_dirfd < 0)
                return -errno;
        }
    }

    lctli->lctli_inotify_fd = inotify_init1(IN_NONBLOCK);

    if (lctli->lctli_inotify_fd < 0)
    {
        int save_err = errno;

        LOG_MSG(LL_ERROR, "inotify_init1(): %s", strerror(save_err));

        return -save_err;
    }

    char input_path[PATH_MAX];

    rc = snprintf(input_path, PATH_MAX, "%s/%s",
                  lctli->lctli_path, lctliSubdirs[LCTLI_SUBDIR_INPUT]);
    if (rc >= PATH_MAX)
        return -ENAMETOOLONG;

    lctli->lctli_inotify_watch_fd =
        inotify_add_watch(lctli->lctli_inotify_fd, input_path,
                          IN_CLOSE_WRITE | IN_ATTRIB | IN_MOVED_TO);

    if (lctli->lctli_inotify_watch_fd < 0)
    {
        int save_err = errno;

        LOG_MSG(LL_ERROR, "inotify_add_watch(): %s", strerror(save_err));

        close(lctli->lctli_inotify_fd);
        return -save_err;
    }

    lctli->lctli_init = true;

    LOG_MSG(LL_DEBUG, "path=%s", lctli->lctli_path);

    return 0;
}

static int
lctli_setup_inotify_path(struct ctl_interface *lctli)
{
    // Presence of the env variable overrides all
    const struct niova_env_var *full_path_ev =
        env_get(NIOVA_ENV_VAR_inotify_path);

    if (full_path_ev && full_path_ev->nev_present)
    {
        strncpy((char *)lctli->lctli_path, full_path_ev->nev_string, PATH_MAX);
        return 0;
    }

    const struct niova_env_var *base_path_ev =
        env_get(NIOVA_ENV_VAR_inotify_base_path);

    const char *base_path =
        (base_path_ev && base_path_ev->nev_present) ?
        base_path_ev->nev_string : DEFAULT_INOTIFY_PATH;

    if (system_info_uuid_is_present())
    {
        uuid_t sys_uuid;
        system_info_get_uuid(sys_uuid);

        DECLARE_AND_INIT_UUID_STR(sys_uuid_str, sys_uuid);

        int rc = snprintf((char *)lctli->lctli_path, PATH_MAX, "/%s/%s/",
                          base_path, sys_uuid_str);
        if (rc < PATH_MAX)
            return 0;
    }

    int rc = snprintf((char *)lctli->lctli_path, PATH_MAX, "/%s/%d/",
                      base_path, getpid());
    if (rc < PATH_MAX)
        return -ENAMETOOLONG;

    LOG_MSG(LL_NOTIFY, "path defaulting to %s", lctli->lctli_path);

    return 0;
}

init_ctx_t
lctli_subsystem_init(void)
{
    struct ctl_interface *lctli = lctli_new();

    NIOVA_ASSERT(lctli);
    NIOVA_ASSERT(numLocalCtlIfs == 1);

    int rc = lctli_setup_inotify_path(lctli);
    FATAL_IF(rc, "lctli_setup_inotify_path(): %s", strerror(-rc));

    rc = lctli_prepare(lctli);
    FATAL_IF(rc, "lctli_prepare(): %s (path=%s)",
             strerror(-rc), lctli->lctli_path);

    rc = util_thread_install_event_src(lctli->lctli_inotify_fd, EPOLLIN,
                                       lctli_epoll_mgr_cb, (void *)lctli);

    FATAL_IF(rc, "util_thread_install_event_src(): %s", strerror(-rc));
}

destroy_ctx_t
lctli_subsystem_destroy(void)
{
    struct ctl_interface *lctli = &localCtlIf[LCTLI_DEFAULT_IDX];

    if (lctli->lctli_init)
    {
        //remove from utility thread?

        inotify_rm_watch(lctli->lctli_inotify_fd,
                         lctli->lctli_inotify_watch_fd);
    }

    return;
}

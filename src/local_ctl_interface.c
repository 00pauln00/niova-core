/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#include <sys/inotify.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <limits.h>

#define _GNU_SOURCE
#include <pthread.h>

#include "log.h"
#include "thread.h"
#include "local_registry.h"
#include "local_ctl_interface.h"
#include "env.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define DEFAULT_INOTIFY_PATH "/tmp/.niova"

#define INOTIFY_POLLING_MSEC 1000
#define INOTIFY_MAX_POLL_FDS 1
#define INOTIFY_BUFFER_SIZE 4096

#define LCTLI_MAX 1
#define LCTLI_DEFAULT_IDX 0

typedef void lctli_inotify_thread_t;
typedef int  lctli_inotify_thread_int_t;

struct local_ctl_interface
{
    const char       *lctli_path;
    bool              lctli_init;
    int               lctli_inotify_fd;
    int               lctli_inotify_watch_fd;
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

static struct local_ctl_interface localCtlIf[LCTLI_MAX];
static pthread_mutex_t lctlMutex = PTHREAD_MUTEX_INITIALIZER;
static int numLocalCtlIfs;

static struct local_ctl_interface *
lctli_new(void)
{
    struct local_ctl_interface *new_lctli = NULL;

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

static lctli_inotify_thread_t
lctli_inotify_thread_poll_parse_buffer(char *buf, const ssize_t len)
{
    const struct inotify_event *event;
    char *ptr;

    for (ptr = buf; ptr < (buf + len);
         ptr += sizeof(struct inotify_event) + event->len)
    {
        event = (const struct inotify_event *)ptr;

        LOG_MSG(LL_WARN, "event@%p mask=%x name=%s %s ",
                event, event->mask, event->name,
                (event->mask & IN_ISDIR) ? "[dir]" : "[file]");
#if 0
        if (!(event->mask & IN_ISDIR) &&
            (event->mask & IN_CLOSE_WRITE || event->mask & IN_ACCESS))
        {
        }
#endif
    }
}

static lctli_inotify_thread_t
lctli_inotify_thread_poll_handle_event(struct local_ctl_interface *lctli)
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
            lctli_inotify_thread_poll_parse_buffer(buf, len);
        }
    }
}

static lctli_inotify_thread_int_t
lctli_inotify_thread_poll(struct local_ctl_interface *lctli)
{
    struct pollfd pfd = {.fd = lctli->lctli_inotify_fd, .events = POLLIN};

    int rc = poll(&pfd, INOTIFY_MAX_POLL_FDS, INOTIFY_POLLING_MSEC);

    rc = rc < 0 ? -errno : rc;

    switch (rc)
    {
    case INOTIFY_MAX_POLL_FDS:
        lctli_inotify_thread_poll_handle_event(lctli);
        rc = 0;
        break;
    case -EINTR:
        rc = 0;
        break;
    default:
        break;
    }

    return rc;
}

static lctli_inotify_thread_t *
lctli_inotify_thread(void *arg)
{
    struct thread_ctl *tc = arg;
    struct local_ctl_interface *lctli = tc->tc_arg;

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");

    NIOVA_ASSERT(lctli);

    THREAD_LOOP_WITH_CTL(tc)
    {
        int rc = lctli_inotify_thread_poll(lctli);
        FATAL_IF(rc, "lctli_inotify_thread_poll(): %s", strerror(-rc));
    }

    return (void *)0;
}

static init_ctx_t
lctli_inotify_thread_start(struct local_ctl_interface *lctli)
{
    int rc = thread_create_watched(lctli_inotify_thread, &lctli->lctli_thr_ctl,
                                   "lctli_inotify", lctli, NULL);

    FATAL_IF(rc, "thread_create(): %s", strerror(errno));
}

static int
lctli_check_and_mk_inotify_path(const char *path)
{
    if (!path)
        return -EINVAL;

    struct stat stb;

    int rc = stat(path, &stb);

    if (rc && errno == ENOENT)
        rc = mkdir(path, 0755);

    return rc ? -errno : 0;
}

static int
lctli_prepare(struct local_ctl_interface *lctli, const char *path)
{
    if (!lctli || !path)
        return -EINVAL;

    else if (lctli->lctli_init)
        return -EALREADY;

    int rc = lctli_check_and_mk_inotify_path(path);
    if (rc)
        return rc;

    for (int i = 0; i < LCTLI_SUBDIR_MAX; i++)
    {
        char subdir_path[PATH_MAX];

        int rc = snprintf(subdir_path, PATH_MAX, "%s/%s",
                          path, lctliSubdirs[i]);

        if (rc >= PATH_MAX)
            return -ENAMETOOLONG;

        rc = lctli_check_and_mk_inotify_path(subdir_path);
        if (rc)
            return rc;
    }

    lctli->lctli_path = path;

    lctli->lctli_inotify_fd = inotify_init1(IN_NONBLOCK);

    if (lctli->lctli_inotify_fd < 0)
    {
        int save_err = errno;

        LOG_MSG(LL_ERROR, "inotify_init1(): %s", strerror(save_err));

        return -save_err;
    }

    char input_path[PATH_MAX];

    rc = snprintf(input_path, PATH_MAX, "%s/%s",
                  path, lctliSubdirs[LCTLI_SUBDIR_INPUT]);
    if (rc >= PATH_MAX)
        return -ENAMETOOLONG;

    lctli->lctli_inotify_watch_fd =
        inotify_add_watch(lctli->lctli_inotify_fd, input_path,
                          IN_CLOSE_WRITE | IN_ACCESS);

    if (lctli->lctli_inotify_watch_fd < 0)
    {
        int save_err = errno;

        LOG_MSG(LL_ERROR, "inotify_add_watch(): %s", strerror(save_err));

        close(lctli->lctli_inotify_fd);
        return -save_err;
    }

    lctli->lctli_init = true;

    LOG_MSG(LL_DEBUG, "path=%s", path);

    return 0;
}

init_ctx_t
lctli_subsystem_init(void)
{
    struct local_ctl_interface *lctli = lctli_new();

    NIOVA_ASSERT(lctli);
    NIOVA_ASSERT(numLocalCtlIfs == 1);

    const struct niova_env_var *ev = env_get(NIOVA_ENV_VAR_inotify_path);

    const char *inotify_path = (ev && ev->nev_present) ?
        ev->nev_string : DEFAULT_INOTIFY_PATH;

    int rc = lctli_prepare(lctli, inotify_path);

    FATAL_IF(rc, "lctli_prepare(): %s", strerror(-rc));

    lctli_inotify_thread_start(lctli);

    thread_ctl_run(&lctli->lctli_thr_ctl);
}

destroy_ctx_t
lctli_subsystem_destroy(void)
{
    struct local_ctl_interface *lctli = &localCtlIf[LCTLI_DEFAULT_IDX];

    if (lctli->lctli_init)
    {
        thread_halt_and_destroy(&lctli->lctli_thr_ctl);

        inotify_rm_watch(lctli->lctli_inotify_fd,
                         lctli->lctli_inotify_watch_fd);
    }

    return;
}

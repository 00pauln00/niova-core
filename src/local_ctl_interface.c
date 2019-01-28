/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#include <sys/inotify.h>
#include <poll.h>

#define _GNU_SOURCE
#include <pthread.h>

#include "log.h"
#include "thread.h"
#include "local_registry.h"
#include "local_ctl_interface.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define TEST_INOTIFY_PATH "/tmp/.niova"

#define INOTIFY_POLLING_MSEC 1000
#define INOTIFY_MAX_POLL_FDS 1
#define INOTIFY_BUFFER_SIZE 4096

typedef void lctli_inotify_thread_t;
typedef int  lctli_inotify_thread_int_t;

static bool              lctliInitialized;
static int               lctliInotifyFd;
static int               lctliInotifyWatchFd;
static struct thread_ctl lctliThreadCtl;

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
lctli_inotify_thread_poll_handle_event(void)
{
    char buf[INOTIFY_BUFFER_SIZE]
        __attribute__ ((aligned(__alignof__(struct inotify_event))));

    for (;;)
    {
        ssize_t len = read(lctliInotifyFd, &buf, INOTIFY_BUFFER_SIZE);

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
lctli_inotify_thread_poll(void)
{
    struct pollfd pfd = {.fd = lctliInotifyFd, .events = POLLIN};

    int rc = poll(&pfd, INOTIFY_MAX_POLL_FDS, INOTIFY_POLLING_MSEC);

    rc = rc < 0 ? -errno : rc;

    switch (rc)
    {
    case INOTIFY_MAX_POLL_FDS:
        lctli_inotify_thread_poll_handle_event();
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

    SIMPLE_LOG_MSG(LL_DEBUG, "hello");

    THREAD_LOOP_WITH_CTL(tc)
    {
        int rc = lctli_inotify_thread_poll();
        FATAL_IF(rc, "lctli_inotify_thread_poll(): %s", strerror(-rc));
    }

    return (void *)0;
}

static init_ctx_t
lctli_inotify_thread_start(void)
{
    int rc = thread_create(lctli_inotify_thread, &lctliThreadCtl,
                           "lctli_inotify", NULL, NULL);

    FATAL_IF(rc, "thread_create(): %s", strerror(errno));
}

init_ctx_t
lctli_subsystem_init(void)
{
    NIOVA_ASSERT(!lctliInitialized);

    lctliInotifyFd = inotify_init1(IN_NONBLOCK);

    FATAL_IF_strerror((lctliInotifyFd < 0),
                      "inotify_init1(), path=%s", TEST_INOTIFY_PATH);

    lctliInotifyWatchFd =
        inotify_add_watch(lctliInotifyFd, TEST_INOTIFY_PATH,
                          IN_CLOSE_WRITE | IN_ACCESS);

    FATAL_IF_strerror((lctliInotifyWatchFd < 0),
                      "inotify_add_watch(), path=%s", TEST_INOTIFY_PATH);

    lctli_inotify_thread_start();

    thread_ctl_run(&lctliThreadCtl);
}

destroy_ctx_t
lctli_subsystem_destroy(void)
{
    thread_halt_and_destroy(&lctliThreadCtl);

    inotify_rm_watch(lctliInotifyFd, lctliInotifyWatchFd);

    return;
}

/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <00pauln00@gmail.com> 2018
 */
#define _GNU_SOURCE 1 // for O_DIRECT
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/epoll.h>
#include <fcntl.h>

#include "init.h"
#include "log.h"
#include "registry.h"
#include "ctl_interface.h"
#include "random.h"
#include "alloc.h"

#include "niosd_io.h"
#include "epoll_mgr.h"

REGISTRY_ENTRY_FILE_GENERATE;

/* This is the default test device, in case none is specified.
 */
#define TEST_DEVICE_NAME "./test_niosd.device"
#define TEST_BULK_BUFFER_SIZE (1024ul * 1024ul)

#define MAX_IO_DEPTH 32768
#define OPTS         "f:s:n:z:d:r:u:t:e:ES"
#define MAX_DEV_SIZE (1ULL << 44) //4TiB
#define MIN_DEV_SIZE (1ULL << 30) //1GiB
#define DEF_DEV_SIZE (MIN_DEV_SIZE * 10ULL)
#define IO_DEPTH     (TEST_BULK_BUFFER_SIZE / NIOVA_BLOCK_SIZE)
#define DEF_NUM_SECTORS 8

static char *testDevName = TEST_DEVICE_NAME;
static struct stat testDeviceStat;
static size_t testDevSize = DEF_DEV_SIZE;
static size_t numOps = 10000;
static size_t ioNumSectors = DEF_NUM_SECTORS;
static unsigned int rwRatio = 50;
static char *ioBuffer;
static struct niosd_io_request *niorqArray;
static size_t niorqsDone;
static size_t niorqsSubmitted;
static size_t ioDepth =
    (TEST_BULK_BUFFER_SIZE / (DEF_NUM_SECTORS * NIOVA_SECTOR_SIZE));

static useconds_t pollSleepUsecs;
static struct timespec runTime;
static int sleepBeforeExit;
static bool useEpoll = false;
struct epoll_mgr epollMgr;
struct epoll_handle epollHandle;
static struct niosd_device ndev;
static bool sequentialIO = false;

static size_t epollCbExecCnt;

static void
epollCb(const struct epoll_handle *eph, uint32_t events)
{
    FUNC_ENTRY(LL_DEBUG);
    NIOVA_ASSERT(eph == &epollHandle);

    struct niosd_io_ctx *nioctx = eph->eph_arg;
    NIOVA_ASSERT(nioctx);

    EV_PIPE_RESET(&nioctx->nioctx_evp);

    epollCbExecCnt++;

    size_t num_complete = 0;
    for (;;)
    {
        size_t n = niosd_io_events_complete(nioctx,
                                            MIN(ioDepth,
                                                (numOps - niorqsDone)));
        num_complete += n;
        if (!n)
            break;
    }

    SIMPLE_LOG_MSG(LL_DEBUG,
                   "epollCbExecCnt=%zu ncomp-here=%zd",
                   epollCbExecCnt, num_complete);
}

static enum niosd_io_request_type
niot_get_op_type(unsigned int rw_ratio, unsigned int val)
{
    if (rw_ratio == 0)
        return NIOSD_REQ_TYPE_PWRITE;

    else if (rw_ratio == 100)
        return NIOSD_REQ_TYPE_PREAD;

    return ((val % 100) < rw_ratio) ?
        NIOSD_REQ_TYPE_PREAD : NIOSD_REQ_TYPE_PWRITE;
}

static pblk_id_t
niot_get_pblk_id(const struct stat *ndev_stb, unsigned int val)
{
    NIOVA_ASSERT(ndev_stb && ndev_stb->st_size >= MIN_DEV_SIZE);

    const pblk_id_t num_pblks = ndev_stb->st_size / PBLK_SIZE_BYTES;

    return val % num_pblks;
}

static size_t
niot_request_to_pos(const struct niosd_io_request *niorq)
{
    NIOVA_ASSERT(niorq >= niorqArray);

    const uintptr_t diff = (const char *)niorq - (const char *)niorqArray;

//    NIOVA_ASSERT(!(diff % sizeof(struct niosd_io_request)));

    return diff / sizeof(struct niosd_io_request);
}

static char *
niot_request_to_buffer(const struct niosd_io_request *niorq)
{
    NIOVA_ASSERT(ioBuffer);

    size_t pos = niot_request_to_pos(niorq);
    size_t off = pos * ioNumSectors * NIOVA_SECTOR_SIZE;

    NIOVA_ASSERT(off < (ioDepth * ioNumSectors * NIOVA_SECTOR_SIZE));

    char *ptr = &ioBuffer[off];

    return ptr;
}

static void
niot_allocate_bulk_and_niorq_buffers(size_t num_sectors, size_t io_depth)
{
    const size_t block_size = num_sectors * NIOVA_SECTOR_SIZE;

    int rc = posix_memalign((void **)&ioBuffer, block_size,
                            block_size * io_depth);

    NIOVA_ASSERT(ioBuffer && !rc);

    niorqArray = niova_calloc(io_depth, sizeof(struct niosd_io_request));

    NIOVA_ASSERT(niorqArray);
}

static int
niot_submit_request(struct niosd_device *ndev, struct niosd_io_request *niorq);

static void
niot_request_cb(struct niosd_io_request *niorq)
{
    niorqsDone++;

    if (niorq_has_error(niorq))
        DBG_NIOSD_REQ(LL_ERROR, niorq, "nreq-done=%zu", niorqsDone);

    if (niorqsDone < numOps)
    {
        int rc =
            niot_submit_request(niosd_ctx_to_device(niorq->niorq_ctx), niorq);

        if (rc)
        {
            STDERR_MSG("niot_submit_request() failed: %s", strerror(-rc));
            exit(-rc);
        }
    }
}

static int
niot_submit_request(struct niosd_device *ndev, struct niosd_io_request *niorq)
{
    NIOVA_ASSERT(ndev && niorq);

    if (niorqsSubmitted >= numOps)
        return 0; // Don't exceed the number of test operations.

    static int counter;

    unsigned int val = sequentialIO ? counter++ : random_get();

    int rc =
        niosd_io_request_init(niorq,
                              niosd_device_to_ctx(ndev,
                                                  NIOSD_IO_CTX_TYPE_USER),
                              niot_get_pblk_id(&ndev->ndev_stb, val),
                              niot_get_op_type(rwRatio, val),
                              ioNumSectors,
                              (void *)niot_request_to_buffer(niorq),
                              niot_request_cb, NULL);
    if (rc)
        return rc;

    rc = niosd_io_submit(&niorq, 1);

    if (rc == 1)
        niorqsSubmitted++;

    counter++;

    return rc == 1 ? 0 : rc;
}

static int
niot_create_test_device(const char *dev_name, size_t dev_size)
{
    int fd = open(dev_name, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0)
    {
        int rc = errno;
        STDERR_MSG("open(%s): %s", dev_name, strerror(rc));

        return -rc;
    }

    int rc = ftruncate(fd, (off_t)dev_size);
    if (rc < 0)
    {
        rc = errno;
        STDERR_MSG("ftruncate(): %s", strerror(rc));

        rc = -rc;
    }

    close(fd);

    return rc;
}

static int
niot_prepare_test_device(const char *dev_name, size_t dev_size)
{
    int rc = stat(dev_name, &testDeviceStat);
    if (rc)
    {
        if (errno == ENOENT)
        {
            if (niot_create_test_device(dev_name, dev_size))
                exit(1);

            rc = stat(dev_name, &testDeviceStat);
            if (rc)
            {
                rc = errno;
                STDERR_MSG("stat(): %s", strerror(rc));
            }
        }
    }

    return rc;
}

static bool
niot_test_time_expired(useconds_t *sleep_usecs,
                       const struct timespec *start_time)
{
    if (runTime.tv_sec > 0)
    {
        struct timespec current_run_time;
        niova_unstable_clock(&current_run_time);

        timespecsub(&current_run_time, start_time, &current_run_time);

        if (timespeccmp(&current_run_time, &runTime, >=))
        {
            return true;
        }
        else
        {
            timespecsub(&runTime, &current_run_time, &current_run_time);
            *sleep_usecs = MIN(timespec_2_usec(&current_run_time),
                               pollSleepUsecs);

            SIMPLE_LOG_MSG(LL_TRACE,
                           "remaining=%ld.%ld", current_run_time.tv_sec,
                           current_run_time.tv_nsec);
        }
    }

    return false;
}

static void
niot_spin_niorq_completion(struct niosd_device *ndev,
                           const struct timespec *start_time)
{
    struct niosd_io_ctx *nioctx =
        niosd_device_to_ctx(ndev, NIOSD_IO_CTX_TYPE_USER);

    useconds_t sleep_usecs = pollSleepUsecs;
    bool timeout_reached = false;

    for (; niorqsDone < numOps;)
    {
        if (niot_test_time_expired(&sleep_usecs, start_time))
        {
            timeout_reached = true;
            break;
        }

        if (useEpoll)
        {
            int rc = epoll_mgr_wait_and_process_events(&epollMgr, 100);
            if (rc < 0)
                STDERR_MSG("epoll_mgr_wait_and_process_events(): %s",
                           strerror(errno));
        }
        else
        {
            for (;;)
            {
                if(!niosd_io_events_complete(nioctx,
                                             MIN(ioDepth,
                                                 (numOps - niorqsDone))))
                    break;
            }

            if (niorqsDone < numOps && pollSleepUsecs)
                usleep(sleep_usecs);
        }
    }

    if (niorqsDone != numOps && !timeout_reached)
        STDERR_MSG("num_completed=%zu", niorqsDone);
}

static void
niot_print_help(const int error)
{
    fprintf(error ? stderr : stdout,
            "niosd_io_test [-f test-device (or file)] [-z dev-size-bytes]\n"
            "              [-n num-ops] [-s io-num-sectors (512-byte)]\n"
            "              [-d io-depth] [-r read-ratio]\n"
            "              [-u poll-usecs-sleep] [-t max-time (secs)]\n"
            "              [-e extend-runtime (secs)] [-E epoll-blocking]\n");
    exit(error);
}

static void
niot_getopt(int argc, char **argv)
{
    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'f':
            testDevName = optarg;
            break;
        case 'z':
            testDevSize = atoll(optarg);
            if (testDevSize > MAX_DEV_SIZE || testDevSize < MIN_DEV_SIZE)
            {
                fprintf(stderr, "invalid device-size: min=%llu, max=%llu\n",
                        MIN_DEV_SIZE, MAX_DEV_SIZE);
                exit(1);
            }
        case 'n':
            numOps = atoll(optarg);
            break;
        case 's':
            ioNumSectors = atoll(optarg);
            break;
        case 'd':
            ioDepth = atoll(optarg);
            if (!ioDepth || ioDepth > MAX_IO_DEPTH)
            {
                fprintf(stderr, "invalid io-depth: min=%u, max=%u\n",
                        1, MAX_IO_DEPTH);
                exit(1);
            }
            break;
        case 'r':
            rwRatio = atoi(optarg);
            if (rwRatio > 100)
            {
                fprintf(stderr, "invalid read-ratio, "
                                "please specify value between 0 and 100\n");
                exit(1);
            }
            break;
        case 'u':
            pollSleepUsecs = atoi(optarg);
            break;
        case 't':
            runTime.tv_sec = atoi(optarg);
            break;
        case 'e':
            sleepBeforeExit = atoi(optarg);
            break;
        case 'E':
            useEpoll = true;
            break;
        case 'S':
            sequentialIO = true;
            break;
        default:
            niot_print_help(EINVAL);
            break;
        }
    }
}

static void
niot_print_stats(const struct timespec *wall_time)
{
    struct rusage rusage = {0};

    if (getrusage(RUSAGE_SELF, &rusage))
        STDERR_MSG("getrusage() failed:  %s", strerror(errno));

    float iops = niorqsDone / timespec_2_float(wall_time);
    float bw = iops * ioNumSectors * NIOVA_SECTOR_SIZE / 1048576;

    fprintf(stdout,
            "NIOVA niosd Test\n"
            "\tdevice:      %s\n"
            "\tio-pattern:  random\n"
            "\tio-depth:    %zu\n"
            "\tio-size:     %zu\n"
            "\tnum-io-ops:  %zu / %zu (Completed:  %.02f%%)\n"
            "\tpoll-usleep: %u usecs\n"
            "\tread-ratio:  %u\n"
            "\twall-time:   %ld.%ld\n"
            "\tiops:        %.2f\n"
            "\tbandwidth:   %.2f MiB/sec\n\n"

            "System Usage Stats\n"
            "\tusr-time:    %ld.%ld\n"
            "\tsys-time:    %ld.%ld\n"
            "\tmaxrss:      %ldkb\n"
            "\tminflt:      %ld\n"
            "\tmajflt:      %ld\n"
            "\tinblock:     %ld\n"
            "\toutblock:    %ld\n"
            "\tvol-ctxsw:   %ld\n"
            "\tinvol-ctxsw: %ld\n",
            testDevName, ioDepth,
            ioNumSectors * NIOVA_SECTOR_SIZE, niorqsDone, numOps,
            (100.00 * (float)niorqsDone / (float)numOps), pollSleepUsecs,
            rwRatio, wall_time->tv_sec, wall_time->tv_nsec, iops, bw,
            rusage.ru_utime.tv_sec, rusage.ru_utime.tv_usec,
            rusage.ru_stime.tv_sec, rusage.ru_stime.tv_usec,
            rusage.ru_maxrss, rusage.ru_minflt, rusage.ru_majflt,
            rusage.ru_inblock, rusage.ru_oublock, rusage.ru_nvcsw,
            rusage.ru_nivcsw);
}

static void
prepare_epoll(struct niosd_device *ndev)
{
    NIOVA_ASSERT(useEpoll);

    int rc = epoll_mgr_setup(&epollMgr);
    FATAL_IF((rc), "epoll_mgr_setup(): %s", strerror(-rc));

    struct niosd_io_ctx *nioctx =
        niosd_device_to_ctx(ndev, NIOSD_IO_CTX_TYPE_USER);

    int fd = nioctx_blocking_mode_fd_get(nioctx);
    FATAL_IF((fd < 0), "nioctx_blocking_mode_fd_get(): %s", strerror(-fd));

    rc = epoll_handle_init(&epollHandle, fd, EPOLLIN, epollCb, NULL, nioctx);
    FATAL_IF((rc), "epoll_handle_init(): %s", strerror(-rc));

    rc = epoll_handle_add(&epollMgr, &epollHandle);
    FATAL_IF((rc), "epoll_handle_add(): %s", strerror(-rc));
}

int
main(int argc, char **argv)
{
    REGISTY_ENTRY_FUNCTION_GENERATE;

    niot_getopt(argc, argv);

    /* Prepare the test device, stat'ing it and creating if needed.
     */
    int rc = niot_prepare_test_device(testDevName, testDevSize);
    if (rc)
        exit(rc);

    niosd_device_params_init(testDevName, &ndev);
    if (useEpoll)
        niosd_device_params_enable_blocking_mode(&ndev,
                                                 NIOSD_IO_CTX_TYPE_USER);

    /* Allocate an aligned buffer which can hold a lots of concurrent requests.
     */
    niot_allocate_bulk_and_niorq_buffers(ioNumSectors, ioDepth);

    /* Open the niosd.
     */
    rc = niosd_device_open(&ndev);
    if (rc)
        exit(rc);

    /* NIOSD_IO_CTX_TYPE_USER should have an epoll'able fd for us.
     */
    if (useEpoll)
        prepare_epoll(&ndev);

    const size_t num_initial_launch = MIN(numOps, ioDepth);

    struct timespec ts[2];
    niova_unstable_clock(&ts[0]);

    for (size_t i = 0; i < num_initial_launch; i++)
    {
        rc = niot_submit_request(&ndev, &niorqArray[i]);
        if (rc)
        {
            STDERR_MSG("niot_submit_request(): %s", strerror(-rc));
            (int)niosd_device_close(&ndev);
            exit(rc);
        }
    }

    niot_spin_niorq_completion(&ndev, &ts[0]);

    niova_unstable_clock(&ts[1]);
    timespecsub(&ts[1], &ts[0], &ts[0]);

    lreg_node_recurse("log_entry_map");
    lreg_node_recurse("nioctx_stats_root_entry");

    niot_print_stats(&ts[0]);

    if (sleepBeforeExit)
        sleep(sleepBeforeExit);

    return niosd_device_close(&ndev);
}

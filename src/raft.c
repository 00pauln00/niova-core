/* Copyright (C) NIOVA Systems, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Paul Nowoczynski <pauln@niova.io> 2020
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/timerfd.h>
#include <linux/limits.h>

#include "log.h"
#include "udp.h"
#include "epoll_mgr.h"
#include "crc32.h"
#include "alloc.h"
#include "io.h"

REGISTRY_ENTRY_FILE_GENERATE;

#define MAX_PEERS 11

typedef uint8_t raft_peer_t;

#define OPTS "p:hL:"

#define RAFT_DEFAULT_LOG_PATH "/var/tmp/niova-raft-log."

#define NUM_RAFT_LOG_HEADERS 2
#define RAFT_ENTRY_PAD_SIZE 95
#define RAFT_ENTRY_MAGIC  0x1a2b3c4dd4c3b2a1
#define RAFT_HEADER_MAGIC 0xafaeadacabaaa9a8

struct raft_entry_header
{
    uint64_t reh_magic;     // Magic is not included in the crc
    crc32_t  reh_crc;       // Crc is after the magic
    uint32_t reh_data_size; // The size of the log entry data
    int64_t  reh_index;    // Must match physical offset + NUM_RAFT_LOG_HEADERS
    int64_t  reh_term;
    uint32_t reh_log_hdr_blk:1;
    char     reh_pad[RAFT_ENTRY_PAD_SIZE];
};

#define RAFT_ENTRY_HEADER_RESERVE 128

#define RAFT_ENTRY_SIZE           65536
#define RAFT_ENTRY_MAX_DATA_SIZE  (RAFT_ENTRY_SIZE - RAFT_ENTRY_HEADER_RESERVE)

static inline void
raft_compile_time_checks(void)
{
    COMPILE_TIME_ASSERT(sizeof(struct raft_entry_header) ==
                        RAFT_ENTRY_HEADER_RESERVE);
}

struct raft_entry
{
    struct raft_entry_header re_header;
    char                     re_data[];
};

struct raft_log_header
{
    uint64_t    rlh_magic;
    int64_t     rlh_current_term;
    uint64_t    rlh_seqno;
    raft_peer_t rlh_voted_for;
};

#define RAFT_LOG_HEADER_DATA_SIZE sizeof(struct raft_log_header)

enum raft_state
{
    RAFT_STATE_LEADER,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_CANDIDATE,
};

#define RAFT_LOG_SUFFIX_MAX_LEN 8

enum raft_epoll_handles
{
    RAFT_EPOLL_HANDLE_PEER_UDP,
    RAFT_EPOLL_HANDLE_CLIENT_UDP,
    RAFT_EPOLL_HANDLE_TIMERFD,
    RAFT_EPOLL_NUM_HANDLES,
};

struct raft_instance
{
    struct udp_socket_handle ri_ush;
    raft_peer_t              ri_peerno;
    uint8_t                  ri_init_log_path;
    enum raft_state          ri_state;
    int                      ri_timer_fd;
    int                      ri_log_fd;
    char                     ri_log[PATH_MAX + 1];
    struct stat              ri_log_stb;
    uint64_t                 ri_log_hdr_seqno;
    int64_t                  ri_term;
    raft_peer_t              ri_voted_for; // in 'ri_term'
    struct epoll_mgr         ri_epoll_mgr;
    struct epoll_handle      ri_epoll_handles[RAFT_EPOLL_NUM_HANDLES];
};

#define RAFT_PEER_ANY ID_ANY_8bit

static struct raft_instance myRaft = {
    .ri_state          = RAFT_STATE_FOLLOWER,
    .ri_peerno         = RAFT_PEER_ANY,
    .ri_log_fd         = -1,
    .ri_init_log_path  = 1,
    .ri_ush.ush_socket = -1,
    .ri_ush.ush_port   = NIOVA_DEFAULT_UDP_PORT,
    .ri_ush.ush_ipaddr = "127.0.0.1",
};

#define DBG_RAFT_ENTRY(log_level, re, fmt, ...)                         \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "re@%p crc=%x size=%u idx=%ld term=%ld lb=%x "fmt,   \
                   (re), (re)->reh_crc, (re)->reh_data_size, (re)->reh_index, \
                   (re)->reh_term, (re)->reh_log_hdr_blk, ##__VA_ARGS__)

#define DBG_RAFT_INSTANCE(log_level, ri, fmt, ...)                      \
    SIMPLE_LOG_MSG(log_level,                                           \
                   "ri@%p peer=%hhu state=%x term=%ld seqno=%ld v=%hhx "fmt, \
                   (ri), (ri)->ri_peerno, (ri)->ri_state, (ri)->ri_term, \
                   (ri)->ri_log_hdr_seqno, (ri)->ri_voted_for, ##__VA_ARGS__)

static crc32_t
raft_server_entry_calc_crc(const struct raft_entry *re)
{
    NIOVA_ASSERT(re);

    const struct raft_entry_header *rh = &re->re_header;
    const size_t offset = offsetof(struct raft_entry_header, reh_data_size);
    const unsigned char *buf = (const unsigned char *)re + offset;
    const int crc_len = sizeof(struct raft_entry) + rh->reh_data_size - offset;
    NIOVA_ASSERT(crc_len >= 0);

    crc32_t crc = crc_pcl(buf, crc_len, 0);

    DBG_RAFT_ENTRY(((crc == rh->reh_crc) ? LL_DEBUG : LL_WARN),
                   &re->re_header, "calculated crc=%x", crc);

    return crc;
}

static int
raft_server_entry_check_crc(const struct raft_entry *re)
{
    NIOVA_ASSERT(re);

    const struct raft_entry_header *reh = &re->re_header;

    return raft_server_entry_calc_crc(re) == reh->reh_crc ? 0 : -EBADMSG;
}

static void
raft_server_entry_init(struct raft_entry *re, const size_t entry_index,
                       const uint64_t current_term, const char *data,
                       const size_t len)
{
    NIOVA_ASSERT(re);
    NIOVA_ASSERT(data && len);
    NIOVA_ASSERT(len <= RAFT_ENTRY_MAX_DATA_SIZE);

    struct raft_entry_header *reh = &re->re_header;

    reh->reh_magic = RAFT_ENTRY_MAGIC;
    reh->reh_data_size = len;
    reh->reh_index = entry_index;
    reh->reh_term = current_term;
    reh->reh_log_hdr_blk = entry_index < NUM_RAFT_LOG_HEADERS ? 1 : 0;

    memset(reh->reh_pad, 0, RAFT_ENTRY_PAD_SIZE);

    memcpy(re->re_data, data, len);

    reh->reh_crc = raft_server_entry_calc_crc(re);
}

static int
raft_server_entry_write(struct raft_instance *ri, const size_t entry_index,
                        const char *data, size_t len)
{
    if (!ri || !data || !len)
        return -EINVAL;

    else if (len > RAFT_ENTRY_MAX_DATA_SIZE)
        return -E2BIG;

    const off_t total_entry_size = sizeof(struct raft_entry) + len;

    struct raft_entry *re = niova_malloc(total_entry_size);
    if (!re)
        return -ENOMEM;

    raft_server_entry_init(re, entry_index, ri->ri_term, data, len);

    DBG_RAFT_ENTRY(LL_WARN, &re->re_header, "");

    const ssize_t write_sz =
        io_pwrite(ri->ri_log_fd, (const char *)re, total_entry_size,
                  (entry_index * RAFT_ENTRY_SIZE));

    NIOVA_ASSERT(write_sz == total_entry_size);

    int rc = io_fsync(ri->ri_log_fd);
    NIOVA_ASSERT(!rc);

    niova_free(re);

    return 0;
}

static int
read_server_entry_validate(const struct raft_entry_header *rh,
                           const size_t intended_entry_index)
{
    if (rh->reh_magic != RAFT_ENTRY_MAGIC ||
        rh->reh_data_size > RAFT_ENTRY_MAX_DATA_SIZE)
        return -EBADMSG;

    ssize_t my_intended_entry_index =
        intended_entry_index -
        (rh->reh_log_hdr_blk ? 0 : NUM_RAFT_LOG_HEADERS);

    if (my_intended_entry_index < 0 ||
        (size_t)my_intended_entry_index != intended_entry_index)
        return -EBADMSG;

    return 0;
}

static int
raft_server_entry_read(struct raft_instance *ri, const size_t entry_index,
                       char *data, const size_t len, size_t *rc_len)
{
    if (!ri || !data || len > RAFT_ENTRY_SIZE)
        return -EINVAL;

    const off_t total_entry_size = sizeof(struct raft_entry) + len;

    struct raft_entry *re = niova_malloc(total_entry_size);
    if (!re)
        return -ENOMEM;

    const ssize_t read_sz =
        io_pread(ri->ri_log_fd, (char *)re, total_entry_size,
                 (entry_index * RAFT_ENTRY_SIZE));

    DBG_RAFT_ENTRY(LL_WARN, &re->re_header, "rrc=%zu", read_sz);

    NIOVA_ASSERT(read_sz == total_entry_size);

    const struct raft_entry_header *rh = &re->re_header;

    int rc = read_server_entry_validate(rh, entry_index);
    if (!rc)
    {
        if (rc_len)
            *rc_len = rh->reh_data_size;

        if (rh->reh_data_size < len)
        {
            rc = -ENOSPC;
        }
        else
        {
            rc = raft_server_entry_check_crc(re);
            if (!rc)
                memcpy(data, re->re_data, len);
        }
    }

    niova_free(re);

    return rc;
}

static int
raft_server_header_write(struct raft_instance *ri, struct raft_log_header *rlh)
{
    if (!ri || !rlh)
        return -EINVAL;

    rlh->rlh_seqno = ri->ri_log_hdr_seqno++;

    const size_t block_num = rlh->rlh_seqno % NUM_RAFT_LOG_HEADERS;

    return raft_server_entry_write(ri, block_num, (const char *)rlh,
                                   sizeof(*rlh));
}

static int
raft_server_header_load(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    struct raft_log_header rlh[NUM_RAFT_LOG_HEADERS] = {0};
    struct raft_log_header *most_recent_rlh = NULL;

    for (size_t i = 0; i < NUM_RAFT_LOG_HEADERS; i++)
    {
        size_t rc_len = 0;
        char *buf = (char *)((struct raft_log_header *)&rlh[i]);

        int rc = raft_server_entry_read(ri, i, buf,
                                        sizeof(struct raft_log_header),
                                        &rc_len);

        if (!rc && rc_len == sizeof(struct raft_log_header))
        {
            if (!most_recent_rlh ||
                rlh[i].rlh_seqno > most_recent_rlh->rlh_seqno)
                most_recent_rlh = &rlh[i];

        }
    }

    if (!most_recent_rlh) // No valid header entries were found
        return -EBADMSG;

    ri->ri_term = most_recent_rlh->rlh_current_term;
    ri->ri_log_hdr_seqno = most_recent_rlh->rlh_seqno;
    ri->ri_voted_for = most_recent_rlh->rlh_voted_for;

    return 0;
}

static int
raft_server_log_file_setup_init_header(struct raft_instance *ri)
{
    if (!ri || ri->ri_log_fd < 0 || ri->ri_log_stb.st_size != 0)
        return -EINVAL;

    struct raft_log_header rlh = {.rlh_magic = RAFT_HEADER_MAGIC,
                                  .rlh_current_term = 0,
                                  .rlh_voted_for = RAFT_PEER_ANY,
                                  .rlh_seqno = 0, // will be overwritten
    };

    for (int i = 0; i < NUM_RAFT_LOG_HEADERS; i++)
    {
        int rc = raft_server_header_write(ri, &rlh);
        if (rc)
            return rc;
    }

    return 0;
}

static int
raft_server_log_file_name_setup(struct raft_instance *ri)
{
    if (ri->ri_init_log_path)
    {
        int rc = snprintf(ri->ri_log, PATH_MAX, "%s", RAFT_DEFAULT_LOG_PATH);
        if (rc > PATH_MAX - RAFT_LOG_SUFFIX_MAX_LEN)
            return -ENAMETOOLONG;
    }

    size_t file_name_len = strnlen(ri->ri_log, PATH_MAX);
    if ((PATH_MAX - file_name_len) <= RAFT_LOG_SUFFIX_MAX_LEN)
        return -ENAMETOOLONG;

    char suffix[RAFT_LOG_SUFFIX_MAX_LEN];
    int rc = snprintf(suffix, RAFT_LOG_SUFFIX_MAX_LEN, "%hhu", ri->ri_peerno);
    if (rc >= RAFT_LOG_SUFFIX_MAX_LEN)
        return -ENAMETOOLONG;

    strncpy(&ri->ri_log[file_name_len], suffix, RAFT_LOG_SUFFIX_MAX_LEN);

    return 0;
}

static int
raft_server_log_file_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = raft_server_log_file_name_setup(ri);
    if (rc)
        return rc;

    SIMPLE_LOG_MSG(LL_WARN, "log-file=%s", ri->ri_log);

    ri->ri_log_fd = open(ri->ri_log, O_CREAT | O_RDWR | O_SYNC, 0600);
    if (ri->ri_log_fd < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "open(`%s'): %s", ri->ri_log, strerror(-rc));
        return rc;
    }

    rc = fstat(ri->ri_log_fd, &ri->ri_log_stb);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "fstat(): %s", strerror(-rc));
        return rc;
    }

    /* Initialize the log header if the file was just created.
     */
    if (!ri->ri_log_stb.st_size)
    {
        rc = raft_server_log_file_setup_init_header(ri);
        if (rc)
        {
            SIMPLE_LOG_MSG(LL_ERROR,
                           "raft_server_log_file_setup_init_header(): %s",
                           strerror(-rc));
            return rc;
        }
    }

    rc = raft_server_header_load(ri);

    DBG_RAFT_INSTANCE(LL_WARN, ri, "raft_server_header_load() rc=%d", rc);

    return rc;
}

static int
raft_server_log_file_close(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    else if (ri->ri_log_fd < 0)
        return 0;

    int rc = close(ri->ri_log_fd);
    ri->ri_log_fd = -1;

    return rc;
};

static void
raft_server_print_help(const int error, char **argv)
{
    fprintf(error ? stderr : stdout,
            "Usage: %s -p peer_num [-L log-prefix]\n", argv[0]);

    exit(error);
}

static void
raft_server_getopt(int argc, char **argv)
{
    int opt;

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'p':
            myRaft.ri_peerno = atoll(optarg);
            if (myRaft.ri_peerno < 0 || myRaft.ri_peerno > MAX_PEERS)
            {
                fprintf(stderr, "peer_num exceeds MAX_PEERS (%d)\n",
                        MAX_PEERS);
                raft_server_print_help(EINVAL, argv);
            }
            break;
        case 'h':
            raft_server_print_help(0, argv);
            break;
        case 'L':
            if (strnlen(optarg, PATH_MAX) >=
                (PATH_MAX - RAFT_LOG_SUFFIX_MAX_LEN))
            {
                fprintf(stderr, "log file name is too long\n");
                raft_server_print_help(EINVAL, argv);
            }
            myRaft.ri_init_log_path = 0;
            strncpy(myRaft.ri_log, optarg, PATH_MAX - RAFT_LOG_SUFFIX_MAX_LEN);
            break;
        default:
            raft_server_print_help(EINVAL, argv);
            break;
        }
    }

    if (myRaft.ri_peerno == ID_ANY_8bit)
        raft_server_print_help(EINVAL, argv);
}

static int
raft_server_udp_socket_setup(struct raft_instance *ri)
{
    ri->ri_ush.ush_port += ri->ri_peerno;

    int rc = udp_socket_setup(&ri->ri_ush);

    return rc;
}

static int
raft_server_udp_socket_close(struct raft_instance *ri)
{
    return udp_socket_close(&ri->ri_ush);
}

static int
raft_server_timerfd_create(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    ri->ri_timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (ri->ri_timer_fd < 0)
        return -errno;

    return 0;
}

static int
raft_server_timerfd_close(struct raft_instance *ri)
{
    if (!ri)
	return -EINVAL;

    if (ri->ri_timer_fd >= 0)
    {
        ri->ri_timer_fd = -1;
        return close(ri->ri_timer_fd);
    }

    return 0;
}

static void
raft_server_timerfd_cb(const struct epoll_handle *eph)
{
    struct raft_instance *ri = eph->eph_arg;

    DBG_RAFT_INSTANCE(LL_WARN, ri, "");

    size_t val;
    while (read(ri->ri_timer_fd, &val, sizeof(size_t)) > 0)
        ;

#if 0
    struct itimerspec its = {.it_interval.tv_sec = 1,
                             .it_interval.tv_nsec = 0,
                             .it_value.tv_sec = 1,
                             .it_value.tv_nsec = 0};

    timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);
#endif
}

static int
raft_epoll_setup(struct raft_instance *ri)
{
    if (!ri)
        return -EINVAL;

    int rc = epoll_mgr_setup(&ri->ri_epoll_mgr);
    if (rc)
        return rc;

    rc = epoll_handle_init(&ri->ri_epoll_handles[RAFT_EPOLL_HANDLE_TIMERFD],
                           ri->ri_timer_fd, EPOLLIN, raft_server_timerfd_cb,
                           ri);
    if (rc)
        goto error;

    rc = epoll_handle_add(&ri->ri_epoll_mgr,
                          &ri->ri_epoll_handles[RAFT_EPOLL_HANDLE_TIMERFD]);
    if (rc)
        goto error;

    return 0;

error:
    epoll_mgr_close(&ri->ri_epoll_mgr);
    return rc;
}

static int
raft_main_loop(struct raft_instance *ri)
{
    struct itimerspec its = {.it_interval.tv_sec = 1,
                             .it_interval.tv_nsec = 0,
                             .it_value.tv_sec = 1,
                             .it_value.tv_nsec = 0};

    timerfd_settime(ri->ri_timer_fd, 0, &its, NULL);

    while (1)
        epoll_mgr_wait_and_process_events(&ri->ri_epoll_mgr, -1);

    return 0;
}

int
main(int argc, char **argv)
{
    int udp_close_rc = 0, file_close_rc = 0;

    raft_server_getopt(argc, argv);

    int rc = raft_server_udp_socket_setup(&myRaft);
    if (rc)
        exit(rc);

    rc = raft_server_log_file_setup(&myRaft);
    if (rc)
        goto udp_close;

    rc = raft_server_timerfd_create(&myRaft);
    if (rc)
        goto file_close;

    raft_epoll_setup(&myRaft);
    raft_main_loop(&myRaft);

    rc = raft_server_timerfd_close(&myRaft);

file_close:
    file_close_rc = raft_server_log_file_close(&myRaft);

udp_close:
    udp_close_rc = raft_server_udp_socket_close(&myRaft);

    exit(rc || file_close_rc || udp_close_rc);
}

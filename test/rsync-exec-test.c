#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#define __USE_GNU
#include <sys/uio.h>
#undef __USE_GNU
#include <regex.h>


#include "niova/io.h"
#include "niova/log.h"
#include "niova/regex_defines.h"
#include "niova/util.h"

char *rsyncBin = "/usr/bin/rsync";
char *rsyncArgsDryRun[] = {
    NULL,  "-a", "--dry-run", "--info=stats2", "/var/tmp", "/tmp/foo", NULL,
};

char *rsyncArgsDryRun2[] = {
    NULL, "-a", "--dry-run", "--info=progress2", "/var/tmp", "/tmp/foo", NULL,
};

const char *rsyncEnv[] = { NULL };

#define BUF_SZ 32768

const char *totalFileSizeRegex = "Total file size: "COMMA_DELIMITED_UNSIGNED_INTEGER_BASE" bytes";

const char *totalXferFileSizeRegex =
    "Total transferred file size: "COMMA_DELIMITED_UNSIGNED_INTEGER_BASE" bytes";

const char *progressRegex = "[\r][ \t]*"COMMA_DELIMITED_UNSIGNED_INTEGER_BASE"[ \t]*[0-9]\\{1,3\\}%[ \t]*[0-9]\\+.[0-9][0-9][KMGT]B/s";

struct status_value_extract
{
    const char         *sve_regex_str;
    unsigned long long  sve_val;
    int                 sve_err;
    bool                sve_continuous_parse;
};

static int
rsync_parent_parse(const char *buf, struct status_value_extract *sve)
{
    regex_t regex;
    int rc = regcomp(&regex, sve->sve_regex_str, 0);
    if (rc)
        return -1;

    regmatch_t match = {0};

    rc = regexec(&regex, buf, 1, &match, 0);

    SIMPLE_LOG_MSG(LL_DEBUG, "here");
    SIMPLE_LOG_MSG(LL_DEBUG, "rc=%d match @offset=%d:%d `%s'",
                   rc, match.rm_so, match.rm_eo, buf);
    if (rc)
        return rc;

    // Sanity check the starting and end match values
    if (match.rm_eo <= match.rm_so)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rc=%d match @offset=%d:%d",
                       rc, match.rm_so, match.rm_eo);
        return -EINVAL;
    }

    size_t match_len = match.rm_eo - match.rm_so;

    sve->sve_err = niova_parse_comma_delimited_uint_string(
        &buf[match.rm_so], match_len, &sve->sve_val);

    SIMPLE_LOG_MSG(LL_WARN, "sve_err=%d byte-cnt=%lld",
                   sve->sve_err, sve->sve_val);

    return 0;
}

static int
rsync_parent(pid_t rsync_pid, int infd, struct status_value_extract *sve,
             int sve_cnt)
{
    int err = 0;
    int wstatus = 0;
    bool child_exited = false;
    char buf[BUF_SZ + 1] = {0};
    size_t off = 0;

    int rc = io_fd_nonblocking(infd);
    if (rc < 0)
    {
        err = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "io_fd_nonblocking(): %s", strerror(-err));
        return err;
    }

    do
    {
        usleep(10);

        ssize_t rrc = read(infd, &buf[off], BUF_SZ - off);
        if (rrc < 0 && errno != EAGAIN)
        {
            rrc = 0;
            err = -errno;
        }
        else
        {
            SIMPLE_LOG_MSG(LL_WARN, "rrc=%zd off=%zu", rrc, off);

            off = (off + rrc) % BUF_SZ;

            for (int i = 0; i < sve_cnt; i++)
                if (sve[i].sve_continuous_parse)
                {
                    rsync_parent_parse(&buf[off], &sve[i]);
                    off = 0;
                }

            pid_t res = waitpid(rsync_pid, &wstatus, WNOHANG);
            SIMPLE_LOG_MSG(LL_DEBUG, "pid=%d res=%d", rsync_pid, res);
            if (res < 0)
                err = -errno;
            else if (res == rsync_pid)
                child_exited = true;
        }

    } while (!err && !child_exited);

    if (err)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "error: %s", strerror(-err));
    }
    else
    {
        SIMPLE_LOG_MSG(LL_WARN, "----\n%s\n----", buf);
        for (int i = 0; i < sve_cnt; i++)
            if (!sve[i].sve_continuous_parse)
                rsync_parent_parse(buf, &sve[i]);
    }

    return err;
}

static int
rsync_child(char *rsync_args[], int outfd)
{
    int rc = dup2(outfd, STDOUT_FILENO);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "dup2(): %s", strerror(-rc));
        return rc;
    }

    rc = execve("/usr/bin/rsync", (char * const *)rsync_args,
                (char * const *)rsyncEnv);

    return rc;
}

static int
rsync_exec(char *rsync_args[], struct status_value_extract *sve, int sve_cnt)
{

    int pipefds[2];
    int rc = pipe(pipefds);
    if (rc < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "pipe(): %s", strerror(-rc));
        return rc;
    }

    pid_t pid = fork();
    if (pid < 0)
    {
        rc = -errno;
        SIMPLE_LOG_MSG(LL_ERROR, "fork(): %s", strerror(-rc));
        return rc;
    }

    close(pid ? pipefds[1] : pipefds[0]);

    return pid
        ? rsync_parent(pid, pipefds[0], sve, sve_cnt)
        : rsync_child(rsync_args, pipefds[1]);
}

int
main(void)
{
    struct status_value_extract prep_phase[2] = {
        { .sve_regex_str = totalFileSizeRegex },
        { .sve_regex_str = totalXferFileSizeRegex }
    };

    struct status_value_extract run_phase =
        { .sve_regex_str = progressRegex, .sve_continuous_parse = true };

    rsyncArgsDryRun[0] = rsyncBin;
    rsyncArgsDryRun2[0] = rsyncBin;

    int rc = rsync_exec(rsyncArgsDryRun, prep_phase, 2);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsync_exec(): %s", strerror(-rc));
        return rc;
    }

    rc = rsync_exec(rsyncArgsDryRun2, &run_phase, 1);
    if (rc)
    {
        SIMPLE_LOG_MSG(LL_ERROR, "rsync_exec(): %s", strerror(-rc));
        return rc;
    }

    return rc;
}

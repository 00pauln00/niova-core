noinst_LTLIBRARIES =
noinst_PROGRAMS =
TESTS =

ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -Isrc/include -Isrc/contrib/include
AM_CFLAGS = -O2 -g

noinst_LTLIBRARIES += src/libniova-core.la
src_libniova_core_la_SOURCES = \
	src/include/alloc.h \
        src/include/binary_hist.h \
        src/include/common.h \
        src/include/config_token.h \
	src/include/ctl_interface_cmd.h \
        src/include/ctl_interface.h \
        src/include/ctl_svc.h \
	src/include/env.h \
        src/include/epoll_mgr.h \
	src/include/ev_pipe.h \
	src/include/fault_inject.h \
        src/include/file_util.h \
        src/include/io.h \
	src/include/init.h \
	src/include/lock.h \
        src/include/log.h \
	src/include/net_ctl.h \
	src/include/raft.h \
        src/include/raft_net.h \
        src/include/random.h \
        src/include/ref_tree_proto.h \
        src/include/regex_defines.h \
	src/include/registry.h \
        src/include/system_info.h \
        src/include/thread.h \
        src/include/udp.h \
        src/include/tcp.h \
        src/include/tcp_mgr.h \
        src/include/util.h \
        src/include/util_thread.h \
        src/include/watchdog.h \
        src/alloc.c \
        src/config_token.c \
        src/contrib/crc32c-pcl-intel-asm_64.S \
        src/ctl_interface.c \
        src/ctl_interface_cmd.c \
        src/ctl_svc.c \
        src/env.c \
        src/epoll_mgr.c \
	src/ev_pipe.c \
        src/fault_inject.c \
        src/file_util.c \
        src/init.c \
        src/io.c \
        src/log.c \
	src/random.c \
        src/raft_net.c \
        src/registry.c \
	src/system_info.c \
        src/thread.c \
	src/udp.c \
	src/tcp.c \
        src/tcp_mgr.c \
        src/util_thread.c \
	src/watchdog.c

noinst_PROGRAMS += test/ref_tree_test
test_ref_tree_test_SOURCES = test/ref_tree_test.c
test_ref_tree_test_LDADD = src/libniova-core.la
TESTS += test/ref_tree_test

noinst_PROGRAMS += test/ev_pipe_test
test_ev_pipe_test_SOURCES = test/ev_pipe_test.c
test_ev_pipe_test_LDADD = src/libniova-core.la
TESTS += test/ev_pipe_test

noinst_PROGRAMS += test/work_dispatch_test
test_work_dispatch_test_SOURCES = test/work_dispatch_test.c
test_work_dispatch_test_LDADD = src/libniova-core.la
TESTS += test/work_dispatch_test

noinst_PROGRAMS += test/binary_hist_test
test_binary_hist_test_SOURCES = test/binary_hist_test.c
test_binary_hist_test_LDADD = src/libniova-core.la
TESTS += test/binary_hist_test

noinst_PROGRAMS += test/common_test
test_common_test_SOURCES = test/common_test.c
test_common_test_LDADD = src/libniova-core.la
TESTS += test/common_test

noinst_PROGRAMS += test/micro_test
test_micro_test_SOURCES = test/micro_test.c
test_micro_test_LDADD = src/libniova-core.la
TESTS += test/micro_test

noinst_PROGRAMS += test/registry_test
test_registry_test_SOURCES = test/registry_test.c
test_registry_test_LDADD = src/libniova-core.la
TESTS += test/registry_test

noinst_PROGRAMS += test/config_token_test
test_config_token_test_SOURCES = test/config_token_test.c
test_config_token_test_LDADD = src/libniova-core.la
TESTS += test/config_token_test

noinst_PROGRAMS += test/tcp_test
test_tcp_test_SOURCES = test/tcp_test.c
test_tcp_test_LDADD = src/libniova-core.la
TESTS += test/tcp_test

noinst_PROGRAMS += test/udp_test
test_udp_test_SOURCES = test/udp_test.c
test_udp_test_LDADD = src/libniova-core.la
TESTS += test/udp_test

noinst_PROGRAMS += test/random-test
test_random_test_SOURCES = test/random-test.c
test_random_test_LDADD = src/libniova-core.la
TESTS += test/random-test

noinst_PROGRAMS += test/queue_test
test_queue_test_SOURCES = test/queue_test.c
test_queue_test_LDADD = src/libniova-core.la
TESTS += test/queue_test

noinst_PROGRAMS += test/rocksdb-test
test_rocksdb_test_SOURCES = test/rocksdb-test.c
test_rocksdb_test_LDADD = src/libniova-core.la
TESTS += test/rocksdb-test

noinst_PROGRAMS += test/util-test
test_util_test_SOURCES = test/util-test.c
test_util_test_LDADD = src/libniova-core.la
TESTS += test/util-test

noinst_PROGRAMS += test/regex-test
test_regex_test_SOURCES = test/regex-tests.c
test_regex_test_LDADD = src/libniova-core.la
TESTS += test/regex-test

noinst_PROGRAMS += test/raft_net_test
test_raft_net_test_SOURCES = test/raft_net_test.c
test_raft_net_test_LDADD = src/libniova-core.la
TESTS += test/raft_net_test

noinst_PROGRAMS += test/epoll_mgr_test
test_epoll_mgr_test_SOURCES = test/epoll_mgr_test.c
test_epoll_mgr_test_LDADD = src/libniova-core.la
TESTS += test/epoll_mgr_test


autofmt:
	uncrustify -c tools/uncrustify.cfg --no-backup `find . -name "*.[ch]"` | tee /dev/null

autofmt-check:
	uncrustify -c tools/uncrustify.cfg -q --check `find . -name "*.[ch]"` | grep -c FAIL | grep ^0$ > /dev/null

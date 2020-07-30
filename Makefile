CC		= gcc
INCLUDE 	= -Isrc/include -Isrc/contrib/include
DEBUG_CFLAGS 	= -Wall -g -O0 $(INCLUDE)
#DEBUG_CFLAGS 	= -Wall -g -O2 $(INCLUDE)
COVERAGE_FLAGS  = -Wall -g -O0 -fprofile-arcs -ftest-coverage --coverage $(INCLUDE)
DEF_CFLAGS	= -O2 -Wall $(INCLUDE)
CFLAGS 		= $(DEF_CFLAGS)
LDFLAGS		= -lpthread -laio -luuid -lssl -lcrypto
NIOVA_LCOV      = niova-lcov

SYS_CORE_INCLUDES = \
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
	src/include/util.h \
	src/include/util_thread.h \
	src/include/watchdog.h

SYS_CORE_OBJFILES = \
	src/alloc.o \
	src/config_token.o \
	src/contrib/crc32c-pcl-intel-asm_64.o \
	src/ctl_interface.o \
	src/ctl_interface_cmd.o \
	src/ctl_svc.o \
	src/env.o \
	src/epoll_mgr.o \
	src/ev_pipe.o \
	src/fault_inject.o \
	src/file_util.o \
	src/init.o \
	src/io.o \
	src/log.o \
	src/random.o \
	src/registry.o \
	src/system_info.o \
	src/thread.o \
	src/udp.o \
	src/util_thread.o \
	src/watchdog.o

CORE_INCLUDES   = \
	src/include/chunk_handle.h \
	src/include/metablock_digest.h \
	src/include/niosd_io.h \
	src/include/niosd_uuid.h \
	src/include/niosd_io_stats.h \
	src/include/vblkdev_handle.h

CORE_OBJFILES   = \
	src/chunk_handle.o \
	src/metablock_digest.o \
	src/niosd_io.o \
	src/niosd_uuid.o \
	src/niosd_io_stats.o \
	src/superblock.o \
	src/vblkdev_handle.o

RAFT_OBJFILES	= \
	src/raft_server.o	\
	src/raft_net.o	\
	src/raft_server_backend_posix.o \
	src/raft_server_backend_rocksdb.o

PUMICEDB_OBJFILES = $(RAFT_OBJFILES)	\
	src/pumice_db.o

PUMICEDB_CLIENT_OBJFILES = src/raft_net.o src/raft_client.o \
	src/pumice_db_client.o

ALL_CORE_OBJFILES = $(SYS_CORE_OBJFILES) $(CORE_OBJFILES)
ALL_INCLUDES      = $(CORE_INCLUDES) $(SYS_CORE_INCLUDES)
ALL_OBJFILES      = src/niova.o $(ALL_CORE_OBJFILES)
TARGET		  = niova

CTL_OBJFILES = src/niova-ctl.o $(ALL_CORE_OBJFILES)
CTL_TARGET = niova-ctl

all: $(TARGET) $(CTL_TARGET)


$(CTL_TARGET): $(CTL_OBJFILES) $(ALL_INCLUDES)
	$(CC) $(CFLAGS) -o $(CTL_TARGET) $(CTL_OBJFILES) $(INCLUDE) $(LDFLAGS)

$(TARGET): $(ALL_OBJFILES) $(ALL_INCLUDES)
	$(CC) $(CFLAGS) -o $(TARGET) $(ALL_OBJFILES) $(INCLUDE) $(LDFLAGS)


tests: $(ALL_CORE_OBJFILES) $(ALL_INCLUDES)
	$(CC) $(CFLAGS) -o test/simple_test test/simple_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/ref_tree_test test/ref_tree_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/niosd_io_test test/niosd_io_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/work_dispatch_test \
		test/work_dispatch_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/binary_hist_test \
		test/binary_hist_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/common_test \
		test/common_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/micro_test \
		test/micro_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/registry_test \
		test/registry_test.c \
		$(SYS_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/config_token_test \
		test/config_token_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/udp_test \
		test/udp_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/random-test \
		test/random-test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/queue_test \
		test/queue_test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/rocksdb-test \
		test/rocksdb-test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS) -lrocksdb
	$(CC) $(CFLAGS) -o test/util-test \
		test/util-test.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)
	$(CC) $(CFLAGS) -o test/regex-test \
		test/regex-tests.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)

raft: $(ALL_CORE_OBJFILES) $(RAFT_OBJFILES) $(ALL_INCLUDES)
	$(CC) $(CFLAGS) -o raft-server test/raft_server_test.c \
		$(ALL_CORE_OBJFILES) $(RAFT_OBJFILES) $(INCLUDE) $(LDFLAGS) \
		-lrocksdb
	$(CC) $(CFLAGS) -o raft-client test/raft_client_test.c \
		$(ALL_CORE_OBJFILES) $(RAFT_OBJFILES) $(INCLUDE) $(LDFLAGS) \
		-lrocksdb

raft-dbg: CFLAGS = $(DEBUG_CFLAGS) -DNIOVA_FAULT_INJECTION_ENABLED \
	-fsanitize=address
raft-dbg: raft

# PumiceDB
pumicedb-common: $(ALL_CORE_OBJFILES) $(PUMICEDB_OBJFILES) \
	$(PUMICEDB_CLIENT_OBJFILES) $(ALL_INCLUDES)
	$(CC) -shared -Wl,-soname,libpumicedb.so.1 -o libpumicedb.so.1.0.1 \
		$(ALL_CORE_OBJFILES) $(PUMICEDB_OBJFILES) -lc
	$(CC) -shared -Wl,-soname,libpumicedb_client.so.1 \
		-o libpumicedb_client.so.1.0.1 $(ALL_CORE_OBJFILES) \
		$(PUMICEDB_CLIENT_OBJFILES) -lc
	ln -sf libpumicedb.so.1.0.1 libpumicedb.so
	ln -sf libpumicedb.so.1.0.1 libpumicedb.so.1
	ln -sf libpumicedb_client.so.1.0.1 libpumicedb_client.so
	ln -sf libpumicedb_client.so.1.0.1 libpumicedb_client.so.1
	$(CC) $(PMDB_CFLAGS) -o pumicedb-server-test \
		test/pumice_db_test_server.c -lrocksdb -L. -lpumicedb \
	$(LDFLAGS)
	$(CC) $(PMDB_CFLAGS) -o pumicedb-client-test \
		test/pumice_db_test_client.c -L. -lpumicedb_client $(LDFLAGS)

pumicedb: CFLAGS = $(DEF_CFLAGS) -fPIC -c
pumicedb: PMDB_CFLAGS = $(DEF_CFLAGS)
pumicedb: pumicedb-common

pumicedb-dbg: CFLAGS = $(DEBUG_CFLAGS) -fPIC -c \
	-DNIOVA_FAULT_INJECTION_ENABLED -fsanitize=address
pumicedb-dbg: PMDB_CFLAGS = $(DEBUG_CFLAGS) -fsanitize=address
pumicedb-dbg: pumicedb-common

raft-cg: CFLAGS = $(DEBUG_CFLAGS) -fdump-rtl-expand
raft-cg: $(ALL_CORE_OBJFILES) $(RAFT_OBJFILES) $(ALL_INCLUDES)
	$(CC) $(CFLAGS) -o raft-server test/raft_server_test.c \
	$(ALL_CORE_OBJFILES) $(RAFT_OBJFILES) $(INCLUDE) $(LDFLAGS) -lrocksdb

test_build: tests
test_build:
	mkdir -p /tmp/.niova
	test/regex-test
	test/config_token_test
	test/micro_test
	test/binary_hist_test
	test/simple_test
	test/ref_tree_test
	test/udp_test
	test/niosd_io_test -t 1
	test/work_dispatch_test
	test/util-test
	taskset -c 0   test/work_dispatch_test
	taskset -c 0,1 test/work_dispatch_test
	test/rocksdb-test

check: test_build

check-noopt: CFLAGS = $(DEBUG_CFLAGS) -fsanitize=address
check-noopt: test_build

asan: CFLAGS = $(DEBUG_CFLAGS) -fsanitize=address -fsanitize=leak
asan: tests
asan: niova
asan: niova-ctl

asan-check: CFLAGS = $(DEBUG_CFLAGS) -fsanitize=address -fsanitize=leak
asan-check: check

debug: CFLAGS = $(DEBUG_CFLAGS)
debug: tests
debug: niova
debug: niova-ctl

cov : CFLAGS = $(COVERAGE_FLAGS)
cov : test_build
cov : $(TARGET)
	./niova
	lcov --no-external -b . --capture --directory . --output-file \
		$(NIOVA_LCOV).out
	genhtml ./niova-lcov.out --output-directory ./$(NIOVA_LCOV)

client-test: private CFLAGS = $(DEBUG_CFLAGS)
client-test: $(ALL_CORE_OBJFILES)
	$(CC) $(DEBUG_CFLAGS) -o test/client_mmap test/client_mmap.c \
		$(ALL_CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)

pahole: CFLAGS = $(DEBUG_CFLAGS)
pahole : tests
	pahole test/niosd_io_test

clean :
	rm -fv test/simple_test test/niosd_io_test test/ref_test_test \
	$(ALL_OBJFILES) $(CTL_OBJFILES) $(TARGET) $(CTL_TARGET) \
	$(RAFT_OBJFILES) $(PUMICEDB_OBJFILES) \
	*~ src/*.gcno src/*.gcda *.gcno *.gcda *.expand src/*.expand \
	$(NIOVA_LCOV).out

clean-cov: clean
	rm -Rfv $(NIOVA_LCOV)

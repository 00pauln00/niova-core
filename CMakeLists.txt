

add_library(niova)
target_sources(niova
  PUBLIC
    src/alloc.c    
    src/buffer.c    
    src/config_token.c    
    src/contrib/crc24q.c    
    #$(ARCH_SOURCES)    
    src/contrib/crc32c_arm64.c
    src/contrib/crct10dif-ce-arm64.S
    src/contrib/dlmalloc.c    
    src/ctl_interface.c    
    src/ctl_interface_cmd.c    
    src/ctl_svc.c    
    src/env.c    
    src/epoll_mgr.c    
    src/ev_pipe.c    
    src/fault_inject.c    
    src/file_util.c    
    src/init.c    
    src/io.c    
    src/log.c    
    src/popen_cmd.c    
    src/random.c    
    src/registry.c    
    src/system_info.c    
    src/thread.c    
    src/udp.c    
    src/tcp.c    
    src/tcp_mgr.c    
    src/util_thread.c    
    src/watchdog.c

  PUBLIC
    FILE_SET myHeaders
    TYPE HEADERS
    BASE_DIRS
      src/include
    FILES
      # include/library_header.h
)
#AM_CFLAGS += -march=armv8-a+crc+crypto
#AM_CCASFLAGS = -march=armv8-a+aes+crc+crypto
target_compile_options(niova
  PRIVATE -march=armv8-a+crc+crypto
)

add_library(niova_bt)
target_sources(niova_bt
  PUBLIC
    src/niova_backtrace.c  

  PUBLIC
    FILE_SET myHeaders
    TYPE HEADERS
    BASE_DIRS
      src/include
    FILES
      # include/library_header.h
)
target_compile_options(niova_bt
  PRIVATE -march=armv8-a+crc+crypto
)


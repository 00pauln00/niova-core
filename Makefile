CC		= gcc
INCLUDE 	= -Isrc/include -Isrc/contrib/include
DEBUG_CFLAGS 	= -Wall -g -O0 $(INCLUDE)
CFLAGS 		= -O2 -Wall $(INCLUDE)
LDFLAGS		= -lpthread

CORE_INCLUDES   = \
	src/include/lock.h \
	src/include/log.h \
	src/include/random.h \
	src/include/vblkdev_handle.h

CORE_OBJFILES   = \
	src/log.o \
	src/random.o \
	src/vblkdev_handle.o

ALL_OBJFILES    = src/niova.o $(CORE_OBJFILES)
TARGET 		= niova

all: $(TARGET)

$(TARGET): $(ALL_OBJFILES) $(CORE_INCLUDES)
	$(CC) $(CFLAGS) -o $(TARGET) $(ALL_OBJFILES) $(INCLUDE) $(LDFLAGS)

check: private CFLAGS = $(DEBUG_CFLAGS)
check: $(CORE_OBJFILES)
	$(CC) $(DEBUG_CFLAGS) -o test/simple_test test/simple_test.c \
		$(CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)

client-test: private CFLAGS = $(DEBUG_CFLAGS)
client-test: $(CORE_OBJFILES)
	$(CC) $(DEBUG_CFLAGS) -o test/client_mmap test/client_mmap.c \
		$(CORE_OBJFILES) $(INCLUDE) $(LDFLAGS)

pahole : check
	pahole test/simple_test

clean :
	rm -fv test/simple_test $(ALL_OBJFILES) $(TARGET) *~

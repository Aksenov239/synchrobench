#VERSION = TCMALLOC_NO_UPDATE

MALLOC=TC

ROOT = ../../..

include $(ROOT)/common/Makefile.common

.PHONY:	all clean
all:	main

#BINS = $(BINDIR)/lockfree-bst
BINS = bronson-bst

CC = g++
CFLAGS += -std=gnu++0x

#LDFLAGS += -L~/setbench/lib/ 
LDFLAGS += -I ~/setbench/ds/bronson_pext_bst_occ/ `find ~/setbench/common -type d | sed s/^/-I/`

main: test.cpp
	$(CC) $(CFLAGS) $(LDFLAGS) test.cpp -o $(BINS)
example: minimal_example.cpp
	$(CC) $(CFLAGS) $(LDFLAGS) minimal_example.cpp -o minimal_example
clean:
	-rm -f $(BINS)

ROOT = ../../..
MALLOC=TC

include $(ROOT)/common/Makefile.common

BINS = $(BINDIR)/$(LOCK)-lazy-list-v2
CFLAGS += -std=c++14

.PHONY:	all clean

all:	main

linkedlist-lock.o: 
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/linkedlist-lock.o linkedlist-lock.c

lazy.o: linkedlist-lock.h linkedlist-lock.o
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/lazy.o lazy.c	 

intset.o: linkedlist-lock.h lazy.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/intset.o intset.c

test.o: linkedlist-lock.h lazy.h intset.h
	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test.o test.c

main: linkedlist-lock.o lazy.o intset.o test.o
	$(CC) $(CFLAGS) $(BUILDIR)/linkedlist-lock.o $(BUILDIR)/lazy.o $(BUILDIR)/intset.o $(BUILDIR)/test.o -o $(BINS) $(LDFLAGS)

clean:
	rm -f $(BINS)

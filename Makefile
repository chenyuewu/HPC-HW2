SRCS = $(wildcard *.c)
PROGS = $(patsubst %.c,%,$(SRCS))
CC = mpicc
CFLAGS = -O3

all: $(PROGS)

%: %.c
	$(CC) $(CFLAGS)  -o $@ $<

clean:
	rm -rf $(PROGS)


STD= -std=c99
WARN= -Wall
OPT= -Os

R_CFLAGS= $(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS)
R_LDFLAGS= $(LDFLAGS)
DEBUG= -g

R_CC=$(CC) $(R_CFLAGS)

AR= ar
ARFLAGS= rcs

libobmm_ownership.a: obmm_ownership.o
	$(AR) $(ARFLAGS) $@ $+

obmm_ownership.o: obmm_ownership.h obmm_ownership.c

.c.o:
	$(R_CC) -c $<

clean:
	rm -f *.o
	rm -f *.a

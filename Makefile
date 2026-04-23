# Top level makefile, the real stuff is at ./src/Makefile and in ./modules/Makefile

SUBDIRS = src
ifeq ($(BUILD_WITH_MODULES), yes)
	ifeq ($(MAKECMDGOALS),32bit)
    	$(error BUILD_WITH_MODULES=yes is not supported on 32 bit systems)
	endif
	SUBDIRS += modules
endif

default: all

# Delegate TLC server targets to src/Makefile
tlc-all:
	$(MAKE) -C src tlc-all

tlc-clean:
	$(MAKE) -C src tlc-clean

# When TLC_VERSION is set, delegate to tlc_single in src/
ifdef TLC_VERSION
all:
	$(MAKE) -C src tlc_single
endif

# When TLC_VERSIONS is set, delegate to tlc_subset in src/
ifdef TLC_VERSIONS
all:
	$(MAKE) -C src tlc_subset
endif

.DEFAULT:
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@; done

install:
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@; done

.PHONY: install tlc-all tlc-clean

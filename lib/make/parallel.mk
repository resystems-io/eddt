# lib/make/parallel.mk — Enable parallel builds scaled to CPU count
#
# Include before the first target so MAKEFLAGS is set before any rules run:
#   include ../../lib/make/parallel.mk
#
# Effect: sets -j<nproc> for Linux/macOS; leaves MAKEFLAGS unchanged on
# other platforms or when -j is already present (e.g. `make -j1`).

ifeq (,$(filter -j%,$(MAKEFLAGS)))
    OS := $(shell uname -s)
    ifeq ($(OS),Linux)
        MAKEFLAGS += -j$(shell nproc)
    endif
    ifeq ($(OS),Darwin)
        MAKEFLAGS += -j$(shell sysctl -n hw.ncpu)
    endif
endif

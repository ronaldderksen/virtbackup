CC ?= gcc
CFLAGS ?= -O2 -fPIC
LDFLAGS ?=
LIBS ?= -lssh2 -lcrypto

PKG_CONFIG ?= pkg-config
HAVE_PKG_CONFIG := $(shell command -v $(PKG_CONFIG) >/dev/null 2>&1 && echo yes)
ifeq ($(HAVE_PKG_CONFIG),yes)
SSH2_CFLAGS := $(shell $(PKG_CONFIG) --cflags libssh2 2>/dev/null)
SSH2_LIBS := $(shell $(PKG_CONFIG) --libs libssh2 2>/dev/null)
endif

BREW_PREFIX := $(shell brew --prefix libssh2 2>/dev/null)
ifneq ($(BREW_PREFIX),)
BREW_CFLAGS := -I$(BREW_PREFIX)/include
BREW_LDFLAGS := -L$(BREW_PREFIX)/lib
endif


UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
OUT_DIR := macos
OUT := $(OUT_DIR)/libvirtbackup_native.dylib
SHARED_FLAG := -dynamiclib
SSH2_STATIC :=
else
OUT_DIR := linux
OUT := $(OUT_DIR)/libvirtbackup_native.so
SHARED_FLAG := -shared
SSH2_STATIC :=
endif

SRC := virtbackup_native.c

.PHONY: all clean

all: $(OUT)

$(OUT): $(SRC)
	mkdir -p $(OUT_DIR)
	$(CC) $(CFLAGS) $(SSH2_CFLAGS) $(BREW_CFLAGS) $(SHARED_FLAG) -o $@ $^ \
		$(LDFLAGS) $(BREW_LDFLAGS) $(SSH2_LIBS) $(LIBS)

clean:
ifeq ($(UNAME_S),Darwin)
	rm -f macos/libvirtbackup_native.dylib
	rmdir macos 2>/dev/null || true
else
	rm -f linux/libvirtbackup_native.so
	rmdir linux 2>/dev/null || true
endif

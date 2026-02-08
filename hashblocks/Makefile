CC ?= gcc
CFLAGS ?= -O3 -Wall -Wextra -std=c11
TARGET = hashblocks
SRC = hashblocks.c

STATIC ?= 0
ifeq ($(STATIC),1)
  LDFLAGS ?= -static
endif
LIBS = -lcrypto

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean

.PHONY: all check build clean

GO             := /usr/bin/go
RM             := /usr/bin/rm
MKDIR          := /usr/bin/mkdir
CP             := /usr/bin/cp
INSTALL        := /usr/bin/install
CLANG          := /usr/bin/clang-15
STRIP          := /usr/bin/llvm-strip-15
MAKE           := /usr/bin/make
PROTO_DIR      := proto
FRONTEND_DIR   := frontend
OUTPUT_DIR     := ./output/
SRC_CONFIG_DIR := ./config/
DST_CONFIG_DIR := $(HOME)/config/
INSTALL_BIN    := /usr/bin/install -m 755
DST_BIN_DIR    := /usr/sbin/
CFLAGS         := -O2 -g -Wall -Werror $(CFLAGS)

all: clean build

check:
	$(GO) vet ./...

generate: export BPF_CLANG := $(CLANG)
generate: export BPF_CFLAGS := $(CFLAGS)
generate:
	$(GO) generate ./backend/ebpf/...

build: generate
	$(MAKE) -C $(PROTO_DIR) build
	$(GO) build -o $(OUTPUT_DIR) ./...
	$(MAKE) -C $(FRONTEND_DIR) build

install: install_bin install_ui

install_bin:
	$(INSTALL_BIN) $(OUTPUT_DIR)* $(DST_BIN_DIR)
	$(MKDIR) -p $(DST_CONFIG_DIR)
	$(CP) -r $(SRC_CONFIG_DIR)* $(DST_CONFIG_DIR)

install_ui:
	$(MAKE) -C $(FRONTEND_DIR) install

clean:
	$(GO) clean
	$(RM) -rf $(OUTPUT_DIR)
	$(RM) -f ./backend/ebpf/*/bpf_bpfeb.go
	$(RM) -f ./backend/ebpf/*/bpf_bpfeb.o
	$(RM) -f ./backend/ebpf/*/bpf_bpfel.go
	$(RM) -f ./backend/ebpf/*/bpf_bpfel.o
	$(MAKE) -C $(PROTO_DIR) clean
	$(MAKE) -C $(FRONTEND_DIR) clean

# Build the Codex Guard plugin for Linux/macOS.
# Requires Go 1.26+, CGO enabled, and a C compiler (gcc/clang).

GOOS   ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

ifeq ($(GOOS),darwin)
	OUT = codex-guard.dylib
else
	OUT = codex-guard.so
endif

.PHONY: build clean

build:
	CGO_ENABLED=1 go build -buildmode=c-shared -o $(OUT) .

clean:
	rm -f codex-guard.so codex-guard.dylib

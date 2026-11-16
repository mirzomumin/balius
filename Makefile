# tulpar build.
#
# The worker embeds CPython, so the binary is built against one interpreter:
# PYTHON selects which (its `-config` script must be available, as shipped by
# python3-dev / python3.X-dev packages or by source installs).
#
#   make                        # system python3
#   make PYTHON=python3.14      # a specific version
#
# Static libpython builds get `--export-dynamic`, so that the interpreter's
# own C extension modules can resolve its symbols from the executable.

PYTHON      ?= python3
PYCONFIG    := $(shell command -v $(PYTHON)-config 2>/dev/null)
BIN         := bin/tulpar
GO          ?= go

ifeq ($(PYCONFIG),)
$(error $(PYTHON)-config not found: install the Python development headers or set PYTHON)
endif

PY_CFLAGS   := $(shell $(PYCONFIG) --includes)
PY_LIBS     := $(shell $(PYCONFIG) --embed --ldflags)
PY_LIBDIR   := $(shell $(PYTHON) -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
PY_LDLIB    := $(shell $(PYTHON) -c 'import sysconfig; print(sysconfig.get_config_var("LDLIBRARY"))')
ifeq ($(suffix $(PY_LDLIB)),.a)
PY_LIBS     += -Wl,--export-dynamic
else
# find the shared library at runtime without LD_LIBRARY_PATH
PY_LIBS     += -Wl,-rpath,$(PY_LIBDIR)
endif

# passed through the environment: the go build cache keys on them
export CGO_CFLAGS  := $(PY_CFLAGS) -O2
export CGO_LDFLAGS := $(PY_LIBS)
GOFLAGS_TAGS := -tags tulpar_pyflags

.PHONY: all build race test test-unit test-integration vet lint clean

all: build

build:
	$(GO) build $(GOFLAGS_TAGS) -trimpath -o $(BIN) .

race:
	$(GO) build $(GOFLAGS_TAGS) -race -o $(BIN)-race .

vet:
	$(GO) vet $(GOFLAGS_TAGS) ./...

test-unit:
	$(GO) test $(GOFLAGS_TAGS) ./internal/...

test-integration: build
	TULPAR_BIN=$(abspath $(BIN)) TULPAR_PYTHON=$(PYTHON) $(GO) test $(GOFLAGS_TAGS) -count=1 -v ./tests/...

test: test-unit test-integration

lint:
	pre-commit run --all-files

clean:
	rm -rf bin

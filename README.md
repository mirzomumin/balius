# tulpar

A Go HTTP server for Python applications: a port of
[granian](https://github.com/emmett-framework/granian) from Rust to Go.

tulpar serves **ASGI 3**, **RSGI 1.6** and **WSGI** applications over
HTTP/1.1 and HTTP/2 (TLS and cleartext), with websockets, static files,
multiple worker processes and the same command line as granian (options are
also readable from `TULPAR_*` environment variables).

```
$ make
$ ./bin/tulpar --interface asgi --workers 4 main:app
```

## How granian works, and how tulpar maps it

granian is three layers, from the bottom:

| Layer | granian (Rust + Python) | tulpar (Go + C) |
| --- | --- | --- |
| Network | tokio runtime(s), hyper HTTP/1+2, rustls, tungstenite | goroutines, `net/http` (HTTP/1, HTTP/2, h2c), `crypto/tls`, coder/websocket |
| Python bridge | pyo3 extension: protocol objects, Rust futures exposed as asyncio awaitables, blocking thread pool holding the GIL | CPython **embedded** in the worker via cgo; C glue (`internal/worker/glue*.c`) defines the `_tulpar` module and its protocol objects |
| Process management | Python: `granian.server` spawns workers with `multiprocessing`, respawns, reloads | Go: `internal/supervisor` spawns copies of its own executable as workers |

The inversion is the main design difference. granian is a Python package
that loads a Rust extension; tulpar is a Go program that loads Python. As a
result the supervisor never starts an interpreter at all, and a worker is the
same binary started with `TULPAR_WORKER=1` and its configuration on an
inherited pipe.

### Worker anatomy

```
                 goroutines (net/http)                      process main thread
  accept ─▶ backpressure ─▶ HTTP/1, HTTP/2, TLS ─▶ handler ──▶ ┌───────────────────┐
                                    ▲                          │ asyncio event loop │  ASGI / RSGI
                                    │   loop bridge (pipe fd)  │  app coroutines    │
                                    └──── futures resolved ◀── └───────────────────┘
                                         on the loop thread
                                                               ┌───────────────────┐
                                    handler ─────────────────▶ │ Python thread pool │  WSGI
                                                               └───────────────────┘
```

* **Async interfaces.** The asyncio loop runs on the process main thread.
  Goroutines never touch Python: they queue closures on a *loop bridge*,
  which writes one byte to a pipe watched with `loop.add_reader`. Everything
  queued between two loop iterations runs in a single drain, so only the
  loop thread ever takes the GIL. Awaitables are ready-made objects when
  the answer is known (`tulpar_done`/`tulpar_empty`), or asyncio futures
  resolved from the bridge otherwise. This is granian's
  `future_into_py_futlike`, without a second thread contending for the GIL.
* **WSGI.** A pool of OS threads, each with its own Python thread state.
  Tasks take the GIL, run the app and release it within **one** cgo call, so
  the GIL is never held while a thread waits for a Go scheduler slot. The
  pool grows only while requests wait *and* no thread is queued for the GIL.
  Threads help when the app blocks on I/O, never when it is CPU-bound. This
  keeps a fast app at a couple dozen threads where granian's rule would
  start one per concurrent request.
* **The C boundary.** Request heads cross it as one flat buffer (one cgo
  call per request whatever the header count); response headers come back
  the same way. Python objects hold an opaque, generation-checked handle on
  the Go state, never Go pointers.

### Correspondence table

| granian | tulpar |
| --- | --- |
| `src/workers.rs` accept loop, semaphore backpressure | `internal/netx` (`Backpressure` listener) |
| `src/serve.rs` mt/st runtimes | Go scheduler; `--runtime-threads` sizes `GOMAXPROCS` |
| `src/callbacks.rs` `CallbackScheduler`, `PyFutureAwaitable` | `internal/worker/py.go` loop bridge, `glue.c` awaitables |
| `src/blocking.rs` `BlockingRunnerPool` | `internal/worker/pool.go` |
| `src/asgi/*` | `asgi.go`, `asgi_ws.go`, `glue_asgi.c`, `pysrc/tulpar/asgi.py` |
| `src/rsgi/*` | `rsgi.go`, `rsgi_ws.go`, `glue_rsgi.c`, `pysrc/tulpar/rsgi.py` |
| `src/wsgi/*` | `wsgi.go`, `glue_wsgi.c`, `pysrc/tulpar/wsgi.py` |
| `src/files.rs` static files | `internal/static` |
| `src/tls.rs` (rustls, encrypted PKCS#8) | `internal/tlsx` (incl. a PBES2 PKCS#8 decrypter) |
| `src/metrics.rs`, `src/ipc.rs` | `internal/metrics`, JSON snapshots over a pipe |
| `granian/server/common.py`, `mp.py` | `internal/supervisor` |
| `granian/cli.py` | `internal/cli` |

## Building

The worker embeds CPython, so a build targets one interpreter (tested with
CPython 3.12 and 3.14; the C glue needs 3.10 or later):

```
make                              # system python3 (needs python3-dev)
make PYTHON=python3.14            # any interpreter with a `-config` script
go build .                        # plain build, uses pkg-config `python3-embed`
```

Static `libpython` builds are handled too (`--export-dynamic` lets the
interpreter's C extensions resolve its symbols).

**Virtualenvs.** tulpar adopts `$VIRTUAL_ENV` when it is set, or the
environment of the interpreter given with `--python /path/to/venv/bin/python`.
The venv must be built from the same Python version tulpar was built against.

## Differences from granian

* Options that configure tokio/hyper internals are accepted for
  compatibility but have no effect: `--runtime-mode`,
  `--runtime-blocking-threads`, `--http1-pipeline-flush`,
  `--http2-adaptive-window`, `--http2-max-send-buffer-size`, `--task-impl`.
* `--reload` uses fsnotify; it does not need the `watchfiles` extra.
  `--env-files` doesn't need `python-dotenv`.
* A subprotocol chosen by the application in `websocket.accept` is only
  confirmed to the client when the client offered it (RFC 6455: a client
  must fail the connection on any other value).
* On an ASGI websocket the client already closed (or left before the
  handshake was accepted), `send` raises `OSError`, as ASGI 2.4 specifies
  (granian silently ignores it, which lets a producer loop spin forever).
  `websocket.send` before `websocket.accept` raises instead of being dropped.
* WSGI responses follow PEP 3333 for generators: the headers go out with the
  first non-empty chunk, so `start_response` may be called lazily. A
  response whose iterable fails mid-body is cut off (connection dropped)
  instead of ending like a complete one; the same goes for an RSGI request
  body that breaks off while the application iterates over it.
* The `TULPAR_*` settings (e.g. the TLS key password) are not visible to the
  application: workers get their configuration from the supervisor directly.
* Metrics are prefixed `tulpar_`; the Python modules live in the `tulpar`
  package (`from tulpar.rsgi import ...`, `tulpar.utils.proxies`).
* Not ported: free-threaded Python, Windows, the embeddable server
  (`granian.server.embed`), and the Python API (`Granian(...).serve()`).

## Testing

```
make test              # unit tests + end-to-end tests against the built binary
make race              # race-detector build: bin/tulpar-race
```

The end-to-end suite (`tests/`) runs granian's test applications through the
real binary and ports granian's assertions: ASGI/RSGI/WSGI scopes and
bodies, websockets, TLS 1.2/1.3 and encrypted keys, static files and path
traversal, Unix sockets, multiple workers, metrics, respawn and CLI errors.

Set `TULPAR_CPUPROFILE=/path/prefix` to write a Go CPU profile per worker.

## Development

One code style per language, enforced by [pre-commit](https://pre-commit.com)
hooks: golangci-lint (with gofumpt and goimports, `.golangci.yml`), ruff for
Python (`ruff.toml`) and clang-format for the C glue (`.clang-format`). The
hook versions are pinned in `.pre-commit-config.yaml`; none of them is a
dependency of tulpar itself.

```
pre-commit install --hook-type pre-commit --hook-type pre-push
make lint              # every hook, over the whole tree
```

The pre-push hook also runs the unit tests.

## Performance

Measured on one 12-core machine (load generator on the same host, 64
concurrent keep-alive connections, one worker, defaults on both sides,
granian 2.8.8, CPython 3.12). Requests per second:

| | ASGI | RSGI | WSGI | WSGI, app sleeps 10 ms |
| --- | --- | --- | --- | --- |
| granian, small response | 31,957 | 36,692 | 34,499 | 5,875 |
| tulpar, small response | **41,193** | **42,884** | 32,185 | 5,690 |
| granian, echo (0–300 KB bodies) | 5,072 | **7,031** | **7,852** | |
| tulpar, echo (0–300 KB bodies) | **6,241** | 5,522 | 5,295 | |

Large request bodies are copied once more than necessary on the RSGI and
WSGI paths; that's the main remaining gap.

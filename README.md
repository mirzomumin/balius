# balius

A Go HTTP server for Python applications: a port of
[granian](https://github.com/emmett-framework/granian) from Rust to Go.

balius serves **ASGI 3**, **RSGI 1.6** and **WSGI** applications over
HTTP/1.1 and HTTP/2 (TLS and cleartext), with websockets, static files,
multiple worker processes and the same command line as granian (options are
also readable from `BALIUS_*` environment variables).

```
$ make
$ ./bin/balius --interface asgi --workers 4 main:app
```

## How granian works, and how balius maps it

granian is three layers, from the bottom:

| Layer | granian (Rust + Python) | balius (Go + C) |
| --- | --- | --- |
| Network | tokio runtime(s), hyper HTTP/1+2, rustls, tungstenite | goroutines, `net/http` (HTTP/1, HTTP/2, h2c), `crypto/tls`, coder/websocket |
| Python bridge | pyo3 extension: protocol objects, Rust futures exposed as asyncio awaitables, blocking thread pool holding the GIL | CPython **embedded** in the worker via cgo; C glue (`internal/worker/glue*.c`) defines the `_balius` module and its protocol objects |
| Process management | Python: `granian.server` spawns workers with `multiprocessing`, respawns, reloads | Go: `internal/supervisor` spawns copies of its own executable as workers |

The inversion is the main design difference. granian is a Python package
that loads a Rust extension; balius is a Go program that loads Python. As a
result the supervisor never starts an interpreter at all, and a worker is the
same binary started with `BALIUS_WORKER=1` and its configuration on an
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
  the answer is known (`balius_done`/`balius_empty`), or asyncio futures
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

| granian | balius |
| --- | --- |
| `src/workers.rs` accept loop, semaphore backpressure | `internal/netx` (`Backpressure` listener) |
| `src/serve.rs` mt/st runtimes | Go scheduler; `--runtime-threads` sets `GOMAXPROCS` |
| `src/callbacks.rs` `CallbackScheduler`, `PyFutureAwaitable` | `internal/worker/py.go` loop bridge, `glue.c` awaitables |
| `src/blocking.rs` `BlockingRunnerPool` | `internal/worker/pool.go` |
| `src/asgi/*` | `asgi.go`, `asgi_ws.go`, `glue_asgi.c`, `pysrc/balius/asgi.py` |
| `src/rsgi/*` | `rsgi.go`, `rsgi_ws.go`, `glue_rsgi.c`, `pysrc/balius/rsgi.py` |
| `src/wsgi/*` | `wsgi.go`, `glue_wsgi.c`, `pysrc/balius/wsgi.py` |
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

**Virtualenvs.** balius adopts `$VIRTUAL_ENV` when it is set, or the
environment of the interpreter given with `--python /path/to/venv/bin/python`.
The venv must be built from the same Python version balius was built against.

## Beyond granian: uvicorn parity

Behaviours uvicorn users rely on, with uvicorn's defaults:

| Option | Default | |
| --- | --- | --- |
| `--keep-alive-timeout` | `5` | Idle keep-alive connections are closed, so they can't hold the `--backpressure` slots forever |
| `--ws-ping-interval`, `--ws-ping-timeout` | `20`, `20` | Websocket keepalive: a peer that doesn't answer a ping in time is closed with 1011. `0` disables |
| `--ws-max-size` | `16777216` | Larger websocket messages close the connection with 1009 |
| `--ws-per-message-deflate` | on | permessage-deflate, when the client offers it |
| `--proxy-headers`, `--forwarded-allow-ips` | on, `127.0.0.1,::1` | `X-Forwarded-For` / `X-Forwarded-Proto` from trusted proxies set the client address and scheme, for all three interfaces. Addresses, networks, unix socket paths, or `*` |
| `--workers-max-requests`, `--workers-max-requests-jitter` | off | A worker is gracefully replaced after serving that many requests (plus a random per-worker jitter) |
| `--workers-healthcheck-timeout` | off | A worker whose interpreter gives no sign of life for this long is killed and replaced |

The health check is a heartbeat sent after taking the GIL from a dedicated
thread: a busy or blocked event loop still beats, an interpreter that is
stuck (a C extension holding the GIL, a deadlock) doesn't. It is opt-in
because legitimately long GIL-holding calls would otherwise get workers
killed; pick a timeout well above the longest one you expect.

The supervisor exports `balius_workers_respawns_for_requests` and
`balius_workers_respawns_for_health` next to the other respawn counters.

HTTP/1.0 requests are reported as `"1.0"` in the ASGI scope and `HTTP/1.0`
in WSGI's `SERVER_PROTOCOL` (RSGI keeps granian's `"1"`).

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
* The `BALIUS_*` settings (e.g. the TLS key password) are not visible to the
  application: workers get their configuration from the supervisor directly.
* Metrics are prefixed `balius_`; the Python modules live in the `balius`
  package (`from balius.rsgi import ...`, `balius.utils.proxies`).
* Not ported: free-threaded Python, Windows, the embeddable server
  (`granian.server.embed`), and the Python API (`Granian(...).serve()`).

## Testing

```
make test              # unit tests + end-to-end tests against the built binary
make race              # race-detector build: bin/balius-race
```

The end-to-end suite (`tests/`) runs granian's test applications through the
real binary and ports granian's assertions: ASGI/RSGI/WSGI scopes and
bodies, websockets, TLS 1.2/1.3 and encrypted keys, static files and path
traversal, Unix sockets, multiple workers, metrics, respawn and CLI errors.
`tests/parity_test.go` covers the uvicorn-parity behaviours above.

Set `BALIUS_CPUPROFILE=/path/prefix` to write a Go CPU profile per worker.

## Development

One code style per language, enforced by [pre-commit](https://pre-commit.com)
hooks: golangci-lint (with gofumpt and goimports, `.golangci.yml`), ruff for
Python (`ruff.toml`) and clang-format for the C glue (`.clang-format`). The
hook versions are pinned in `.pre-commit-config.yaml`; none of them is a
dependency of balius itself.

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
| balius, small response | **41,193** | **42,884** | 32,185 | 5,690 |
| granian, echo (0–300 KB bodies) | 5,072 | **7,031** | **7,852** | |
| balius, echo (0–300 KB bodies) | **6,241** | 5,522 | 5,295 | |

Those echo numbers predate the zero-copy body paths: ASGI and RSGI read a
request body of known length straight into the `bytes` object the
application gets (ASGI in events of up to 1 MiB), and write a large `bytes`
(or RSGI `str`) response from where it is instead of copying it, which
roughly doubled their echo throughput. WSGI bodies are still copied.

**Throughput per core.** A worker runs Python on one thread and the Go side
of every request (parsing, I/O, websocket framing) on others, so by default
it uses more than one core: `--runtime-threads` sets how many threads run Go
code (4 by default for the async interfaces). Fewer threads serve less per
worker but more per core; `--runtime-threads 1` keeps a worker on a single
core, like a uvicorn worker. With uvicorn 0.54 (uvloop, httptools) on the
same machine, one worker each, 64 connections:

| | GET rps | cores | websocket echo rps | cores |
| --- | --- | --- | --- | --- |
| uvicorn | 22,238 | 1.0 | 39,263 | 1.0 |
| balius `--runtime-threads 1` | 19,135 | 1.0 | 37,242 | 0.9 |
| balius `--runtime-threads 2` | 31,547 | 1.9 | 65,397 | 1.9 |
| balius (default) | 43,917 | 3.1 | 103,429 | 3.4 |

Websocket sends and receives that can complete on the spot don't suspend
the application (a message already received, a send with room in the
outgoing queue), and frames queued together go out in one write.

/*
 * balius - C glue between the Go runtime and the embedded CPython interpreter.
 *
 * Everything Python can see (the `_balius` builtin module and its types) is
 * defined on the C side; everything that does real work lives in Go and is
 * reached through the exported `go*` functions declared in _cgo_export.h.
 *
 * Unless stated otherwise every function here must be called with the GIL
 * held.
 */
#ifndef BALIUS_GLUE_H
#define BALIUS_GLUE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>

/* Error kinds accepted by balius_raise() and balius_future_fail(). */
enum {
    GC_ERR_RUNTIME = 0,
    GC_ERR_RSGI_PROTO = 1,
    GC_ERR_RSGI_CLOSED = 2,
    GC_ERR_STOP_ASYNC_ITER = 4,
    GC_ERR_OS = 5,
};

/* Protocol kinds accepted by the dispatch functions. */
enum {
    GC_KIND_HTTP = 0,
    GC_KIND_WS = 1,
};

/* Flags of the serialized request head (see head.go). */
enum {
    GC_HEAD_AUTHORITY = 0x01, /* the request carries an authority */
    GC_HEAD_UDS = 0x02,       /* addresses are unix socket paths */
};

/* RSGI response kinds (goRsgiResponse). */
enum {
    RK_EMPTY,
    RK_BYTES,
    RK_STR,
    RK_FILE,
    RK_FILE_RANGE,
    RK_STREAM,
};

/* WSGI request body fill modes (goWsgiBodyFill). */
enum {
    BODY_ALL,
    BODY_SIZE,
    BODY_LINE,
};

/* ---- interpreter lifecycle ------------------------------------------- */

/* Initializes the interpreter on the calling thread, which becomes the
 * Python main thread and holds the GIL on return. Returns 0 on success;
 * on failure a message is copied in errbuf. */
int balius_py_init(const char *executable, char *errbuf, size_t errlen);

/* Registers the source of an embedded module (before the importer install). */
int balius_py_add_source(const char *name, const char *src, Py_ssize_t len, int is_pkg);

/* Runs the bootstrap code that installs the embedded modules importer. */
int balius_py_bootstrap(const char *src);

/* Imports `balius._worker` and runs `main(cfg_json)`. Returns the exit code. */
int balius_py_run_worker(const char *cfg_json);

/* Finalizes the interpreter. */
int balius_py_finalize(void);

/* ---- generic helpers -------------------------------------------------- */

void balius_incref(PyObject *o);
void balius_decref(PyObject *o);

PyObject *balius_none(void);
PyObject *balius_bytes(const char *p, Py_ssize_t n);
/* A bytes object of n bytes left to fill: *data points at its contents,
 * which may be written without the GIL while nothing else references it. */
PyObject *balius_bytes_alloc(Py_ssize_t n, char **data);
/* Shrinks a bytes object built with balius_bytes_alloc (reference stolen). */
PyObject *balius_bytes_shrink(PyObject *b, Py_ssize_t n);

/* Sets a Python exception of the given kind; always returns NULL. */
PyObject *balius_raise(int kind, const char *msg);

/* Awaitables that complete without suspending the coroutine. */
PyObject *balius_empty(void);           /* -> None  */
PyObject *balius_done(PyObject *value); /* -> value (reference stolen) */

/* asyncio futures bound to the worker's event loop.
 * balius_future_new returns a future carrying TWO references: one to hand
 * to Python as the return value, one owned by the caller and consumed by
 * balius_future_set / balius_future_fail. */
PyObject *balius_future_new(void);
void balius_future_set(PyObject *fut, PyObject *value /* stolen */);
void balius_future_fail(PyObject *fut, int kind, const char *msg);

/* Invokes the Python callback that stops the worker's event loop. */
void balius_call_stop(void);

/* ---- ASGI ------------------------------------------------------------- */

void balius_asgi_dispatch(int kind, uintptr_t h, const char *head, Py_ssize_t n);

PyObject *balius_asgi_msg_request(const char *body, Py_ssize_t n, int more);
PyObject *balius_asgi_msg_request_obj(PyObject *body /* stolen */, int more);
PyObject *balius_asgi_msg_disconnect(void);
PyObject *balius_asgi_msg_ws_connect(void);
PyObject *balius_asgi_msg_ws_text(const char *p, Py_ssize_t n);
PyObject *balius_asgi_msg_ws_bytes(const char *p, Py_ssize_t n);
PyObject *balius_asgi_msg_ws_disconnect(int code);

/* ---- RSGI ------------------------------------------------------------- */

void balius_rsgi_dispatch(int kind, uintptr_t h, const char *head, Py_ssize_t n);

/* Builds the websocket transport object for an accepted connection. */
PyObject *balius_rsgi_ws_transport(PyObject *proto);
/* kind: 0 close, 1 bytes, 2 str */
PyObject *balius_rsgi_ws_message(int kind, const char *p, Py_ssize_t n);

/* ---- WSGI ------------------------------------------------------------- */

/* Builds the environ and runs the application callback.
 * Returns 0 on success, 1 when the callback raised (already logged). */
int balius_wsgi_call(uintptr_t h, const char *head, Py_ssize_t n);

/* Runs balius_wsgi_call on a pool thread that does NOT hold the GIL: the
 * GIL is acquired with `tstate`, and released again before returning, all
 * within this one call (the GIL is never held across a return to Go).
 * The time spent waiting for the GIL is stored in *wait_us. */
int balius_wsgi_run(PyThreadState *tstate, uintptr_t h, const char *head, Py_ssize_t n, int64_t *wait_us);

/* Number of pool threads queued for the GIL. Callable without the GIL. */
int balius_gil_waiters(void);

#endif /* BALIUS_GLUE_H */

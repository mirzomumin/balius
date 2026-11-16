/*
 * balius - C glue: interpreter lifecycle, the `_balius` builtin module,
 * shared helpers and the awaitable types.
 */
#include "glue_internal.h"

/* ---- globals ----------------------------------------------------------- */

PyObject *g_create_task = NULL;
PyObject *g_create_future = NULL;
PyObject *g_wcallback = NULL;
PyObject *g_stop_cb = NULL;
PyObject *g_wsgi_callback = NULL;
PyObject *g_log_exc = NULL;
PyObject *g_rsgi_headers_cls = NULL;
PyObject *g_empty = NULL;
PyObject *g_sources = NULL;
PyObject *GcExc_RSGIProtocolError = NULL;
PyObject *GcExc_RSGIProtocolClosed = NULL;

#define X(name, lit) PyObject *S_##name = NULL;
GC_STRINGS(X)
#undef X

/* ---- head cursor ------------------------------------------------------- */

int gc_head_parse(gc_head *hd, const char *buf, Py_ssize_t n) {
    gc_cur c = {(const unsigned char *)buf, (const unsigned char *)buf + n};
    if (n < 12) {
        return -1;
    }
    hd->http_version = c.p[0];
    hd->tls = c.p[1];
    hd->flags = c.p[2];
    c.p += 4;
    hd->server_port = gc_rd_u32(&c);
    hd->client_port = gc_rd_u32(&c);
    if (gc_rd_str(&c, &hd->server_ip) || gc_rd_str(&c, &hd->client_ip) || gc_rd_str(&c, &hd->method) ||
        gc_rd_str(&c, &hd->path) || gc_rd_str(&c, &hd->raw_path) || gc_rd_str(&c, &hd->query) ||
        gc_rd_str(&c, &hd->authority)) {
        return -1;
    }
    hd->rest = c;
    return 0;
}

PyObject *gc_http_version(const gc_head *hd) {
    switch (hd->http_version) {
    case 1:
        return S_v11;
    case 2:
        return S_v2;
    default:
        return S_v1;
    }
}

/* ---- header serialization ---------------------------------------------- */

static int hbuf_reserve(gc_hbuf *b, Py_ssize_t extra) {
    if (b->len + extra <= b->cap) {
        return 0;
    }
    Py_ssize_t ncap = b->cap * 2;
    while (ncap < b->len + extra) {
        ncap *= 2;
    }
    char *nbuf;
    if (b->buf == b->stack) {
        nbuf = PyMem_RawMalloc((size_t)ncap);
        if (nbuf) {
            memcpy(nbuf, b->stack, (size_t)b->len);
        }
    } else {
        nbuf = PyMem_RawRealloc(b->buf, (size_t)ncap);
    }
    if (!nbuf) {
        PyErr_NoMemory();
        return -1;
    }
    b->buf = nbuf;
    b->cap = ncap;
    return 0;
}

void gc_hbuf_init(gc_hbuf *b) {
    b->buf = b->stack;
    b->len = 0;
    b->cap = (Py_ssize_t)sizeof(b->stack);
    b->n = 0;
}

void gc_hbuf_free(gc_hbuf *b) {
    if (b->buf != b->stack) {
        PyMem_RawFree(b->buf);
    }
    b->buf = b->stack;
}

static int hbuf_put(gc_hbuf *b, const char *p, Py_ssize_t n) {
    if (n > 0x7fffffff || hbuf_reserve(b, n + 4)) {
        return -1;
    }
    uint32_t l = (uint32_t)n;
    memcpy(b->buf + b->len, &l, 4);
    memcpy(b->buf + b->len + 4, p, (size_t)n);
    b->len += n + 4;
    return 0;
}

/* bytes-like (bytes or bytearray) -> pointer/len without copying */
static int as_bytes(PyObject *o, const char **p, Py_ssize_t *n) {
    if (PyBytes_Check(o)) {
        *p = PyBytes_AS_STRING(o);
        *n = PyBytes_GET_SIZE(o);
        return 0;
    }
    if (PyByteArray_Check(o)) {
        *p = PyByteArray_AS_STRING(o);
        *n = PyByteArray_GET_SIZE(o);
        return 0;
    }
    return -1;
}

/* Serializes an iterable of 2-item iterables. `text` selects str items
 * (RSGI/WSGI) instead of bytes items (ASGI). Returns 0, or -1 with either a
 * Python exception set or none (meaning "malformed"). */
int gc_hbuf_headers(gc_hbuf *b, PyObject *headers, int text) {
    if (headers == NULL || headers == Py_None) {
        return 0;
    }
    PyObject *seq = PySequence_Fast(headers, "headers must be an iterable");
    if (!seq) {
        return -1;
    }
    Py_ssize_t count = PySequence_Fast_GET_SIZE(seq);
    for (Py_ssize_t i = 0; i < count; i++) {
        PyObject *pair = PySequence_Fast(PySequence_Fast_GET_ITEM(seq, i), "header must be a pair");
        if (!pair) {
            Py_DECREF(seq);
            return -1;
        }
        if (PySequence_Fast_GET_SIZE(pair) != 2) {
            Py_DECREF(pair);
            Py_DECREF(seq);
            return -1;
        }
        for (int j = 0; j < 2; j++) {
            PyObject *item = PySequence_Fast_GET_ITEM(pair, j);
            const char *p;
            Py_ssize_t n;
            int bad = 0;
            if (text) {
                if (!PyUnicode_Check(item)) {
                    PyErr_SetString(PyExc_TypeError, "header names and values must be str");
                    bad = 1;
                } else if (!(p = PyUnicode_AsUTF8AndSize(item, &n))) {
                    bad = 1;
                }
            } else if (as_bytes(item, &p, &n)) {
                bad = 1;
            }
            if (bad || hbuf_put(b, p, n)) {
                Py_DECREF(pair);
                Py_DECREF(seq);
                return -1;
            }
        }
        b->n++;
        Py_DECREF(pair);
    }
    Py_DECREF(seq);
    return 0;
}

/* ---- generic helpers --------------------------------------------------- */

void balius_incref(PyObject *o) {
    Py_XINCREF(o);
}
void balius_decref(PyObject *o) {
    Py_XDECREF(o);
}

PyObject *balius_none(void) {
    Py_RETURN_NONE;
}
PyObject *balius_bytes(const char *p, Py_ssize_t n) {
    return PyBytes_FromStringAndSize(p, n);
}

static PyObject *exc_type(int kind) {
    switch (kind) {
    case GC_ERR_RSGI_PROTO:
        return GcExc_RSGIProtocolError;
    case GC_ERR_RSGI_CLOSED:
        return GcExc_RSGIProtocolClosed;
    case GC_ERR_STOP_ASYNC_ITER:
        return PyExc_StopAsyncIteration;
    case GC_ERR_OS:
        return PyExc_OSError;
    default:
        return PyExc_RuntimeError;
    }
}

PyObject *balius_raise(int kind, const char *msg) {
    PyErr_SetString(exc_type(kind), msg);
    return NULL;
}

/* Reports the current exception without unwinding. */
void gc_report_exc(const char *where) {
    if (!PyErr_Occurred()) {
        return;
    }
    PySys_WriteStderr("balius: unexpected error in %s\n", where);
    PyErr_Print();
}

/* Takes the current exception as a single (normalized) object. */
PyObject *gc_take_exc(void) {
#if PY_VERSION_HEX >= 0x030c0000
    return PyErr_GetRaisedException();
#else
    PyObject *t, *v, *tb;
    PyErr_Fetch(&t, &v, &tb);
    PyErr_NormalizeException(&t, &v, &tb);
    if (tb && v) {
        PyException_SetTraceback(v, tb);
    }
    Py_XDECREF(t);
    Py_XDECREF(tb);
    return v;
#endif
}

void gc_display_exc(PyObject *exc) {
    PyObject *tb = PyException_GetTraceback(exc);
    PyErr_Display((PyObject *)Py_TYPE(exc), exc, tb);
    Py_XDECREF(tb);
}

/* Logs an application exception through the Python-side logger. */
void gc_log_app_exc(void) {
    PyObject *exc = gc_take_exc();
    if (!exc) {
        return;
    }
    if (g_log_exc) {
        PyObject *r = PyObject_CallOneArg(g_log_exc, exc);
        if (!r) {
            PyErr_Print();
        }
        Py_XDECREF(r);
    } else {
        gc_display_exc(exc);
    }
    Py_DECREF(exc);
}

/* ---- awaitables -------------------------------------------------------- */

typedef struct {
    PyObject_HEAD PyObject *value; /* NULL means None */
} ReadyObject;

static void ready_dealloc(ReadyObject *self) {
    Py_XDECREF(self->value);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *ready_self(PyObject *self) {
    return Py_NewRef(self);
}

static PyObject *ready_next(ReadyObject *self) {
    if (self->value && self->value != Py_None) {
        PyObject *e = PyObject_CallOneArg(PyExc_StopIteration, self->value);
        if (e) {
            PyErr_SetObject(PyExc_StopIteration, e);
            Py_DECREF(e);
        }
    }
    /* NULL without an exception set means StopIteration(None) */
    return NULL;
}

static PyAsyncMethods ready_async = {
    .am_await = ready_self,
};

static PyTypeObject ReadyType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.Ready",
    .tp_basicsize = sizeof(ReadyObject),
    .tp_dealloc = (destructor)ready_dealloc,
    .tp_as_async = &ready_async,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_doc = "An awaitable that is already completed.",
    .tp_iter = ready_self,
    .tp_iternext = (iternextfunc)ready_next,
};

PyObject *balius_empty(void) {
    return Py_NewRef(g_empty);
}

PyObject *balius_done(PyObject *value) {
    if (!value) {
        return NULL;
    }
    ReadyObject *r = PyObject_New(ReadyObject, &ReadyType);
    if (!r) {
        Py_DECREF(value);
        return NULL;
    }
    r->value = value;
    return (PyObject *)r;
}

PyObject *balius_future_new(void) {
    if (!g_create_future) {
        PyErr_SetString(PyExc_RuntimeError, "balius event loop is not running");
        return NULL;
    }
    PyObject *fut = PyObject_CallNoArgs(g_create_future);
    if (fut) {
        Py_INCREF(fut);
    }
    return fut;
}

void balius_future_set(PyObject *fut, PyObject *value) {
    if (!value) {
        /* building the value failed: surface the error to the awaiter */
        PyObject *exc = gc_take_exc();
        if (exc) {
            PyObject *r = PyObject_CallMethodOneArg(fut, S_set_exception, exc);
            if (!r) {
                PyErr_Clear();
            }
            Py_XDECREF(r);
            Py_DECREF(exc);
        }
        Py_DECREF(fut);
        return;
    }
    PyObject *r = PyObject_CallMethodOneArg(fut, S_set_result, value);
    if (!r) {
        /* the future was cancelled meanwhile */
        PyErr_Clear();
    }
    Py_XDECREF(r);
    Py_DECREF(value);
    Py_DECREF(fut);
}

void balius_future_fail(PyObject *fut, int kind, const char *msg) {
    PyObject *exc = PyObject_CallFunction(exc_type(kind), "s", msg);
    if (exc) {
        PyObject *r = PyObject_CallMethodOneArg(fut, S_set_exception, exc);
        if (!r) {
            PyErr_Clear();
        }
        Py_XDECREF(r);
        Py_DECREF(exc);
    } else {
        PyErr_Clear();
    }
    Py_DECREF(fut);
}

void balius_call_stop(void) {
    if (!g_stop_cb) {
        return;
    }
    PyObject *r = PyObject_CallNoArgs(g_stop_cb);
    if (!r) {
        gc_report_exc("stop callback");
    }
    Py_XDECREF(r);
}

/* ---- protocol base object ---------------------------------------------- */

void gc_proto_dealloc(GcProto *self) {
    if (self->h) {
        goHandleRelease(self->h);
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

GcProto *gc_proto_new(PyTypeObject *type, uintptr_t h) {
    GcProto *p = PyObject_New(GcProto, type);
    if (p) {
        p->h = h;
    }
    return p;
}

GcProtoRef *gc_protoref_new(PyTypeObject *type, PyObject *proto) {
    GcProtoRef *r = PyObject_New(GcProtoRef, type);
    if (r) {
        r->proto = Py_NewRef(proto);
        r->h = ((GcProto *)proto)->h;
    }
    return r;
}

void gc_protoref_dealloc(GcProtoRef *self) {
    Py_XDECREF(self->proto);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* ---- watcher ------------------------------------------------------------ */

static void watcher_dealloc(GcWatcher *self) {
    Py_XDECREF(self->proto);
    Py_XDECREF(self->scope);
    Py_XDECREF(self->task);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *watcher_done(GcWatcher *self, PyObject *Py_UNUSED(ignored)) {
    goWatcherDone(self->h);
    Py_RETURN_NONE;
}

static PyMethodDef watcher_methods[] = {
    {"done", (PyCFunction)watcher_done, METH_NOARGS, "Signals the application callable returned."},
    {NULL},
};

static PyMemberDef watcher_members[] = {
    {"proto", T_OBJECT, offsetof(GcWatcher, proto), READONLY, NULL},
    {"scope", T_OBJECT, offsetof(GcWatcher, scope), READONLY, NULL},
    {NULL},
};

/* NOTE: deliberately not a GC type. The watcher owns the only strong
 * reference to the running asyncio task; keeping that edge invisible to the
 * cycle collector is what keeps the task alive until the coroutine ends. */
static PyTypeObject WatcherType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.CallbackWatcher",
    .tp_basicsize = sizeof(GcWatcher),
    .tp_dealloc = (destructor)watcher_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = watcher_methods,
    .tp_members = watcher_members,
};

/* Wraps (proto, scope) in a watcher and schedules the application
 * coroutine on the event loop. Steals the references to proto and scope. */
void gc_schedule(PyObject *proto, PyObject *scope, uintptr_t h) {
    GcWatcher *w = PyObject_New(GcWatcher, &WatcherType);
    if (!w) {
        Py_DECREF(proto);
        Py_DECREF(scope);
        gc_report_exc("dispatch");
        goWatcherDone(h);
        return;
    }
    w->proto = proto;
    w->scope = scope;
    w->task = NULL;
    w->h = h;

    PyObject *coro = PyObject_CallOneArg(g_wcallback, (PyObject *)w);
    if (coro) {
        w->task = PyObject_CallOneArg(g_create_task, coro);
        Py_DECREF(coro);
    }
    if (!w->task) {
        gc_report_exc("dispatch");
        goWatcherDone(h);
    }
    Py_DECREF(w);
}

/* Builds the protocol object and the scope of a request, then schedules
 * the application. On failure the Go side is told the application is done,
 * so the request still gets an answer. */
void gc_dispatch(PyTypeObject *type, gc_scope_builder build, int kind, uintptr_t h, const char *head, Py_ssize_t n) {
    GcProto *proto = gc_proto_new(type, h);
    if (!proto) {
        gc_report_exc("dispatch");
        goWatcherDone(h);
        goHandleRelease(h);
        return;
    }
    PyObject *scope = build(kind, head, n);
    if (!scope) {
        gc_report_exc("dispatch");
        goWatcherDone(h);
        Py_DECREF(proto);
        return;
    }
    gc_schedule((PyObject *)proto, scope, h);
}

/* ---- module functions --------------------------------------------------- */

static PyObject *mod_drain(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(args)) {
    goLoopDrain();
    Py_RETURN_NONE;
}

static PyObject *mod_wake_fd(PyObject *Py_UNUSED(self), PyObject *Py_UNUSED(args)) {
    return PyLong_FromLong((long)goLoopWakeFd());
}

static void set_global(PyObject **slot, PyObject *value) {
    PyObject *old = *slot;
    *slot = Py_XNewRef(value);
    Py_XDECREF(old);
}

static PyObject *mod_serve(PyObject *Py_UNUSED(self), PyObject *args) {
    PyObject *loop, *wcallback, *stop_cb, *log_exc;
    if (!PyArg_ParseTuple(args, "OOOO", &loop, &wcallback, &stop_cb, &log_exc)) {
        return NULL;
    }
    PyObject *ct = PyObject_GetAttrString(loop, "create_task");
    if (!ct) {
        return NULL;
    }
    PyObject *cf = PyObject_GetAttrString(loop, "create_future");
    if (!cf) {
        Py_DECREF(ct);
        return NULL;
    }
    set_global(&g_create_task, ct);
    set_global(&g_create_future, cf);
    Py_DECREF(ct);
    Py_DECREF(cf);
    set_global(&g_wcallback, wcallback);
    set_global(&g_stop_cb, stop_cb);
    set_global(&g_log_exc, log_exc);

    char *err = goServe();
    if (err) {
        PyErr_SetString(PyExc_RuntimeError, err);
        free(err);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *mod_serve_wsgi(PyObject *Py_UNUSED(self), PyObject *args) {
    PyObject *callback, *log_exc;
    if (!PyArg_ParseTuple(args, "OO", &callback, &log_exc)) {
        return NULL;
    }
    set_global(&g_wsgi_callback, callback);
    set_global(&g_log_exc, log_exc);

    char *err;
    Py_BEGIN_ALLOW_THREADS err = goServeWSGI();
    Py_END_ALLOW_THREADS if (err) {
        PyErr_SetString(PyExc_RuntimeError, err);
        free(err);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *mod_set_types(PyObject *Py_UNUSED(self), PyObject *args) {
    PyObject *headers_cls;
    if (!PyArg_ParseTuple(args, "O", &headers_cls)) {
        return NULL;
    }
    set_global(&g_rsgi_headers_cls, headers_cls);
    Py_RETURN_NONE;
}

static PyObject *mod_log(PyObject *Py_UNUSED(self), PyObject *args) {
    int level;
    const char *msg;
    Py_ssize_t n;
    if (!PyArg_ParseTuple(args, "is#", &level, &msg, &n)) {
        return NULL;
    }
    goLog(level, (char *)msg, n);
    Py_RETURN_NONE;
}

static PyMethodDef module_methods[] = {
    {"drain", mod_drain, METH_NOARGS, "Runs the work queued by the Go runtime for the event loop."},
    {"wake_fd", mod_wake_fd, METH_NOARGS, "File descriptor that becomes readable when work is queued."},
    {"serve", mod_serve, METH_VARARGS, "Starts serving an async (ASGI/RSGI) application."},
    {"serve_wsgi", mod_serve_wsgi, METH_VARARGS, "Serves a WSGI application until shutdown."},
    {"set_types", mod_set_types, METH_VARARGS, NULL},
    {"log", mod_log, METH_VARARGS, "Emits a message through the Go runtime logger."},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT, .m_name = "_balius",         .m_doc = "balius runtime bindings",
    .m_size = -1,          .m_methods = module_methods,
};

static PyObject *PyInit__balius(void) {
    PyObject *m = PyModule_Create(&module_def);
    if (!m) {
        return NULL;
    }

#define X(name, lit)                                                                                                   \
    if (!S_##name) {                                                                                                   \
        S_##name = PyUnicode_InternFromString(lit);                                                                    \
        if (!S_##name) {                                                                                               \
            return NULL;                                                                                               \
        }                                                                                                              \
    }
    GC_STRINGS(X)
#undef X

    if (PyType_Ready(&ReadyType) < 0 || PyType_Ready(&WatcherType) < 0) {
        return NULL;
    }
    ReadyObject *empty = PyObject_New(ReadyObject, &ReadyType);
    if (!empty) {
        return NULL;
    }
    empty->value = NULL;
    g_empty = (PyObject *)empty;

    GcExc_RSGIProtocolError = PyErr_NewException("_balius.RSGIProtocolError", PyExc_RuntimeError, NULL);
    GcExc_RSGIProtocolClosed = PyErr_NewException("_balius.RSGIProtocolClosed", PyExc_RuntimeError, NULL);
    if (!GcExc_RSGIProtocolError || !GcExc_RSGIProtocolClosed) {
        return NULL;
    }
    if (PyModule_AddObjectRef(m, "RSGIProtocolError", GcExc_RSGIProtocolError) < 0 ||
        PyModule_AddObjectRef(m, "RSGIProtocolClosed", GcExc_RSGIProtocolClosed) < 0 ||
        PyModule_AddStringConstant(m, "__version__", goVersion()) < 0) {
        return NULL;
    }
    if (gc_asgi_init(m) < 0 || gc_rsgi_init(m) < 0 || gc_wsgi_init(m) < 0) {
        return NULL;
    }
    return m;
}

/* ---- interpreter lifecycle --------------------------------------------- */

static void copy_err(char *errbuf, size_t errlen, const char *msg) {
    if (errbuf && errlen) {
        snprintf(errbuf, errlen, "%s", msg ? msg : "unknown error");
    }
}

int balius_py_init(const char *executable, char *errbuf, size_t errlen) {
    if (PyImport_AppendInittab("_balius", PyInit__balius) < 0) {
        copy_err(errbuf, errlen, "unable to register the _balius module");
        return -1;
    }

    PyConfig config;
    PyConfig_InitPythonConfig(&config);
    /* Signals are owned by the Go runtime. */
    config.install_signal_handlers = 0;
    config.parse_argv = 0;

    PyStatus status;
    if (executable && executable[0]) {
        /* Adopting an interpreter path lets getpath discover a pyvenv.cfg
         * next to it, which is what makes virtualenvs work when embedded. */
        status = PyConfig_SetBytesString(&config, &config.program_name, executable);
        if (!PyStatus_Exception(status)) {
            status = PyConfig_SetBytesString(&config, &config.executable, executable);
        }
        if (PyStatus_Exception(status)) {
            goto fail;
        }
    }
    {
        char *argv[] = {"balius"};
        status = PyConfig_SetBytesArgv(&config, 1, argv);
        if (PyStatus_Exception(status)) {
            goto fail;
        }
    }
    status = Py_InitializeFromConfig(&config);
    if (PyStatus_Exception(status)) {
        goto fail;
    }
    PyConfig_Clear(&config);

    g_sources = PyDict_New();
    if (!g_sources) {
        copy_err(errbuf, errlen, "out of memory");
        return -1;
    }
    return 0;

fail:
    copy_err(errbuf, errlen, status.err_msg);
    PyConfig_Clear(&config);
    return -1;
}

int balius_py_add_source(const char *name, const char *src, Py_ssize_t len, int is_pkg) {
    PyObject *value = Py_BuildValue("(s#O)", src, len, is_pkg ? Py_True : Py_False);
    if (!value) {
        PyErr_Print();
        return -1;
    }
    int rc = PyDict_SetItemString(g_sources, name, value);
    Py_DECREF(value);
    if (rc < 0) {
        PyErr_Print();
    }
    return rc;
}

int balius_py_bootstrap(const char *src) {
    PyObject *globals = PyDict_New();
    if (!globals) {
        PyErr_Print();
        return -1;
    }
    if (PyDict_SetItemString(globals, "__builtins__", PyEval_GetBuiltins()) < 0 ||
        PyDict_SetItemString(globals, "SOURCES", g_sources) < 0) {
        PyErr_Print();
        Py_DECREF(globals);
        return -1;
    }
    PyObject *res = PyRun_String(src, Py_file_input, globals, globals);
    Py_DECREF(globals);
    if (!res) {
        PyErr_Print();
        return -1;
    }
    Py_DECREF(res);
    return 0;
}

int balius_py_run_worker(const char *cfg_json) {
    PyObject *mod = PyImport_ImportModule("balius._worker");
    if (!mod) {
        PyErr_Print();
        return 1;
    }
    PyObject *res = PyObject_CallMethod(mod, "main", "s", cfg_json);
    Py_DECREF(mod);
    if (!res) {
        PyErr_Print();
        return 1;
    }
    long code = PyLong_AsLong(res);
    Py_DECREF(res);
    if (code == -1 && PyErr_Occurred()) {
        PyErr_Clear();
        code = 1;
    }
    return (int)code;
}

int balius_py_finalize(void) {
    return Py_FinalizeEx();
}

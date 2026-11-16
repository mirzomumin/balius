/*
 * tulpar - C glue: WSGI environ, response protocol and request body.
 *
 * WSGI runs on a pool of Python threads owned by the Go runtime. Calls that
 * may block on the network release the GIL around the Go call.
 */
#include "glue_internal.h"
#include <time.h>

/* ---- response protocol ------------------------------------------------------ */

static int pack_headers(int status, PyObject *headers, gc_hbuf *b) {
    if (status < 100 || status > 999) {
        PyErr_SetString(PyExc_ValueError, "Invalid status code");
        return -1;
    }
    gc_hbuf_init(b);
    if (gc_hbuf_headers(b, headers, 1) < 0) {
        gc_hbuf_free(b);
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "headers must be a list of (str, str) tuples");
        }
        return -1;
    }
    return 0;
}

static PyObject *wsgi_response_bytes(GcProto *self, PyObject *args) {
    int status;
    PyObject *headers;
    Py_buffer view;
    gc_hbuf b;
    if (!PyArg_ParseTuple(args, "iOy*", &status, &headers, &view)) {
        return NULL;
    }
    if (pack_headers(status, headers, &b) < 0) {
        PyBuffer_Release(&view);
        return NULL;
    }
    goWsgiResponse(self->h, status, b.buf, b.len, b.n, (char *)view.buf, view.len);
    PyBuffer_Release(&view);
    gc_hbuf_free(&b);
    Py_RETURN_NONE;
}

static void close_iter(PyObject *body) {
    PyObject *r = PyObject_CallMethodNoArgs(body, S_close);
    if (!r) {
        PyErr_Clear(); /* no close() is fine */
    }
    Py_XDECREF(r);
}

/* Queues one chunk without the GIL; non-zero when nobody listens anymore. */
static int send_chunk(GcProto *self, PyObject *item, int *failed) {
    Py_buffer view;
    if (PyObject_GetBuffer(item, &view, PyBUF_SIMPLE) < 0) {
        gc_log_app_exc(); /* not bytes: a broken response */
        *failed = 1;
        return 1;
    }
    int gone = 0;
    if (view.len > 0) {
        Py_BEGIN_ALLOW_THREADS gone = goWsgiChunk(self->h, (char *)view.buf, view.len);
        Py_END_ALLOW_THREADS
    }
    PyBuffer_Release(&view);
    return gone;
}

/* response_iter(status, headers, first, it, owner): `first` is the first
 * chunk (already pulled from `it`), `owner` the object to close. A failure
 * mid-body aborts the connection, so the client can tell it's truncated. */
static PyObject *wsgi_response_iter(GcProto *self, PyObject *args) {
    int status;
    PyObject *headers, *first, *it, *owner;
    gc_hbuf b;
    if (!PyArg_ParseTuple(args, "iOOOO", &status, &headers, &first, &it, &owner) ||
        pack_headers(status, headers, &b) < 0) {
        return NULL;
    }
    int failed = 0;
    int gone = goWsgiIterStart(self->h, status, b.buf, b.len, b.n);
    gc_hbuf_free(&b);
    if (!gone) {
        gone = send_chunk(self, first, &failed);
    }
    while (!gone) {
        PyObject *item = PyIter_Next(it);
        if (!item) {
            if (PyErr_Occurred()) {
                gc_log_app_exc();
                failed = 1;
            }
            break;
        }
        gone = send_chunk(self, item, &failed);
        Py_DECREF(item);
    }
    close_iter(owner);
    Py_BEGIN_ALLOW_THREADS goWsgiIterEnd(self->h, failed);
    Py_END_ALLOW_THREADS Py_RETURN_NONE;
}

static PyMethodDef wsgi_proto_methods[] = {
    {"response_bytes", (PyCFunction)wsgi_response_bytes, METH_VARARGS, NULL},
    {"response_iter", (PyCFunction)wsgi_response_iter, METH_VARARGS, NULL},
    {NULL},
};

static PyTypeObject WsgiProtoType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_tulpar.WSGIProtocol",
    .tp_basicsize = sizeof(GcProto),
    .tp_dealloc = (destructor)gc_proto_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = wsgi_proto_methods,
};

/* ---- request body (wsgi.input) ------------------------------------------------ */

/* Waits (without the GIL) until the requested amount is buffered on the Go
 * side, then moves it into a new bytes object. */
static PyObject *body_take(GcProtoRef *self, int mode, Py_ssize_t size) {
    Py_ssize_t n;
    Py_BEGIN_ALLOW_THREADS n = goWsgiBodyFill(self->h, mode, size);
    Py_END_ALLOW_THREADS PyObject *out = PyBytes_FromStringAndSize(NULL, n);
    if (out && n > 0) {
        goWsgiBodyTake(self->h, PyBytes_AS_STRING(out), n);
    }
    return out;
}

static int parse_size(PyObject *args, const char *name, Py_ssize_t *size, int *unbounded) {
    PyObject *psize = Py_None;
    if (!PyArg_UnpackTuple(args, name, 0, 1, &psize)) {
        return -1;
    }
    *unbounded = 1;
    *size = 0;
    if (psize != Py_None) {
        Py_ssize_t v = PyNumber_AsSsize_t(psize, PyExc_OverflowError);
        if (v == -1 && PyErr_Occurred()) {
            return -1;
        }
        if (v >= 0) {
            *unbounded = 0;
            *size = v;
        }
    }
    return 0;
}

static PyObject *body_read(GcProtoRef *self, PyObject *args) {
    Py_ssize_t size;
    int unbounded;
    if (parse_size(args, "read", &size, &unbounded) < 0) {
        return NULL;
    }
    if (unbounded) {
        return body_take(self, BODY_ALL, 0);
    }
    if (size == 0) {
        return PyBytes_FromStringAndSize(NULL, 0);
    }
    return body_take(self, BODY_SIZE, size);
}

static PyObject *body_readline(GcProtoRef *self, PyObject *args) {
    Py_ssize_t size;
    int unbounded;
    if (parse_size(args, "readline", &size, &unbounded) < 0) {
        return NULL;
    }
    return body_take(self, BODY_LINE, 0);
}

static PyObject *body_readlines(GcProtoRef *self, PyObject *args) {
    PyObject *hint = Py_None;
    if (!PyArg_UnpackTuple(args, "readlines", 0, 1, &hint)) {
        return NULL;
    }
    PyObject *data = body_take(self, BODY_ALL, 0);
    if (!data) {
        return NULL;
    }
    PyObject *lines = PyList_New(0);
    if (!lines) {
        Py_DECREF(data);
        return NULL;
    }
    const char *p = PyBytes_AS_STRING(data);
    Py_ssize_t n = PyBytes_GET_SIZE(data), start = 0;
    while (start < n) {
        const char *nl = memchr(p + start, '\n', (size_t)(n - start));
        Py_ssize_t stop = nl ? (nl - p) + 1 : n;
        PyObject *line = PyBytes_FromStringAndSize(p + start, stop - start);
        if (!line || PyList_Append(lines, line) < 0) {
            Py_XDECREF(line);
            Py_DECREF(lines);
            Py_DECREF(data);
            return NULL;
        }
        Py_DECREF(line);
        start = stop;
    }
    Py_DECREF(data);
    return lines;
}

static PyObject *body_iter(PyObject *self) {
    return Py_NewRef(self);
}

static PyObject *body_next(GcProtoRef *self) {
    PyObject *line = body_take(self, BODY_LINE, 0);
    if (line && PyBytes_GET_SIZE(line) == 0) {
        Py_DECREF(line);
        return NULL; /* StopIteration */
    }
    return line;
}

static PyMethodDef body_methods[] = {
    {"read", (PyCFunction)body_read, METH_VARARGS, NULL},
    {"readline", (PyCFunction)body_readline, METH_VARARGS, NULL},
    {"readlines", (PyCFunction)body_readlines, METH_VARARGS, NULL},
    {NULL},
};

static PyTypeObject WsgiBodyType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_tulpar.WSGIBody",
    .tp_basicsize = sizeof(GcProtoRef),
    .tp_dealloc = (destructor)gc_protoref_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_iter = body_iter,
    .tp_iternext = (iternextfunc)body_next,
    .tp_methods = body_methods,
};

/* ---- environ ------------------------------------------------------------------ */

static PyObject *build_environ(const char *buf, Py_ssize_t n, PyObject *proto) {
    gc_head hd;
    if (gc_head_parse(&hd, buf, n) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "malformed request head");
        return NULL;
    }
    PyObject *environ = PyDict_New();
    if (!environ) {
        return NULL;
    }
    PyObject *server_proto =
        hd.http_version == 2 ? S_proto_http2 : (hd.http_version == 1 ? S_proto_http11 : S_proto_http1);
    if (PyDict_SetItem(environ, S_SERVER_PROTOCOL, server_proto) < 0 ||
        PyDict_SetItem(environ, S_wsgi_url_scheme, hd.tls ? S_https : S_http) < 0) {
        goto fail;
    }
    GC_DICT_SET(environ, S_SERVER_NAME, PyUnicode_DecodeUTF8(hd.server_ip.p, hd.server_ip.n, "replace"));
    GC_DICT_SET(environ, S_SERVER_PORT, PyUnicode_FromFormat("%u", (unsigned)hd.server_port));
    GC_DICT_SET(environ, S_REMOTE_ADDR, PyUnicode_DecodeUTF8(hd.client_ip.p, hd.client_ip.n, "replace"));
    GC_DICT_SET(environ, S_REQUEST_METHOD, PyUnicode_DecodeUTF8(hd.method.p, hd.method.n, "replace"));
    /* PEP 3333: "bytes-as-unicode" strings, i.e. latin-1 decoded */
    GC_DICT_SET(environ, S_PATH_INFO, PyUnicode_DecodeLatin1(hd.path.p, hd.path.n, NULL));
    GC_DICT_SET(environ, S_QUERY_STRING, PyUnicode_DecodeLatin1(hd.query.p, hd.query.n, NULL));
    GC_DICT_SET(environ, S_wsgi_input, (PyObject *)gc_protoref_new(&WsgiBodyType, proto));

    gc_cur c = hd.rest;
    uint32_t nsub = gc_rd_u32(&c);
    for (uint32_t i = 0; i < nsub; i++) {
        gc_span skip;
        gc_rd_str(&c, &skip);
    }
    /* the Go side already produced the final CGI variable names */
    uint32_t nhdr = gc_rd_u32(&c);
    for (uint32_t i = 0; i < nhdr; i++) {
        gc_span k, v;
        gc_rd_str(&c, &k);
        gc_rd_str(&c, &v);
        PyObject *pk = PyUnicode_DecodeLatin1(k.p, k.n, NULL);
        if (!pk) {
            goto fail;
        }
        PyObject *pv = PyUnicode_DecodeLatin1(v.p, v.n, NULL);
        if (!pv) {
            Py_DECREF(pk);
            goto fail;
        }
        int rc = PyDict_SetItem(environ, pk, pv);
        Py_DECREF(pk);
        Py_DECREF(pv);
        if (rc < 0) {
            goto fail;
        }
    }
    return environ;

fail:
    Py_DECREF(environ);
    return NULL;
}

int tulpar_wsgi_call(uintptr_t h, const char *head, Py_ssize_t n) {
    GcProto *proto = gc_proto_new(&WsgiProtoType, h);
    if (!proto) {
        gc_report_exc("wsgi dispatch");
        goHandleRelease(h);
        return 1;
    }
    PyObject *environ = build_environ(head, n, (PyObject *)proto);
    if (!environ) {
        gc_report_exc("wsgi dispatch");
        proto->h = 0;
        Py_DECREF(proto);
        return 1;
    }
    PyObject *args[2] = {(PyObject *)proto, environ};
    PyObject *res = PyObject_Vectorcall(g_wsgi_callback, args, 2, NULL);
    int rc = 0;
    if (!res) {
        gc_log_app_exc();
        rc = 1;
    }
    Py_XDECREF(res);
    Py_DECREF(environ);
    /* the Go side releases the handle once this returns: spare the
     * deallocator a callback made while holding the GIL */
    proto->h = 0;
    Py_DECREF(proto);
    return rc;
}

static int64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* Pool threads currently waiting for the GIL. The pool uses it to tell
 * whether more threads would help (see pool.go). */
static _Atomic int gil_waiters = 0;

int tulpar_gil_waiters(void) {
    return gil_waiters;
}

int tulpar_wsgi_run(PyThreadState *tstate, uintptr_t h, const char *head, Py_ssize_t n, int64_t *wait_us) {
    int64_t start = now_us();
    gil_waiters++;
    PyEval_RestoreThread(tstate);
    gil_waiters--;
    *wait_us = now_us() - start;
    int rc = tulpar_wsgi_call(h, head, n);
    PyEval_SaveThread();
    return rc;
}

int gc_wsgi_init(PyObject *module) {
    if (PyType_Ready(&WsgiProtoType) < 0 || PyType_Ready(&WsgiBodyType) < 0) {
        return -1;
    }
    if (PyModule_AddObjectRef(module, "WSGIProtocol", (PyObject *)&WsgiProtoType) < 0 ||
        PyModule_AddObjectRef(module, "WSGIBody", (PyObject *)&WsgiBodyType) < 0) {
        return -1;
    }
    return 0;
}

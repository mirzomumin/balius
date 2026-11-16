/*
 * balius - C glue: RSGI scope, protocol objects and transports.
 */
#include "glue_internal.h"

/* ---- scope --------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD PyObject *proto;
    PyObject *http_version;
    PyObject *server;
    PyObject *client;
    PyObject *scheme;
    PyObject *method;
    PyObject *path;
    PyObject *query_string;
    PyObject *headers;
    PyObject *authority;
} RsgiScope;

static void scope_dealloc(RsgiScope *self) {
    Py_XDECREF(self->proto);
    Py_XDECREF(self->http_version);
    Py_XDECREF(self->server);
    Py_XDECREF(self->client);
    Py_XDECREF(self->scheme);
    Py_XDECREF(self->method);
    Py_XDECREF(self->path);
    Py_XDECREF(self->query_string);
    Py_XDECREF(self->headers);
    Py_XDECREF(self->authority);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *scope_rsgi_version(PyObject *Py_UNUSED(self), void *Py_UNUSED(closure)) {
    return Py_NewRef(S_rsgi_version);
}

static PyMemberDef scope_members[] = {
    {"proto", T_OBJECT, offsetof(RsgiScope, proto), READONLY, NULL},
    {"http_version", T_OBJECT, offsetof(RsgiScope, http_version), READONLY, NULL},
    {"server", T_OBJECT, offsetof(RsgiScope, server), READONLY, NULL},
    {"client", T_OBJECT, offsetof(RsgiScope, client), READONLY, NULL},
    {"scheme", T_OBJECT, offsetof(RsgiScope, scheme), READONLY, NULL},
    {"method", T_OBJECT, offsetof(RsgiScope, method), READONLY, NULL},
    {"path", T_OBJECT, offsetof(RsgiScope, path), READONLY, NULL},
    {"query_string", T_OBJECT, offsetof(RsgiScope, query_string), READONLY, NULL},
    {"headers", T_OBJECT, offsetof(RsgiScope, headers), READONLY, NULL},
    {"authority", T_OBJECT, offsetof(RsgiScope, authority), READONLY, NULL},
    {NULL},
};

static PyGetSetDef scope_getset[] = {
    {"rsgi_version", scope_rsgi_version, NULL, NULL, NULL},
    {NULL},
};

static PyTypeObject RsgiScopeType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.RSGIScope",
    .tp_basicsize = sizeof(RsgiScope),
    .tp_dealloc = (destructor)scope_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_members = scope_members,
    .tp_getset = scope_getset,
};

static PyObject *addr_str(gc_span ip, uint32_t port, int uds) {
    if (uds) {
        return PyUnicode_DecodeUTF8(ip.p, ip.n, "replace");
    }
    char buf[96];
    int v6 = memchr(ip.p, ':', (size_t)ip.n) != NULL;
    int n =
        snprintf(buf, sizeof(buf), v6 ? "[%.*s]:%u" : "%.*s:%u", (int)(ip.n > 64 ? 64 : ip.n), ip.p, (unsigned)port);
    return PyUnicode_FromStringAndSize(buf, n);
}

static PyObject *build_headers(gc_cur *c) {
    uint32_t nhdr = gc_rd_u32(c);
    PyObject *d = PyDict_New();
    if (!d) {
        return NULL;
    }
    for (uint32_t i = 0; i < nhdr; i++) {
        gc_span k, v;
        gc_rd_str(c, &k);
        gc_rd_str(c, &v);
        PyObject *pk = PyUnicode_DecodeLatin1(k.p, k.n, NULL);
        PyObject *pv = PyUnicode_DecodeLatin1(v.p, v.n, NULL);
        if (!pk || !pv) {
            goto item_fail;
        }
        PyObject *lst = PyDict_GetItemWithError(d, pk);
        if (lst) {
            if (PyList_Append(lst, pv) < 0) {
                goto item_fail;
            }
        } else {
            if (PyErr_Occurred()) {
                goto item_fail;
            }
            lst = PyList_New(1);
            if (!lst) {
                goto item_fail;
            }
            PyList_SET_ITEM(lst, 0, Py_NewRef(pv));
            int rc = PyDict_SetItem(d, pk, lst);
            Py_DECREF(lst);
            if (rc < 0) {
                goto item_fail;
            }
        }
        Py_DECREF(pk);
        Py_DECREF(pv);
        continue;
    item_fail:
        Py_XDECREF(pk);
        Py_XDECREF(pv);
        Py_DECREF(d);
        return NULL;
    }
    if (!g_rsgi_headers_cls) {
        return d;
    }
    PyObject *headers = PyObject_CallOneArg(g_rsgi_headers_cls, d);
    Py_DECREF(d);
    return headers;
}

static PyObject *build_scope(int kind, const char *buf, Py_ssize_t n) {
    gc_head hd;
    if (gc_head_parse(&hd, buf, n) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "malformed request head");
        return NULL;
    }
    RsgiScope *s = PyObject_New(RsgiScope, &RsgiScopeType);
    if (!s) {
        return NULL;
    }
    int uds = hd.flags & GC_HEAD_UDS;
    s->proto = Py_NewRef(kind == GC_KIND_WS ? S_ws : S_http);
    s->http_version = Py_NewRef(gc_http_version(&hd));
    s->scheme = Py_NewRef(hd.tls ? S_https : S_http);
    s->server = addr_str(hd.server_ip, hd.server_port, uds);
    s->client = addr_str(hd.client_ip, hd.client_port, uds);
    s->method = PyUnicode_DecodeUTF8(hd.method.p, hd.method.n, "replace");
    s->path = PyUnicode_DecodeUTF8(hd.path.p, hd.path.n, "replace");
    s->query_string = PyUnicode_DecodeUTF8(hd.query.p, hd.query.n, "replace");
    if (hd.flags & GC_HEAD_AUTHORITY) {
        s->authority = PyUnicode_DecodeUTF8(hd.authority.p, hd.authority.n, "replace");
    } else {
        s->authority = Py_NewRef(Py_None);
    }
    s->headers = NULL;

    gc_cur c = hd.rest;
    uint32_t nsub = gc_rd_u32(&c);
    for (uint32_t i = 0; i < nsub; i++) {
        gc_span skip;
        gc_rd_str(&c, &skip);
    }
    s->headers = build_headers(&c);

    if (!s->server || !s->client || !s->method || !s->path || !s->query_string || !s->authority || !s->headers) {
        Py_DECREF(s);
        return NULL;
    }
    return (PyObject *)s;
}

/* ---- helpers --------------------------------------------------------------- */

static PyObject *raise_proto(void) {
    PyErr_SetString(GcExc_RSGIProtocolError, "RSGI protocol error");
    return NULL;
}

/* ---- HTTP stream transport -------------------------------------------------- */

static void stream_dealloc(GcProtoRef *self) {
    /* dropping the transport is what terminates the response stream */
    goRsgiStreamClose(self->h);
    gc_protoref_dealloc(self);
}

/* Exposes the argument of send_bytes (any buffer) or send_str (str, sent
 * UTF-8 encoded) as a buffer; release it with PyBuffer_Release. */
static int payload(PyObject *arg, int text, Py_buffer *view) {
    if (!text) {
        return PyObject_GetBuffer(arg, view, PyBUF_SIMPLE);
    }
    if (!PyUnicode_Check(arg)) {
        PyErr_SetString(PyExc_TypeError, "send_str() argument must be str");
        return -1;
    }
    Py_ssize_t n;
    const char *p = PyUnicode_AsUTF8AndSize(arg, &n);
    if (!p) {
        return -1;
    }
    /* the str owns the UTF-8 buffer: borrow it read-only */
    return PyBuffer_FillInfo(view, arg, (void *)p, n, 1, PyBUF_SIMPLE);
}

static PyObject *stream_send(GcProtoRef *self, PyObject *arg, int text) {
    Py_buffer view;
    if (payload(arg, text, &view) < 0) {
        return NULL;
    }
    PyObject *r = goRsgiStreamSend(self->h, (char *)view.buf, view.len);
    PyBuffer_Release(&view);
    return r;
}

static PyObject *stream_send_bytes(GcProtoRef *self, PyObject *arg) {
    return stream_send(self, arg, 0);
}
static PyObject *stream_send_str(GcProtoRef *self, PyObject *arg) {
    return stream_send(self, arg, 1);
}

static PyMethodDef stream_methods[] = {
    {"send_bytes", (PyCFunction)stream_send_bytes, METH_O, NULL},
    {"send_str", (PyCFunction)stream_send_str, METH_O, NULL},
    {NULL},
};

static PyTypeObject RsgiStreamType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.RSGIHTTPStreamTransport",
    .tp_basicsize = sizeof(GcProtoRef),
    .tp_dealloc = (destructor)stream_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = stream_methods,
};

/* ---- HTTP protocol ----------------------------------------------------------- */

static PyObject *respond(GcProto *self, int kind, int status, PyObject *headers, const char *body, Py_ssize_t blen,
                         unsigned long long start, unsigned long long end) {
    if (status < 100 || status > 999) {
        PyErr_SetString(PyExc_ValueError, "Invalid status code");
        return NULL;
    }
    gc_hbuf b;
    gc_hbuf_init(&b);
    if (gc_hbuf_headers(&b, headers, 1) < 0) {
        gc_hbuf_free(&b);
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_TypeError, "headers must be a list of (str, str) tuples");
        }
        return NULL;
    }
    int rc = goRsgiResponse(self->h, kind, status, b.buf, b.len, b.n, (char *)body, blen, start, end);
    gc_hbuf_free(&b);

    if (kind == RK_STREAM) {
        if (rc != 0) {
            return raise_proto();
        }
        return (PyObject *)gc_protoref_new(&RsgiStreamType, (PyObject *)self);
    }
    Py_RETURN_NONE;
}

static char *kw_status_headers[] = {"status", "headers", NULL};
static char *kw_status_headers_body[] = {"status", "headers", "body", NULL};
static char *kw_status_headers_file[] = {"status", "headers", "file", NULL};
static char *kw_status_headers_file_range[] = {"status", "headers", "file", "start", "end", NULL};

static PyObject *http_response_empty(GcProto *self, PyObject *args, PyObject *kwargs) {
    int status = 200;
    PyObject *headers = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iO:response_empty", kw_status_headers, &status, &headers)) {
        return NULL;
    }
    return respond(self, RK_EMPTY, status, headers, "", 0, 0, 0);
}

static PyObject *http_response_bytes(GcProto *self, PyObject *args, PyObject *kwargs) {
    int status = 200;
    PyObject *headers = NULL;
    Py_buffer body = {0};
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iOy*:response_bytes", kw_status_headers_body, &status, &headers,
                                     &body)) {
        return NULL;
    }
    PyObject *r = respond(self, RK_BYTES, status, headers, body.buf ? body.buf : "", body.buf ? body.len : 0, 0, 0);
    if (body.obj) {
        PyBuffer_Release(&body);
    }
    return r;
}

static PyObject *http_response_str(GcProto *self, PyObject *args, PyObject *kwargs) {
    int status = 200;
    PyObject *headers = NULL;
    const char *body = "";
    Py_ssize_t blen = 0;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iOs#:response_str", kw_status_headers_body, &status, &headers,
                                     &body, &blen)) {
        return NULL;
    }
    return respond(self, RK_STR, status, headers, body, blen, 0, 0);
}

static PyObject *http_response_file(GcProto *self, PyObject *args, PyObject *kwargs) {
    int status;
    PyObject *headers;
    const char *file;
    Py_ssize_t flen;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iOs#:response_file", kw_status_headers_file, &status, &headers,
                                     &file, &flen)) {
        return NULL;
    }
    return respond(self, RK_FILE, status, headers, file, flen, 0, 0);
}

static PyObject *http_response_file_range(GcProto *self, PyObject *args, PyObject *kwargs) {
    int status;
    PyObject *headers;
    const char *file;
    Py_ssize_t flen;
    unsigned long long start, end;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iOs#KK:response_file_range", kw_status_headers_file_range, &status,
                                     &headers, &file, &flen, &start, &end)) {
        return NULL;
    }
    if (start >= end) {
        PyErr_SetString(PyExc_ValueError, "Invalid range");
        return NULL;
    }
    return respond(self, RK_FILE_RANGE, status, headers, file, flen, start, end);
}

static PyObject *http_response_stream(GcProto *self, PyObject *args, PyObject *kwargs) {
    int status = 200;
    PyObject *headers = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iO:response_stream", kw_status_headers, &status, &headers)) {
        return NULL;
    }
    return respond(self, RK_STREAM, status, headers, "", 0, 0, 0);
}

static PyObject *http_client_disconnect(GcProto *self, PyObject *Py_UNUSED(ignored)) {
    return goRsgiClientDisconnect(self->h);
}

static PyObject *http_call(GcProto *self, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwargs)) {
    return goRsgiBody(self->h);
}

static PyObject *http_aiter(GcProto *self) {
    if (goRsgiAiter(self->h) != 0) {
        return raise_proto();
    }
    return Py_NewRef((PyObject *)self);
}

static PyObject *http_anext(GcProto *self) {
    return goRsgiAnext(self->h);
}

static PyAsyncMethods http_async = {
    .am_aiter = (unaryfunc)http_aiter,
    .am_anext = (unaryfunc)http_anext,
};

static PyMethodDef http_methods[] = {
    {"client_disconnect", (PyCFunction)http_client_disconnect, METH_NOARGS, NULL},
    {"response_empty", (PyCFunction)http_response_empty, METH_VARARGS | METH_KEYWORDS, NULL},
    {"response_str", (PyCFunction)http_response_str, METH_VARARGS | METH_KEYWORDS, NULL},
    {"response_bytes", (PyCFunction)http_response_bytes, METH_VARARGS | METH_KEYWORDS, NULL},
    {"response_file", (PyCFunction)http_response_file, METH_VARARGS | METH_KEYWORDS, NULL},
    {"response_file_range", (PyCFunction)http_response_file_range, METH_VARARGS | METH_KEYWORDS, NULL},
    {"response_stream", (PyCFunction)http_response_stream, METH_VARARGS | METH_KEYWORDS, NULL},
    {NULL},
};

static PyTypeObject RsgiHTTPType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.RSGIHTTPProtocol",
    .tp_basicsize = sizeof(GcProto),
    .tp_dealloc = (destructor)gc_proto_dealloc,
    .tp_as_async = &http_async,
    .tp_call = (ternaryfunc)http_call,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = http_methods,
};

/* ---- websocket --------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD int kind;
    PyObject *data;
} RsgiWSMessage;

static void wsmsg_dealloc(RsgiWSMessage *self) {
    Py_XDECREF(self->data);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyMemberDef wsmsg_members[] = {
    {"kind", T_INT, offsetof(RsgiWSMessage, kind), READONLY, NULL},
    {"data", T_OBJECT, offsetof(RsgiWSMessage, data), READONLY, NULL},
    {NULL},
};

static PyTypeObject RsgiWSMessageType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.RSGIWebsocketMessage",
    .tp_basicsize = sizeof(RsgiWSMessage),
    .tp_dealloc = (destructor)wsmsg_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_members = wsmsg_members,
};

PyObject *balius_rsgi_ws_message(int kind, const char *p, Py_ssize_t n) {
    PyObject *data = NULL;
    if (kind == 1) {
        data = PyBytes_FromStringAndSize(p, n);
    } else if (kind == 2) {
        data = PyUnicode_DecodeUTF8(p, n, "replace");
    } else {
        data = Py_NewRef(Py_None);
    }
    if (!data) {
        return NULL;
    }
    RsgiWSMessage *m = PyObject_New(RsgiWSMessage, &RsgiWSMessageType);
    if (!m) {
        Py_DECREF(data);
        return NULL;
    }
    m->kind = kind;
    m->data = data;
    return (PyObject *)m;
}

static PyObject *wstrx_receive(GcProtoRef *self, PyObject *Py_UNUSED(ignored)) {
    return goRsgiWSReceive(self->h);
}

static PyObject *wstrx_send(GcProtoRef *self, PyObject *arg, int text) {
    Py_buffer view;
    if (payload(arg, text, &view) < 0) {
        return NULL;
    }
    PyObject *r = goRsgiWSSend(self->h, text, (char *)view.buf, view.len);
    PyBuffer_Release(&view);
    return r;
}

static PyObject *wstrx_send_bytes(GcProtoRef *self, PyObject *arg) {
    return wstrx_send(self, arg, 0);
}
static PyObject *wstrx_send_str(GcProtoRef *self, PyObject *arg) {
    return wstrx_send(self, arg, 1);
}

static PyMethodDef wstrx_methods[] = {
    {"receive", (PyCFunction)wstrx_receive, METH_NOARGS, NULL},
    {"send_bytes", (PyCFunction)wstrx_send_bytes, METH_O, NULL},
    {"send_str", (PyCFunction)wstrx_send_str, METH_O, NULL},
    {NULL},
};

static PyTypeObject RsgiWSTransportType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.RSGIWebsocketTransport",
    .tp_basicsize = sizeof(GcProtoRef),
    .tp_dealloc = (destructor)gc_protoref_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = wstrx_methods,
};

PyObject *balius_rsgi_ws_transport(PyObject *proto) {
    return (PyObject *)gc_protoref_new(&RsgiWSTransportType, proto);
}

static PyObject *ws_accept(GcProto *self, PyObject *Py_UNUSED(ignored)) {
    return goRsgiWSAccept(self->h, (PyObject *)self);
}

static PyObject *ws_close(GcProto *self, PyObject *args, PyObject *kwargs) {
    static char *kw[] = {"status", NULL};
    PyObject *pstatus = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:close", kw, &pstatus)) {
        return NULL;
    }
    int status = -1;
    if (pstatus && pstatus != Py_None) {
        long v = PyLong_AsLong(pstatus);
        if (v == -1 && PyErr_Occurred()) {
            return NULL;
        }
        status = (int)v;
    }
    goRsgiWSClose(self->h, status);
    Py_RETURN_NONE;
}

static PyMethodDef ws_methods[] = {
    {"accept", (PyCFunction)ws_accept, METH_NOARGS, NULL},
    {"close", (PyCFunction)ws_close, METH_VARARGS | METH_KEYWORDS, NULL},
    {NULL},
};

static PyTypeObject RsgiWSType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.RSGIWebsocketProtocol",
    .tp_basicsize = sizeof(GcProto),
    .tp_dealloc = (destructor)gc_proto_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = ws_methods,
};

/* ---- dispatch ------------------------------------------------------------------ */

void balius_rsgi_dispatch(int kind, uintptr_t h, const char *head, Py_ssize_t n) {
    gc_dispatch(kind == GC_KIND_WS ? &RsgiWSType : &RsgiHTTPType, build_scope, kind, h, head, n);
}

int gc_rsgi_init(PyObject *module) {
    PyTypeObject *types[] = {
        &RsgiScopeType, &RsgiStreamType, &RsgiHTTPType, &RsgiWSMessageType, &RsgiWSTransportType, &RsgiWSType,
    };
    const char *names[] = {
        "RSGIScope",
        "RSGIHTTPStreamTransport",
        "RSGIHTTPProtocol",
        "RSGIWebsocketMessage",
        "RSGIWebsocketTransport",
        "RSGIWebsocketProtocol",
    };
    for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        if (PyType_Ready(types[i]) < 0 || PyModule_AddObjectRef(module, names[i], (PyObject *)types[i]) < 0) {
            return -1;
        }
    }
    return 0;
}

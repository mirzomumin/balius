/*
 * balius - C glue: ASGI scope construction, protocol objects and messages.
 */
#include "glue_internal.h"

static PyObject *raise_unsupported(void) {
    if (!PyErr_Occurred()) {
        PyErr_SetString(PyExc_RuntimeError, "Unsupported ASGI message");
    }
    return NULL;
}

/* ---- scope --------------------------------------------------------------- */

static PyObject *addr_tuple(gc_span ip, uint32_t port) {
    PyObject *host = PyUnicode_DecodeUTF8(ip.p, ip.n, "replace");
    if (!host) {
        return NULL;
    }
    PyObject *pport = PyLong_FromUnsignedLong(port);
    if (!pport) {
        Py_DECREF(host);
        return NULL;
    }
    PyObject *t = PyTuple_New(2);
    if (!t) {
        Py_DECREF(host);
        Py_DECREF(pport);
        return NULL;
    }
    PyTuple_SET_ITEM(t, 0, host);
    PyTuple_SET_ITEM(t, 1, pport);
    return t;
}

static PyObject *build_scope(int kind, const char *buf, Py_ssize_t n) {
    gc_head hd;
    if (gc_head_parse(&hd, buf, n) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "malformed request head");
        return NULL;
    }
    PyObject *scope = PyDict_New();
    if (!scope) {
        return NULL;
    }
    PyObject *scheme;
    if (kind == GC_KIND_WS) {
        scheme = hd.tls ? S_wss : S_ws;
    } else {
        scheme = hd.tls ? S_https : S_http;
    }

    /* fresh per scope: middlewares are free to mutate them */
    GC_DICT_SET(scope, S_asgi, Py_BuildValue("{s:s,s:s}", "version", "3.0", "spec_version", "2.3"));
    GC_DICT_SET(scope, S_extensions, Py_BuildValue("{s:{},s:{}}", "http.response.pathsend", "websocket.http.response"));
    if (PyDict_SetItem(scope, S_type, kind == GC_KIND_WS ? S_websocket : S_http) < 0 ||
        PyDict_SetItem(scope, S_http_version, hd.http_version == 0 ? S_v10 : gc_http_version(&hd)) < 0) {
        goto fail;
    }
    GC_DICT_SET(scope, S_server, addr_tuple(hd.server_ip, hd.server_port));
    GC_DICT_SET(scope, S_client, addr_tuple(hd.client_ip, hd.client_port));
    if (PyDict_SetItem(scope, S_scheme, scheme) < 0) {
        goto fail;
    }
    GC_DICT_SET(scope, S_path, PyUnicode_DecodeUTF8(hd.path.p, hd.path.n, "replace"));
    GC_DICT_SET(scope, S_raw_path, PyBytes_FromStringAndSize(hd.raw_path.p, hd.raw_path.n));
    GC_DICT_SET(scope, S_query_string, PyBytes_FromStringAndSize(hd.query.p, hd.query.n));

    gc_cur c = hd.rest;
    uint32_t nsub = gc_rd_u32(&c);
    PyObject *subs = NULL;
    if (kind == GC_KIND_WS) {
        subs = PyList_New((Py_ssize_t)nsub);
        if (!subs) {
            goto fail;
        }
    }
    for (uint32_t i = 0; i < nsub; i++) {
        gc_span s;
        gc_rd_str(&c, &s);
        if (subs) {
            PyObject *item = PyUnicode_DecodeUTF8(s.p, s.n, "replace");
            if (!item) {
                Py_DECREF(subs);
                goto fail;
            }
            PyList_SET_ITEM(subs, (Py_ssize_t)i, item);
        }
    }

    uint32_t nhdr = gc_rd_u32(&c);
    PyObject *headers = PyList_New((Py_ssize_t)nhdr);
    if (!headers) {
        Py_XDECREF(subs);
        goto fail;
    }
    for (uint32_t i = 0; i < nhdr; i++) {
        gc_span k, v;
        gc_rd_str(&c, &k);
        gc_rd_str(&c, &v);
        PyObject *pair = PyTuple_New(2);
        PyObject *pk = PyBytes_FromStringAndSize(k.p, k.n);
        PyObject *pv = PyBytes_FromStringAndSize(v.p, v.n);
        if (!pair || !pk || !pv) {
            Py_XDECREF(pair);
            Py_XDECREF(pk);
            Py_XDECREF(pv);
            Py_DECREF(headers);
            Py_XDECREF(subs);
            goto fail;
        }
        PyTuple_SET_ITEM(pair, 0, pk);
        PyTuple_SET_ITEM(pair, 1, pv);
        PyList_SET_ITEM(headers, (Py_ssize_t)i, pair);
    }
    {
        int rc = PyDict_SetItem(scope, S_headers, headers);
        Py_DECREF(headers);
        if (rc < 0) {
            Py_XDECREF(subs);
            goto fail;
        }
    }
    if (kind == GC_KIND_WS) {
        int rc = PyDict_SetItem(scope, S_subprotocols, subs);
        Py_DECREF(subs);
        if (rc < 0) {
            goto fail;
        }
    } else {
        GC_DICT_SET(scope, S_method, PyUnicode_DecodeUTF8(hd.method.p, hd.method.n, "replace"));
    }
    return scope;

fail:
    Py_DECREF(scope);
    return NULL;
}

/* ---- messages (Go -> Python) ---------------------------------------------- */

static PyObject *msg_new(PyObject *type) {
    PyObject *d = PyDict_New();
    if (d && PyDict_SetItem(d, S_type, type) < 0) {
        Py_CLEAR(d);
    }
    return d;
}

static int msg_set(PyObject *d, PyObject *key, PyObject *value /* stolen */) {
    if (!value) {
        return -1;
    }
    int rc = PyDict_SetItem(d, key, value);
    Py_DECREF(value);
    return rc;
}

PyObject *balius_asgi_msg_request(const char *body, Py_ssize_t n, int more) {
    PyObject *d = msg_new(S_http_request);
    if (!d) {
        return NULL;
    }
    if (msg_set(d, S_body, PyBytes_FromStringAndSize(body, n)) < 0 ||
        PyDict_SetItem(d, S_more_body, more ? Py_True : Py_False) < 0) {
        Py_DECREF(d);
        return NULL;
    }
    return d;
}

PyObject *balius_asgi_msg_disconnect(void) {
    return msg_new(S_http_disconnect);
}

PyObject *balius_asgi_msg_ws_connect(void) {
    return msg_new(S_websocket_connect);
}

PyObject *balius_asgi_msg_ws_text(const char *p, Py_ssize_t n) {
    PyObject *d = msg_new(S_websocket_receive);
    if (d && msg_set(d, S_text, PyUnicode_DecodeUTF8(p, n, "replace")) < 0) {
        Py_CLEAR(d);
    }
    return d;
}

PyObject *balius_asgi_msg_ws_bytes(const char *p, Py_ssize_t n) {
    PyObject *d = msg_new(S_websocket_receive);
    if (d && msg_set(d, S_bytes, PyBytes_FromStringAndSize(p, n)) < 0) {
        Py_CLEAR(d);
    }
    return d;
}

PyObject *balius_asgi_msg_ws_disconnect(int code) {
    PyObject *d = msg_new(S_websocket_disconnect);
    if (d && msg_set(d, S_code, PyLong_FromLong(code)) < 0) {
        Py_CLEAR(d);
    }
    return d;
}

/* ---- messages (Python -> Go) ---------------------------------------------- */

static PyObject *send_start(GcProto *self, PyObject *msg, int ws) {
    PyObject *st = PyDict_GetItemWithError(msg, S_status);
    if (!st) {
        return raise_unsupported();
    }
    long status = PyLong_AsLong(st);
    if (status == -1 && PyErr_Occurred()) {
        PyErr_Clear();
        return raise_unsupported();
    }
    if (status < 100 || status > 999) {
        return raise_unsupported();
    }
    PyObject *headers = PyDict_GetItemWithError(msg, S_headers);
    if (!headers && PyErr_Occurred()) {
        return NULL;
    }
    gc_hbuf b;
    gc_hbuf_init(&b);
    if (gc_hbuf_headers(&b, headers, 0) < 0) {
        gc_hbuf_free(&b);
        PyErr_Clear();
        return raise_unsupported();
    }
    PyObject *r = goAsgiSendStart(self->h, ws, (int)status, b.buf, b.len, b.n);
    gc_hbuf_free(&b);
    return r;
}

static PyObject *send_body(GcProto *self, PyObject *msg, int ws) {
    const char *p = "";
    Py_ssize_t n = 0;
    Py_buffer view;
    int have_view = 0;

    PyObject *body = PyDict_GetItemWithError(msg, S_body);
    if (!body && PyErr_Occurred()) {
        return NULL;
    }
    if (body && body != Py_None) {
        if (PyBytes_Check(body)) {
            p = PyBytes_AS_STRING(body);
            n = PyBytes_GET_SIZE(body);
        } else if (PyObject_GetBuffer(body, &view, PyBUF_SIMPLE) == 0) {
            have_view = 1;
            p = view.buf;
            n = view.len;
        } else {
            PyErr_Clear();
        }
    }
    int more = 0;
    PyObject *pmore = PyDict_GetItemWithError(msg, S_more_body);
    if (pmore) {
        more = PyObject_IsTrue(pmore);
        if (more < 0) {
            PyErr_Clear();
            more = 0;
        }
    } else if (PyErr_Occurred()) {
        if (have_view) {
            PyBuffer_Release(&view);
        }
        return NULL;
    }
    PyObject *r = goAsgiSendBody(self->h, ws, (char *)p, n, more);
    if (have_view) {
        PyBuffer_Release(&view);
    }
    return r;
}

static PyObject *send_pathsend(GcProto *self, PyObject *msg) {
    PyObject *path = PyDict_GetItemWithError(msg, S_path);
    if (!path) {
        return raise_unsupported();
    }
    PyObject *fspath = PyOS_FSPath(path);
    if (!fspath) {
        return NULL;
    }
    PyObject *r = NULL;
    Py_ssize_t n;
    const char *p = NULL;
    if (PyUnicode_Check(fspath)) {
        p = PyUnicode_AsUTF8AndSize(fspath, &n);
    } else if (PyBytes_Check(fspath)) {
        p = PyBytes_AS_STRING(fspath);
        n = PyBytes_GET_SIZE(fspath);
    }
    if (p) {
        r = goAsgiSendFile(self->h, (char *)p, n);
    }
    Py_DECREF(fspath);
    return r;
}

static PyObject *send_ws_accept(GcProto *self, PyObject *msg) {
    const char *p = NULL;
    Py_ssize_t n = -1;
    PyObject *sub = PyDict_GetItemWithError(msg, S_subprotocol);
    if (!sub && PyErr_Occurred()) {
        return NULL;
    }
    if (sub && PyUnicode_Check(sub)) {
        p = PyUnicode_AsUTF8AndSize(sub, &n);
        if (!p) {
            return NULL;
        }
    }
    gc_hbuf b;
    gc_hbuf_init(&b);
    PyObject *headers = PyDict_GetItemWithError(msg, S_headers);
    if (!headers && PyErr_Occurred()) {
        return NULL;
    }
    if (gc_hbuf_headers(&b, headers, 0) < 0) {
        gc_hbuf_free(&b);
        PyErr_Clear();
        return raise_unsupported();
    }
    PyObject *r = goAsgiWSAccept(self->h, (char *)p, n, b.buf, b.len, b.n);
    gc_hbuf_free(&b);
    return r;
}

static PyObject *send_ws_close(GcProto *self, PyObject *msg) {
    long code = 1000;
    const char *reason = "";
    Py_ssize_t rn = 0;

    PyObject *pcode = PyDict_GetItemWithError(msg, S_code);
    if (pcode && PyLong_Check(pcode)) {
        code = PyLong_AsLong(pcode);
        if (code == -1 && PyErr_Occurred()) {
            PyErr_Clear();
            code = 1000;
        }
    } else if (PyErr_Occurred()) {
        return NULL;
    }
    PyObject *preason = PyDict_GetItemWithError(msg, S_reason);
    if (preason && PyUnicode_Check(preason)) {
        reason = PyUnicode_AsUTF8AndSize(preason, &rn);
        if (!reason) {
            return NULL;
        }
    } else if (PyErr_Occurred()) {
        return NULL;
    }
    return goAsgiWSClose(self->h, (int)code, (char *)reason, rn);
}

static PyObject *send_ws_message(GcProto *self, PyObject *msg) {
    PyObject *pbytes = PyDict_GetItemWithError(msg, S_bytes);
    if (!pbytes && PyErr_Occurred()) {
        return NULL;
    }
    PyObject *ptext = PyDict_GetItemWithError(msg, S_text);
    if (!ptext && PyErr_Occurred()) {
        return NULL;
    }
    int has_bytes = pbytes && pbytes != Py_None;
    int has_text = ptext && ptext != Py_None;

    if (has_text && !has_bytes) {
        if (!PyUnicode_Check(ptext)) {
            return raise_unsupported();
        }
        Py_ssize_t n;
        const char *p = PyUnicode_AsUTF8AndSize(ptext, &n);
        if (!p) {
            return NULL;
        }
        return goAsgiWSSend(self->h, 1, (char *)p, n);
    }
    if (has_bytes && !has_text) {
        Py_buffer view;
        if (PyObject_GetBuffer(pbytes, &view, PyBUF_SIMPLE) < 0) {
            PyErr_Clear();
            return raise_unsupported();
        }
        PyObject *r = goAsgiWSSend(self->h, 0, (char *)view.buf, view.len);
        PyBuffer_Release(&view);
        return r;
    }
    if (!has_bytes && !has_text && (pbytes || ptext)) {
        /* an explicit None payload: an empty frame of the matching kind */
        return goAsgiWSSend(self->h, pbytes ? 0 : 1, (char *)"", 0);
    }
    return raise_unsupported();
}

#define TYPE_IS(lit) (tn == (Py_ssize_t)(sizeof(lit) - 1) && memcmp(t, lit, sizeof(lit) - 1) == 0)

static PyObject *asgi_send(GcProto *self, PyObject *msg) {
    if (!PyDict_Check(msg)) {
        return raise_unsupported();
    }
    PyObject *type = PyDict_GetItemWithError(msg, S_type);
    if (!type || !PyUnicode_Check(type)) {
        return raise_unsupported();
    }
    Py_ssize_t tn;
    const char *t = PyUnicode_AsUTF8AndSize(type, &tn);
    if (!t) {
        return NULL;
    }
    if (TYPE_IS("http.response.start")) {
        return send_start(self, msg, 0);
    }
    if (TYPE_IS("http.response.body")) {
        return send_body(self, msg, 0);
    }
    if (TYPE_IS("websocket.send")) {
        return send_ws_message(self, msg);
    }
    if (TYPE_IS("websocket.accept")) {
        return send_ws_accept(self, msg);
    }
    if (TYPE_IS("websocket.close")) {
        return send_ws_close(self, msg);
    }
    if (TYPE_IS("http.response.pathsend")) {
        return send_pathsend(self, msg);
    }
    if (TYPE_IS("websocket.http.response.start")) {
        return send_start(self, msg, 1);
    }
    if (TYPE_IS("websocket.http.response.body")) {
        return send_body(self, msg, 1);
    }
    return raise_unsupported();
}

static PyObject *asgi_receive(GcProto *self, PyObject *Py_UNUSED(ignored)) {
    return goAsgiReceive(self->h);
}

static PyObject *asgi_sent_code(GcProto *self, void *Py_UNUSED(closure)) {
    return PyLong_FromLong((long)goAsgiSentCode(self->h));
}

static PyMethodDef asgi_methods[] = {
    {"receive", (PyCFunction)asgi_receive, METH_NOARGS, "Awaitable returning the next ASGI event."},
    {"send", (PyCFunction)asgi_send, METH_O, "Awaitable sending an ASGI event."},
    {NULL},
};

static PyGetSetDef asgi_http_getset[] = {
    {"sent_response_code", (getter)asgi_sent_code, NULL, NULL, NULL},
    {NULL},
};

static PyTypeObject AsgiHTTPType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.ASGIHTTPProtocol",
    .tp_basicsize = sizeof(GcProto),
    .tp_dealloc = (destructor)gc_proto_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = asgi_methods,
    .tp_getset = asgi_http_getset,
};

static PyTypeObject AsgiWSType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_balius.ASGIWebsocketProtocol",
    .tp_basicsize = sizeof(GcProto),
    .tp_dealloc = (destructor)gc_proto_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .tp_methods = asgi_methods,
};

/* ---- dispatch -------------------------------------------------------------- */

void balius_asgi_dispatch(int kind, uintptr_t h, const char *head, Py_ssize_t n) {
    gc_dispatch(kind == GC_KIND_WS ? &AsgiWSType : &AsgiHTTPType, build_scope, kind, h, head, n);
}

int gc_asgi_init(PyObject *module) {
    if (PyType_Ready(&AsgiHTTPType) < 0 || PyType_Ready(&AsgiWSType) < 0) {
        return -1;
    }
    if (PyModule_AddObjectRef(module, "ASGIHTTPProtocol", (PyObject *)&AsgiHTTPType) < 0 ||
        PyModule_AddObjectRef(module, "ASGIWebsocketProtocol", (PyObject *)&AsgiWSType) < 0) {
        return -1;
    }
    return 0;
}

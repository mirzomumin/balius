/*
 * balius - declarations shared by the C glue translation units.
 */
#ifndef BALIUS_GLUE_INTERNAL_H
#define BALIUS_GLUE_INTERNAL_H

#include "glue.h"
#include <structmember.h>
#include <string.h>

#include "_cgo_export.h"

/* ---- interned strings --------------------------------------------------- */

#define GC_STRINGS(X)                                                                                                  \
    X(type, "type")                                                                                                    \
    X(asgi, "asgi")                                                                                                    \
    X(extensions, "extensions")                                                                                        \
    X(http_version, "http_version")                                                                                    \
    X(server, "server")                                                                                                \
    X(client, "client")                                                                                                \
    X(scheme, "scheme")                                                                                                \
    X(path, "path")                                                                                                    \
    X(raw_path, "raw_path")                                                                                            \
    X(query_string, "query_string")                                                                                    \
    X(headers, "headers")                                                                                              \
    X(method, "method")                                                                                                \
    X(subprotocols, "subprotocols")                                                                                    \
    X(body, "body")                                                                                                    \
    X(more_body, "more_body")                                                                                          \
    X(status, "status")                                                                                                \
    X(subprotocol, "subprotocol")                                                                                      \
    X(code, "code")                                                                                                    \
    X(reason, "reason")                                                                                                \
    X(bytes, "bytes")                                                                                                  \
    X(text, "text")                                                                                                    \
    X(set_result, "set_result")                                                                                        \
    X(set_exception, "set_exception")                                                                                  \
    X(close, "close")                                                                                                  \
    X(http, "http")                                                                                                    \
    X(https, "https")                                                                                                  \
    X(ws, "ws")                                                                                                        \
    X(wss, "wss")                                                                                                      \
    X(websocket, "websocket")                                                                                          \
    X(v1, "1")                                                                                                         \
    X(v10, "1.0")                                                                                                      \
    X(v11, "1.1")                                                                                                      \
    X(v2, "2")                                                                                                         \
    X(http_request, "http.request")                                                                                    \
    X(http_disconnect, "http.disconnect")                                                                              \
    X(websocket_connect, "websocket.connect")                                                                          \
    X(websocket_receive, "websocket.receive")                                                                          \
    X(websocket_disconnect, "websocket.disconnect")                                                                    \
    X(rsgi_version, "1.6")                                                                                             \
    X(SERVER_PROTOCOL, "SERVER_PROTOCOL")                                                                              \
    X(SERVER_NAME, "SERVER_NAME")                                                                                      \
    X(SERVER_PORT, "SERVER_PORT")                                                                                      \
    X(REMOTE_ADDR, "REMOTE_ADDR")                                                                                      \
    X(REQUEST_METHOD, "REQUEST_METHOD")                                                                                \
    X(PATH_INFO, "PATH_INFO")                                                                                          \
    X(QUERY_STRING, "QUERY_STRING")                                                                                    \
    X(wsgi_url_scheme, "wsgi.url_scheme")                                                                              \
    X(wsgi_input, "wsgi.input")                                                                                        \
    X(proto_http1, "HTTP/1.0")                                                                                         \
    X(proto_http11, "HTTP/1.1")                                                                                        \
    X(proto_http2, "HTTP/2")

#define X(name, lit) extern PyObject *S_##name;
GC_STRINGS(X)
#undef X

/* ---- globals ------------------------------------------------------------ */

extern PyObject *g_create_task;
extern PyObject *g_create_future;
extern PyObject *g_wcallback;
extern PyObject *g_stop_cb;
extern PyObject *g_wsgi_callback;
extern PyObject *g_log_exc;
extern PyObject *g_rsgi_headers_cls;
extern PyObject *g_empty;
extern PyObject *GcExc_RSGIProtocolError;
extern PyObject *GcExc_RSGIProtocolClosed;

/* ---- request head ------------------------------------------------------- */

typedef struct {
    const unsigned char *p, *end;
} gc_cur;

typedef struct {
    const char *p;
    Py_ssize_t n;
} gc_span;

typedef struct {
    int http_version; /* 0: 1.0, 1: 1.1, 2: 2 */
    int tls;
    int flags;
    uint32_t server_port, client_port;
    gc_span server_ip, client_ip, method, path, raw_path, query, authority;
    gc_cur rest; /* subprotocols, then headers */
} gc_head;

static inline uint32_t gc_rd_u32(gc_cur *c) {
    uint32_t v = 0;
    if (c->end - c->p >= 4) {
        memcpy(&v, c->p, 4);
        c->p += 4;
    } else {
        c->p = c->end;
    }
    return v;
}

static inline int gc_rd_str(gc_cur *c, gc_span *s) {
    uint32_t n = gc_rd_u32(c);
    if ((size_t)(c->end - c->p) < n) {
        s->p = "";
        s->n = 0;
        c->p = c->end;
        return -1;
    }
    s->p = (const char *)c->p;
    s->n = (Py_ssize_t)n;
    c->p += n;
    return 0;
}

int gc_head_parse(gc_head *hd, const char *buf, Py_ssize_t n);
PyObject *gc_http_version(const gc_head *hd);

/* ---- headers serialization ---------------------------------------------- */

typedef struct {
    char *buf;
    Py_ssize_t len, cap;
    int n;
    char stack[1024];
} gc_hbuf;

void gc_hbuf_init(gc_hbuf *b);
void gc_hbuf_free(gc_hbuf *b);
int gc_hbuf_headers(gc_hbuf *b, PyObject *headers, int text);

/* ---- shared objects ------------------------------------------------------ */

/* Base layout of every protocol object: a handle on the Go side state. */
typedef struct {
    PyObject_HEAD uintptr_t h;
} GcProto;

GcProto *gc_proto_new(PyTypeObject *type, uintptr_t h);
void gc_proto_dealloc(GcProto *self);

/* An object borrowing the Go state of a protocol object it keeps alive. */
typedef struct {
    PyObject_HEAD PyObject *proto;
    uintptr_t h;
} GcProtoRef;

GcProtoRef *gc_protoref_new(PyTypeObject *type, PyObject *proto);
void gc_protoref_dealloc(GcProtoRef *self);

/* Sets d[key] = expr (a new reference, stolen); jumps to `fail` on error. */
#define GC_DICT_SET(d, key, expr)                                                                                      \
    do {                                                                                                               \
        PyObject *_v = (expr);                                                                                         \
        if (!_v) {                                                                                                     \
            goto fail;                                                                                                 \
        }                                                                                                              \
        int _rc = PyDict_SetItem((d), (key), _v);                                                                      \
        Py_DECREF(_v);                                                                                                 \
        if (_rc < 0) {                                                                                                 \
            goto fail;                                                                                                 \
        }                                                                                                              \
    } while (0)

typedef struct {
    PyObject_HEAD PyObject *proto;
    PyObject *scope;
    PyObject *task;
    uintptr_t h;
} GcWatcher;

void gc_schedule(PyObject *proto, PyObject *scope, uintptr_t h);

typedef PyObject *(*gc_scope_builder)(int kind, const char *head, Py_ssize_t n);
void gc_dispatch(PyTypeObject *type, gc_scope_builder build, int kind, uintptr_t h, const char *head, Py_ssize_t n);

void gc_report_exc(const char *where);
PyObject *gc_take_exc(void);
void gc_display_exc(PyObject *exc);
void gc_log_app_exc(void);

int gc_asgi_init(PyObject *module);
int gc_rsgi_init(PyObject *module);
int gc_wsgi_init(PyObject *module);

#endif /* BALIUS_GLUE_INTERNAL_H */

# Ported from granian (https://github.com/emmett-framework/granian), BSD-3-Clause.
import time
from enum import Enum
from functools import wraps

import _tulpar
from _tulpar import (
    RSGIHTTPProtocol as HTTPProtocol,  # noqa: F401
)
from _tulpar import (
    RSGIProtocolClosed as ProtocolClosed,  # noqa: F401
)
from _tulpar import (
    RSGIProtocolError as ProtocolError,  # noqa: F401
)
from _tulpar import (
    RSGIScope as Scope,  # noqa: F401
)
from _tulpar import (
    RSGIWebsocketMessage as WebsocketMessage,  # noqa: F401
)
from _tulpar import (
    RSGIWebsocketProtocol as WebsocketProtocol,  # noqa: F401
)

from .log import log_request_builder


class Headers:
    """Read-only view on the request headers.

    Names are lower-case; `get_all` returns every value of a repeated header.
    """

    __slots__ = ['_data']

    def __init__(self, data):
        self._data = data

    def keys(self):
        return list(self._data.keys())

    def values(self):
        return [value for values in self._data.values() for value in values]

    def items(self):
        return [(key, value) for key, values in self._data.items() for value in values]

    def __contains__(self, key):
        return key.lower() in self._data

    def __getitem__(self, key):
        return self._data[key.lower()][0]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self._data)

    def get(self, key, default=None):
        values = self._data.get(key.lower())
        return values[0] if values else default

    def get_all(self, key):
        return list(self._data.get(key.lower(), ()))

    def __repr__(self):
        return f'Headers({self.items()!r})'


_tulpar.set_types(Headers)


class WebsocketMessageType(int, Enum):
    close = 0
    bytes = 1
    string = 2


class _LoggingProto:
    __slots__ = ['inner', 'status']

    def __init__(self, inner):
        self.inner = inner
        self.status = 500

    def __call__(self):
        return self.inner()

    def __aiter__(self):
        return self.inner.__aiter__()

    def client_disconnect(self):
        return self.inner.client_disconnect()

    def response_empty(self, status=200, headers=()):
        self.status = status
        return self.inner.response_empty(status, headers)

    def response_str(self, status=200, headers=(), body=''):
        self.status = status
        return self.inner.response_str(status, headers, body)

    def response_bytes(self, status=200, headers=(), body=b''):
        self.status = status
        return self.inner.response_bytes(status, headers, body)

    def response_file(self, status, headers, file):
        self.status = status
        return self.inner.response_file(status, headers, file)

    def response_file_range(self, status, headers, file, start, end):
        self.status = status
        return self.inner.response_file_range(status, headers, file, start, end)

    def response_stream(self, status=200, headers=()):
        self.status = status
        return self.inner.response_stream(status, headers)


def _callbacks_from_target(target):
    callback = target.__rsgi__ if hasattr(target, '__rsgi__') else target
    callback_init = target.__rsgi_init__ if hasattr(target, '__rsgi_init__') else lambda *args, **kwargs: None
    callback_del = target.__rsgi_del__ if hasattr(target, '__rsgi_del__') else lambda *args, **kwargs: None
    return callback, callback_init, callback_del


def _callback_wrapper(callback, access_log_fmt=False):
    async def _http_logger(scope, proto):
        rt, mt = time.time(), time.perf_counter()
        try:
            rv = await callback(scope, proto)
        finally:
            access_log(rt, mt, scope, proto.status)
        return rv

    def _ws_logger(scope, proto):
        access_log(time.time(), time.perf_counter(), scope, 101)
        return callback(scope, proto)

    def _logger(scope, proto):
        if scope.proto == 'http':
            return _http_logger(scope, _LoggingProto(proto))
        return _ws_logger(scope, proto)

    access_log = _build_access_logger(access_log_fmt)
    wrapper = callback
    if access_log_fmt:
        wrapper = _logger
        wraps(callback)(wrapper)

    return wrapper


def _build_access_logger(fmt):
    logger = log_request_builder(fmt)

    def _log_dict(scope):
        return {
            'addr_remote': scope.client.rsplit(':', 1)[0],
            'protocol': 'HTTP/' + scope.http_version,
            'path': scope.path,
            'qs': scope.query_string,
            'method': scope.method,
            'scheme': scope.scheme,
        }

    def _access_log(rt, mt, scope, resp_code):
        logger(rt, mt, _log_dict(scope), resp_code)

    def _access_log_with_headers(rt, mt, scope, resp_code):
        data = _log_dict(scope)
        data['headers'] = scope.headers.get
        logger(rt, mt, data, resp_code)

    return _access_log_with_headers if logger.parse_headers else _access_log

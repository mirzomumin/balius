# Ported from granian (https://github.com/emmett-framework/granian), BSD-3-Clause.
import os
import sys
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

from .log import log_request_builder


class Response:
    __slots__ = ['headers', 'status']

    def __init__(self):
        self.status = 200
        self.headers = []

    def __call__(self, status: str, headers: list[tuple[str, str]], exc_info: Any = None):
        self.status = int(status.split(' ', 1)[0])
        self.headers = headers


def _callback_wrapper(callback: Callable[..., Any], scope_opts: dict[str, Any], access_log_fmt=None):
    basic_env: dict[str, Any] = dict(os.environ)
    basic_env.update(
        {
            'GATEWAY_INTERFACE': 'CGI/1.1',
            'SCRIPT_NAME': scope_opts.get('url_path_prefix') or '',
            'SERVER_SOFTWARE': 'balius',
            'wsgi.errors': sys.stderr,
            #: this is not in PEP333, but you know, werkzeug..
            'wsgi.input_terminated': True,
            'wsgi.multiprocess': bool(scope_opts.get('multiprocess')),
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.version': (1, 0),
        }
    )

    def _runner(proto, scope):
        resp = Response()
        environ = basic_env | scope
        if basic_env['SCRIPT_NAME']:
            environ['PATH_INFO'] = scope['PATH_INFO'][len(basic_env['SCRIPT_NAME']) :] or '/'

        rv = callback(environ, resp)

        if isinstance(rv, list):
            proto.response_bytes(resp.status, resp.headers, b''.join(rv))
            return resp.status

        # PEP 3333: the headers go out with the first non-empty chunk, as a
        # generator may only call start_response once it starts running
        it, first = iter(rv), b''
        try:
            for first in it:
                if first:
                    break
        except BaseException:
            if hasattr(rv, 'close'):
                rv.close()
            raise
        proto.response_iter(resp.status, resp.headers, first, it, rv)
        return resp.status

    def _logger(proto, scope):
        rt, mt = time.time(), time.perf_counter()
        try:
            status = _runner(proto, scope)
            access_log(rt, mt, scope, status)
        except BaseException:
            access_log(rt, mt, scope, 500)
            raise
        return status

    access_log = _build_access_logger(access_log_fmt)
    wrapper = _logger if access_log_fmt else _runner
    wraps(callback)(wrapper)

    return wrapper


def _build_access_logger(fmt):
    logger = log_request_builder(fmt)

    def _log_dict(scope):
        return {
            'addr_remote': scope['REMOTE_ADDR'],
            'protocol': scope['SERVER_PROTOCOL'],
            'path': scope['PATH_INFO'],
            'qs': scope['QUERY_STRING'],
            'method': scope['REQUEST_METHOD'],
            'scheme': scope['wsgi.url_scheme'],
        }

    def _access_log(rt, mt, scope, resp_code):
        logger(rt, mt, _log_dict(scope), resp_code)

    def _access_log_with_headers(rt, mt, scope, resp_code):
        data = _log_dict(scope)
        data['headers'] = lambda key: scope.get('HTTP_' + key.upper().replace('-', '_'))
        logger(rt, mt, data, resp_code)

    return _access_log_with_headers if logger.parse_headers else _access_log

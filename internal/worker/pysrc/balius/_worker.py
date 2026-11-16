"""Python side of a balius worker process.

The Go runtime boots the interpreter on the process main thread and calls
`main` with the worker specification. From there this module loads the
application, wires it to the server and, on async interfaces, runs the
event loop until the Go side asks to stop.
"""

import asyncio
import importlib
import json
import sys
import traceback
from functools import partial
from pathlib import Path

import _balius

from ._internal import load_target
from .asgi import LifespanProtocol
from .asgi import _callback_wrapper as _asgi_call_wrap
from .log import configure_logging, logger
from .rsgi import _callback_wrapper as _rsgi_call_wrap
from .rsgi import _callbacks_from_target as _rsgi_cbs_from_target
from .wsgi import _callback_wrapper as _wsgi_call_wrap


def _new_loop(name):
    """Builds the event loop: `auto` takes the first available of rloop,
    uvloop and asyncio."""
    for impl in ('rloop', 'uvloop', 'asyncio') if name == 'auto' else (name,):
        if impl == 'asyncio':
            loop = asyncio.new_event_loop()
        else:
            try:
                loop = importlib.import_module(impl).new_event_loop()
            except ImportError:
                if name != 'auto':
                    raise RuntimeError(f"'{impl}' implementation not available.") from None
                continue
        asyncio.set_event_loop(loop)
        return loop


def _log_app_exc(exc):
    logger.error('Application callable raised an exception', exc_info=exc)


def _future_watcher_wrapper(inner):
    async def future_watcher(watcher):
        try:
            await inner(watcher.scope, watcher.proto)
        except asyncio.CancelledError:
            watcher.done()
            raise
        except GeneratorExit:
            raise
        except BaseException as exc:
            watcher.done()
            _log_app_exc(exc)
            return
        watcher.done()

    return future_watcher


def _serve_loop(loop, wcallback):
    """Hands the application to the Go server and runs the event loop until
    the server asks to stop."""
    stop = asyncio.Event()
    wake_fd = _balius.wake_fd()
    # everything the Go runtime wants done on the loop goes through `drain`
    loop.add_reader(wake_fd, _balius.drain)
    try:
        _balius.serve(loop, wcallback, stop.set, _log_app_exc)
        loop.run_until_complete(stop.wait())
    finally:
        loop.remove_reader(wake_fd)


def _close_loop(loop):
    try:
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
    except Exception:
        pass
    finally:
        loop.close()


def _run_asgi(cfg, loader, access_log_fmt, scope_opts, lifespan):
    loop = _new_loop(cfg['loop'])
    callback = loader()

    if not lifespan:
        wcallback = _future_watcher_wrapper(_asgi_call_wrap(callback, scope_opts, {}, access_log_fmt))
        _serve_loop(loop, wcallback)
        _close_loop(loop)
        return 0

    lifespan_handler = LifespanProtocol(callback)
    wcallback = _future_watcher_wrapper(_asgi_call_wrap(callback, scope_opts, lifespan_handler.state, access_log_fmt))
    loop.run_until_complete(lifespan_handler.startup())
    if lifespan_handler.interrupt:
        logger.error('ASGI lifespan startup failed', exc_info=lifespan_handler.exc)
        return 1

    _serve_loop(loop, wcallback)
    loop.run_until_complete(lifespan_handler.shutdown())
    _close_loop(loop)
    return 0


def _run_rsgi(cfg, loader, access_log_fmt):
    loop = _new_loop(cfg['loop'])
    callback, callback_init, callback_del = _rsgi_cbs_from_target(loader())
    wcallback = _future_watcher_wrapper(_rsgi_call_wrap(callback, access_log_fmt))

    callback_init(loop)
    _serve_loop(loop, wcallback)
    callback_del(loop)
    _close_loop(loop)
    return 0


def _run_wsgi(cfg, loader, access_log_fmt, scope_opts):
    callback = loader()
    wcallback = _wsgi_call_wrap(callback, scope_opts, access_log_fmt)
    # blocks (without holding the GIL) until the server stops
    _balius.serve_wsgi(wcallback, _log_app_exc)
    return 0


def _run(spec):
    cfg = spec['config']
    log_config = json.loads(cfg['log_config']) if cfg.get('log_config') else None
    configure_logging(cfg['log_level'], log_config, cfg['log_enabled'])

    working_dir = Path(cfg['working_dir']) if cfg.get('working_dir') else None
    loader = partial(load_target, cfg['target'], wd=working_dir, factory=cfg['factory'])
    access_log_fmt = cfg['access_log_fmt'] if cfg['access_log'] else None
    scope_opts = {'url_path_prefix': cfg.get('url_path_prefix') or None, 'multiprocess': cfg['workers'] > 1}

    interface = cfg['interface']
    if interface == 'wsgi':
        return _run_wsgi(cfg, loader, access_log_fmt, scope_opts)
    if interface == 'rsgi':
        return _run_rsgi(cfg, loader, access_log_fmt)
    return _run_asgi(cfg, loader, access_log_fmt, scope_opts, lifespan=interface == 'asgi')


def main(spec_json):
    try:
        return _run(json.loads(spec_json)) or 0
    except SystemExit as exc:
        if exc.code is None:
            return 0
        if isinstance(exc.code, int):
            return exc.code
        print(exc.code, file=sys.stderr)
        return 1
    except BaseException:
        traceback.print_exc()
        return 1
    finally:
        sys.stdout.flush()
        sys.stderr.flush()

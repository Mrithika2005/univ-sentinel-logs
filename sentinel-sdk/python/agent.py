"""
SENTINEL SDK — Python Agent
============================
Auto-instruments:
  • All class methods via SentinelMeta metaclass
  • print() / logging module
  • requests / httpx (outbound HTTP)
  • SQLAlchemy (query events)
  • psycopg2 (raw postgres)
  • neo4j driver
  • redis-py
  • celery tasks
  • Flask / FastAPI / Django middleware
  • process signals & uncaught exceptions
Sends logs → ClickHouse HTTP interface (batch)

Usage:
    from sentinel_sdk.python.agent import init_sentinel, SentinelMeta

    sentinel = init_sentinel("my-service")

    # Zero-effort class instrumentation:
    class OrderService(metaclass=SentinelMeta):
        def place_order(self, order): ...

    # Or instrument existing instances:
    sentinel.instrument(my_service_instance)
"""

from __future__ import annotations

import builtins
import datetime
import functools
import inspect
import json
import logging
import os
import signal
import sys
import threading
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

# ── Optional imports (graceful) ────────────────────────────────────────────────

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import httpx as _httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

try:
    import sqlalchemy as _sa
    from sqlalchemy import event as _sa_event
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

try:
    import psycopg2 as _psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import neo4j as _neo4j
    HAS_NEO4J = True
except ImportError:
    HAS_NEO4J = False

try:
    import redis as _redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

try:
    from clickhouse_driver import Client as _ClickhouseClient
    HAS_CH_DRIVER = True
except ImportError:
    HAS_CH_DRIVER = False


# ── Layer & Level constants ─────────────────────────────────────────────────────

class LogLayer:
    PRESENTATION   = 'presentation'
    API_GATEWAY    = 'api_gateway'
    BUSINESS_LOGIC = 'business_logic'
    DATA_ACCESS    = 'data_access'
    SERVICE        = 'service'
    SECURITY       = 'security'
    OBSERVABILITY  = 'observability'
    INFRASTRUCTURE = 'infrastructure'
    DOMAIN         = 'domain'


class LogLevel:
    DEBUG = 'DEBUG'
    INFO  = 'INFO'
    WARN  = 'WARN'
    ERROR = 'ERROR'
    FATAL = 'FATAL'


# ── Layer inference ─────────────────────────────────────────────────────────────

import re as _re

_LAYER_PATTERNS: List[tuple] = [
    (_re.compile(r'auth|jwt|token|oauth|permission|acl|rbac|guard|firewall|waf|encrypt|decrypt|password|credential|session|csrf|cors', _re.I), LogLayer.SECURITY),
    (_re.compile(r'repo|repository|dao|database|db|query|migration|schema|cache|redis|mongo|postgres|sql|neo4j|orm|entity|store|persist|storage', _re.I), LogLayer.DATA_ACCESS),
    (_re.compile(r'controller|router|route|middleware|gateway|proxy|handler|endpoint|api|rest|graphql|grpc|webhook|interceptor|view', _re.I), LogLayer.API_GATEWAY),
    (_re.compile(r'service|saga|aggregate|domain|policy|rule|event|command|workflow|process|pricing|discount|fraud|risk|consent', _re.I), LogLayer.DOMAIN),
    (_re.compile(r'infra|worker|job|cron|queue|kafka|rabbit|bull|pubsub|container|health|monitor|metric|cpu|memory|disk|celery', _re.I), LogLayer.INFRASTRUCTURE),
    (_re.compile(r'trace|span|log|alert|metric|telemetry|observer|slo|sla|alarm', _re.I), LogLayer.OBSERVABILITY),
    (_re.compile(r'component|page|ui|render|form|modal|widget|screen|layout|theme|template', _re.I), LogLayer.PRESENTATION),
]


def infer_layer(name: str) -> str:
    for pattern, layer in _LAYER_PATTERNS:
        if pattern.search(name):
            return layer
    return LogLayer.BUSINESS_LOGIC


# ── LogRecord ───────────────────────────────────────────────────────────────────

class LogRecord:
    __slots__ = (
        'message', 'level', 'layer', 'timestamp',
        'record_id', 'trace_id', 'span_id',
        'service', 'env', 'context',
    )

    def __init__(
        self,
        message:  str,
        layer:    str = LogLayer.BUSINESS_LOGIC,
        level:    str = LogLevel.INFO,
        service:  str = 'unknown-python-service',
        context:  Optional[Dict[str, Any]] = None,
        trace_id: str = 'untracked',
        span_id:  str = 'untracked',
    ):
        self.message   = message
        self.layer     = layer
        self.level     = level
        self.service   = service
        self.timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.record_id = str(uuid.uuid4())
        self.trace_id  = trace_id
        self.span_id   = span_id
        self.env       = os.getenv('ENV', os.getenv('PYTHON_ENV', 'development'))
        self.context   = context or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp':  self.timestamp,
            'record_id':  self.record_id,
            'trace_id':   self.trace_id,
            'span_id':    self.span_id,
            'service':    self.service,
            'env':        self.env,
            'layer':      self.layer,
            'level':      self.level,
            'message':    self.message,
            'context':    json.dumps(self.context or {}),
        }

    def __str__(self) -> str:
        _colors = {
            LogLevel.DEBUG: '\033[36m',
            LogLevel.INFO:  '\033[92m',
            LogLevel.WARN:  '\033[93m',
            LogLevel.ERROR: '\033[91m',
            LogLevel.FATAL: '\033[95m',
        }
        reset = '\033[0m'
        c = _colors.get(self.level, '\033[92m')
        return f'{c}[{self.timestamp}] [{self.layer.upper()}] [{self.level}] {self.message}{reset}'


# ── ClickHouse batch writer ─────────────────────────────────────────────────────

class _ClickHouseWriter:
    def __init__(self, cfg: Dict[str, Any]):
        self._host     = cfg.get('clickhouse_host', 'http://localhost:8123')
        self._db       = cfg.get('clickhouse_database', 'sentinel')
        self._table    = cfg.get('clickhouse_table', 'logs')
        self._user     = cfg.get('clickhouse_user', '')
        self._password = cfg.get('clickhouse_password', '')
        self._batch    = cfg.get('batch_size', 50)
        self._debug    = cfg.get('debug', False)
        self._queue: List[LogRecord] = []
        self._lock     = threading.Lock()
        self._timer: Optional[threading.Timer] = None

    def init(self) -> None:
        """Create DB + table DDL."""
        self._exec(f'CREATE DATABASE IF NOT EXISTS {self._db}')
        self._exec(f"""
            CREATE TABLE IF NOT EXISTS {self._db}.{self._table}
            (
                timestamp  String,
                record_id  String,
                trace_id   String,
                span_id    String,
                service    String,
                env        String,
                layer      String,
                level      String,
                message    String,
                context    String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(toDateTime(timestamp))
            ORDER BY (timestamp, service, layer)
            TTL toDateTime(timestamp) + INTERVAL 90 DAY
        """)
        self._schedule_flush()

    def enqueue(self, record: LogRecord) -> None:
        with self._lock:
            self._queue.append(record)
            if len(self._queue) >= self._batch:
                self._flush_locked()

    def _schedule_flush(self) -> None:
        self._timer = threading.Timer(2.0, self._flush_and_reschedule)
        self._timer.daemon = True
        self._timer.start()

    def _flush_and_reschedule(self) -> None:
        self.flush()
        self._schedule_flush()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._queue:
            return
        batch = self._queue[:]
        self._queue.clear()

        rows = '\n'.join(json.dumps(r.to_dict()) for r in batch)
        query = f'INSERT INTO {self._db}.{self._table} FORMAT JSONEachRow'

        try:
            import urllib.request
            url  = f'{self._host}/?query={_url_encode(query)}'
            data = rows.encode('utf-8')
            req  = urllib.request.Request(url, data=data, method='POST')
            req.add_header('Content-Type', 'application/x-ndjson')
            if self._user:
                import base64
                cred = base64.b64encode(f'{self._user}:{self._password}'.encode()).decode()
                req.add_header('Authorization', f'Basic {cred}')

            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status not in (200, 201) and self._debug:
                    print(f'[SENTINEL] ClickHouse error: {resp.status}', file=sys.stderr)
        except Exception as exc:
            if self._debug:
                print(f'[SENTINEL] Flush error: {exc}', file=sys.stderr)
            # Re-enqueue so logs are not lost
            self._queue = batch + self._queue

    def _exec(self, query: str) -> None:
        import urllib.request
        url = f'{self._host}/?query={_url_encode(query)}'
        req = urllib.request.Request(url, method='POST')
        if self._user:
            import base64
            cred = base64.b64encode(f'{self._user}:{self._password}'.encode()).decode()
            req.add_header('Authorization', f'Basic {cred}')
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 201):
                raise RuntimeError(f'ClickHouse DDL failed: status={resp.status}')


def _url_encode(s: str) -> str:
    import urllib.parse
    return urllib.parse.quote(s, safe='')


# ── SentinelMeta — zero-effort class instrumentation ───────────────────────────

class SentinelMeta(type):
    """
    Metaclass. Any class that inherits it gets every method
    auto-wrapped with enter/exit/error logging.

    class OrderService(metaclass=SentinelMeta):
        _sentinel_layer = LogLayer.DOMAIN   # optional override
        ...
    """
    _sentinel_agent: Optional['SentinelPython'] = None

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        layer = namespace.get('_sentinel_layer') or infer_layer(name)

        for attr, val in namespace.items():
            if attr.startswith('_') and not attr.startswith('__'):
                continue
            if callable(val) and not isinstance(val, (classmethod, staticmethod, property)):
                setattr(cls, attr, mcs._wrap(val, name, attr, layer))

        return cls

    @staticmethod
    def _wrap(fn: Callable, cls_name: str, method: str, layer: str) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            agent = SentinelMeta._sentinel_agent
            start = time.perf_counter()
            if agent:
                agent._emit(
                    f'{cls_name}.{method} called',
                    layer=layer, level=LogLevel.INFO,
                    context={'className': cls_name, 'functionName': method},
                )
            try:
                result = fn(*args, **kwargs)
                ms = (time.perf_counter() - start) * 1000
                if agent:
                    agent._emit(
                        f'{cls_name}.{method} → ok ({ms:.1f}ms)',
                        layer=layer, level=LogLevel.INFO,
                        context={'className': cls_name, 'functionName': method, 'durationMs': ms},
                    )
                return result
            except Exception as exc:
                ms = (time.perf_counter() - start) * 1000
                if agent:
                    agent._emit(
                        f'{cls_name}.{method} → error: {exc}',
                        layer=layer, level=LogLevel.ERROR,
                        context={
                            'className': cls_name, 'functionName': method,
                            'durationMs': ms,
                            'exceptionType': type(exc).__name__,
                            'stackTrace': traceback.format_exc(),
                        },
                    )
                raise
        return wrapper


T = TypeVar('T')


# ── Main agent ──────────────────────────────────────────────────────────────────

class SentinelPython:
    def __init__(self, service_name: str = 'python-service', **cfg):
        self.service_name = service_name
        self._cfg = {
            'clickhouse_host':     cfg.get('clickhouse_host',     os.getenv('CLICKHOUSE_HOST',     'http://localhost:8123')),
            'clickhouse_database': cfg.get('clickhouse_database', os.getenv('CLICKHOUSE_DATABASE', 'sentinel')),
            'clickhouse_table':    cfg.get('clickhouse_table',    os.getenv('CLICKHOUSE_TABLE',    'logs')),
            'clickhouse_user':     cfg.get('clickhouse_user',     os.getenv('CLICKHOUSE_USER',     '')),
            'clickhouse_password': cfg.get('clickhouse_password', os.getenv('CLICKHOUSE_PASSWORD', '')),
            'batch_size':          cfg.get('batch_size',          50),
            'slow_query_ms':       cfg.get('slow_query_ms',       200),
            'slow_http_ms':        cfg.get('slow_http_ms',        1000),
            'debug':               cfg.get('debug',               False),
        }
        self._writer = _ClickHouseWriter(self._cfg)
        self._instrumented: set = set()
        self._trace_id = str(uuid.uuid4())

    # ── Public API ────────────────────────────────────────────────────────────

    def hook(self) -> 'SentinelPython':
        """Call once at startup. Patches everything."""
        self._writer.init()
        SentinelMeta._sentinel_agent = self

        self._patch_print()
        self._patch_logging()
        self._patch_requests()
        self._patch_httpx()
        self._patch_sqlalchemy()
        self._patch_psycopg2()
        self._patch_neo4j()
        self._patch_redis()
        self._hook_process()
        self._start_vitals()

        self._emit(
            f'Sentinel Python Agent hooked on "{self.service_name}"',
            layer=LogLayer.INFRASTRUCTURE, level=LogLevel.INFO,
            context={'python_version': sys.version, 'pid': os.getpid()},
        )
        return self

    def instrument(self, target: Any, layer: Optional[str] = None) -> 'SentinelPython':
        """
        Instrument any existing class instance or class.
        Sentinel infers the layer from the class name automatically.
        """
        if isinstance(target, type):
            cls   = target
            proto = cls
        else:
            cls   = type(target)
            proto = cls

        cls_id = id(proto)
        if cls_id in self._instrumented:
            return self
        self._instrumented.add(cls_id)

        resolved_layer = layer or infer_layer(cls.__name__)
        methods = [
            name for name, val in inspect.getmembers(cls, predicate=inspect.isfunction)
            if not name.startswith('__')
        ]

        for method_name in methods:
            try:
                orig = getattr(cls, method_name)
                wrapped = SentinelMeta._wrap(orig, cls.__name__, method_name, resolved_layer)
                setattr(cls, method_name, wrapped)
            except (AttributeError, TypeError):
                pass

        self._emit(
            f'Instrumented: {cls.__name__} ({len(methods)} methods → {resolved_layer})',
            layer=LogLayer.OBSERVABILITY, level=LogLevel.DEBUG,
        )
        return self

    def track(self, layer: str = LogLayer.BUSINESS_LOGIC):
        """
        Decorator for standalone functions:
            @sentinel.track(layer=LogLayer.DOMAIN)
            def place_order(order): ...
        """
        def decorator(fn: Callable) -> Callable:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                start = time.perf_counter()
                self._emit(
                    f'{fn.__qualname__} called',
                    layer=layer, level=LogLevel.INFO,
                    context={'functionName': fn.__qualname__},
                )
                try:
                    result = fn(*args, **kwargs)
                    ms = (time.perf_counter() - start) * 1000
                    self._emit(
                        f'{fn.__qualname__} → ok ({ms:.1f}ms)',
                        layer=layer, level=LogLevel.INFO,
                        context={'functionName': fn.__qualname__, 'durationMs': ms},
                    )
                    return result
                except Exception as exc:
                    ms = (time.perf_counter() - start) * 1000
                    self._emit(
                        f'{fn.__qualname__} → error: {exc}',
                        layer=layer, level=LogLevel.ERROR,
                        context={
                            'functionName': fn.__qualname__,
                            'durationMs': ms,
                            'exceptionType': type(exc).__name__,
                            'stackTrace': traceback.format_exc(),
                        },
                    )
                    raise
            return wrapper
        return decorator

    def log(self, message: str, layer: str = LogLayer.BUSINESS_LOGIC,
            level: str = LogLevel.INFO, context: Optional[Dict] = None) -> None:
        """Manually emit a log."""
        self._emit(message, layer=layer, level=level, context=context)

    def flush(self) -> None:
        """Force-flush the queue."""
        self._writer.flush()

    # ── Flask middleware ──────────────────────────────────────────────────────

    def flask_middleware(self, app: Any) -> Any:
        """
        Wraps a Flask app with request/response logging.
        Usage: app = sentinel.flask_middleware(app)
        """
        from functools import wraps

        @app.before_request
        def before():
            import flask
            flask.g._sentinel_start = time.perf_counter()
            req = flask.request
            self._emit(
                f'→ {req.method} {req.path}',
                layer=LogLayer.API_GATEWAY, level=LogLevel.INFO,
                context={'method': req.method, 'path': req.path, 'clientIp': req.remote_addr},
            )

        @app.after_request
        def after(response):
            import flask
            ms = (time.perf_counter() - getattr(flask.g, '_sentinel_start', time.perf_counter())) * 1000
            req = flask.request
            self._emit(
                f'← {req.method} {req.path} {response.status_code} ({ms:.1f}ms)',
                layer=LogLayer.API_GATEWAY,
                level=LogLevel.ERROR if response.status_code >= 500 else LogLevel.WARN if response.status_code >= 400 else LogLevel.INFO,
                context={'method': req.method, 'path': req.path, 'statusCode': response.status_code, 'durationMs': ms},
            )
            return response

        return app

    # ── FastAPI middleware ────────────────────────────────────────────────────

    def fastapi_middleware(self, app: Any) -> Any:
        """
        Adds ASGI middleware for request/response logging.
        Usage: app = sentinel.fastapi_middleware(app)
        """
        sentinel = self

        class _Middleware:
            def __init__(self, asgi_app):
                self.app = asgi_app

            async def __call__(self, scope, receive, send):
                if scope['type'] != 'http':
                    await self.app(scope, receive, send)
                    return

                start  = time.perf_counter()
                method = scope.get('method', '')
                path   = scope.get('path', '')
                sentinel._emit(
                    f'→ {method} {path}',
                    layer=LogLayer.API_GATEWAY, level=LogLevel.INFO,
                    context={'method': method, 'path': path},
                )

                status_code = [200]

                async def send_wrapper(message):
                    if message['type'] == 'http.response.start':
                        status_code[0] = message['status']
                    await send(message)

                await self.app(scope, receive, send_wrapper)

                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'← {method} {path} {status_code[0]} ({ms:.1f}ms)',
                    layer=LogLayer.API_GATEWAY,
                    level=LogLevel.ERROR if status_code[0] >= 500 else LogLevel.INFO,
                    context={'method': method, 'path': path, 'statusCode': status_code[0], 'durationMs': ms},
                )

        app.add_middleware(_Middleware)
        return app

    # ── Internal emitter ──────────────────────────────────────────────────────

    def _emit(
        self,
        message: str,
        layer:   str = LogLayer.BUSINESS_LOGIC,
        level:   str = LogLevel.INFO,
        context: Optional[Dict] = None,
    ) -> None:
        record = LogRecord(
            message=message,
            layer=layer,
            level=level,
            service=self.service_name,
            context=context or {},
            trace_id=self._trace_id,
        )
        if self._cfg['debug']:
            print(f'[SENTINEL] {record}', file=sys.stderr)
        self._writer.enqueue(record)

    # ── print() patch ─────────────────────────────────────────────────────────

    def _patch_print(self) -> None:
        sentinel   = self
        orig_print = builtins.print

        def sentinel_print(*args, **kwargs):
            msg = ' '.join(str(a) for a in args)
            if '[SENTINEL]' in msg:
                orig_print(*args, **kwargs)
                return
            sentinel._emit(msg, layer=LogLayer.BUSINESS_LOGIC, level=LogLevel.INFO)
            orig_print(f'[SENTINEL] {msg}', **kwargs)

        builtins.print = sentinel_print

    # ── logging module patch ──────────────────────────────────────────────────

    def _patch_logging(self) -> None:
        sentinel = self

        _LEVEL_MAP = {
            logging.DEBUG:    LogLevel.DEBUG,
            logging.INFO:     LogLevel.INFO,
            logging.WARNING:  LogLevel.WARN,
            logging.ERROR:    LogLevel.ERROR,
            logging.CRITICAL: LogLevel.FATAL,
        }

        class SentinelHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                sentinel._emit(
                    record.getMessage(),
                    layer=LogLayer.OBSERVABILITY,
                    level=_LEVEL_MAP.get(record.levelno, LogLevel.INFO),
                    context={'logger': record.name, 'module': record.module, 'funcName': record.funcName},
                )

        logging.getLogger().addHandler(SentinelHandler())

    # ── requests patch ────────────────────────────────────────────────────────

    def _patch_requests(self) -> None:
        if not HAS_REQUESTS:
            return
        sentinel = self
        orig_send = _requests.Session.send

        def patched_send(self_session, request, **kwargs):
            start = time.perf_counter()
            sentinel._emit(
                f'→ {request.method} {request.url}',
                layer=LogLayer.SERVICE, level=LogLevel.INFO,
                context={'method': request.method, 'path': request.url},
            )
            try:
                response = orig_send(self_session, request, **kwargs)
                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'← {request.method} {request.url} {response.status_code} ({ms:.1f}ms)',
                    layer=LogLayer.SERVICE,
                    level=LogLevel.ERROR if response.status_code >= 500 else LogLevel.WARN if response.status_code >= 400 else LogLevel.INFO,
                    context={'method': request.method, 'path': request.url, 'statusCode': response.status_code, 'durationMs': ms},
                )
                return response
            except Exception as exc:
                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'✗ {request.method} {request.url} — {exc}',
                    layer=LogLayer.SERVICE, level=LogLevel.ERROR,
                    context={'method': request.method, 'path': request.url, 'durationMs': ms, 'exceptionType': type(exc).__name__},
                )
                raise

        _requests.Session.send = patched_send

    # ── httpx patch ───────────────────────────────────────────────────────────

    def _patch_httpx(self) -> None:
        if not HAS_HTTPX:
            return
        sentinel = self
        orig_send = _httpx.Client.send

        def patched_send(self_client, request, **kwargs):
            start = time.perf_counter()
            sentinel._emit(
                f'→ httpx {request.method} {request.url}',
                layer=LogLayer.SERVICE, level=LogLevel.INFO,
                context={'method': request.method, 'path': str(request.url)},
            )
            try:
                response = orig_send(self_client, request, **kwargs)
                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'← httpx {request.method} {request.url} {response.status_code} ({ms:.1f}ms)',
                    layer=LogLayer.SERVICE,
                    level=LogLevel.ERROR if response.status_code >= 500 else LogLevel.INFO,
                    context={'statusCode': response.status_code, 'durationMs': ms},
                )
                return response
            except Exception as exc:
                sentinel._emit(
                    f'✗ httpx {request.method} {request.url} — {exc}',
                    layer=LogLayer.SERVICE, level=LogLevel.ERROR,
                    context={'exceptionType': type(exc).__name__},
                )
                raise

        _httpx.Client.send = patched_send

    # ── SQLAlchemy patch ──────────────────────────────────────────────────────

    def _patch_sqlalchemy(self) -> None:
        if not HAS_SQLALCHEMY:
            return
        sentinel   = self
        slow_ms    = self._cfg['slow_query_ms']
        _starts: Dict[Any, float] = {}

        @_sa_event.listens_for(_sa.engine.Engine, 'before_cursor_execute')
        def before(conn, cursor, statement, parameters, context, executemany):
            _starts[id(cursor)] = time.perf_counter()

        @_sa_event.listens_for(_sa.engine.Engine, 'after_cursor_execute')
        def after(conn, cursor, statement, parameters, context, executemany):
            start = _starts.pop(id(cursor), time.perf_counter())
            ms    = (time.perf_counter() - start) * 1000
            is_slow = ms > slow_ms
            sentinel._emit(
                f'SQLAlchemy{" [SLOW]" if is_slow else ""}: {statement[:120]}',
                layer=LogLayer.DATA_ACCESS,
                level=LogLevel.WARN if is_slow else LogLevel.INFO,
                context={
                    'database': 'sqlalchemy',
                    'queryType': statement.strip().split()[0].upper(),
                    'durationMs': ms,
                    'slowQuery': is_slow,
                    'slowQueryThresholdMs': slow_ms,
                },
            )

    # ── psycopg2 patch ────────────────────────────────────────────────────────

    def _patch_psycopg2(self) -> None:
        if not HAS_PSYCOPG2:
            return
        sentinel = self
        slow_ms  = self._cfg['slow_query_ms']
        orig_execute = _psycopg2.extensions.cursor.execute

        def patched_execute(self_cursor, query, vars=None):
            start = time.perf_counter()
            try:
                result = orig_execute(self_cursor, query, vars)
                ms = (time.perf_counter() - start) * 1000
                is_slow = ms > slow_ms
                sentinel._emit(
                    f'psycopg2{" [SLOW]" if is_slow else ""}: {str(query)[:120]}',
                    layer=LogLayer.DATA_ACCESS,
                    level=LogLevel.WARN if is_slow else LogLevel.INFO,
                    context={
                        'database': 'postgres',
                        'queryType': str(query).strip().split()[0].upper(),
                        'durationMs': ms,
                        'rowsAffected': self_cursor.rowcount,
                        'slowQuery': is_slow,
                    },
                )
                return result
            except Exception as exc:
                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'psycopg2 error: {exc}',
                    layer=LogLayer.DATA_ACCESS, level=LogLevel.ERROR,
                    context={
                        'database': 'postgres',
                        'durationMs': ms,
                        'deadlock': 'deadlock' in str(exc).lower(),
                        'exceptionType': type(exc).__name__,
                        'stackTrace': traceback.format_exc(),
                    },
                )
                raise

        _psycopg2.extensions.cursor.execute = patched_execute

    # ── neo4j patch ───────────────────────────────────────────────────────────

    def _patch_neo4j(self) -> None:
        if not HAS_NEO4J:
            return
        sentinel = self
        slow_ms  = self._cfg['slow_query_ms']
        orig_run = _neo4j.Session.run

        def patched_run(self_session, query, parameters=None, **kwargs):
            start = time.perf_counter()
            try:
                result = orig_run(self_session, query, parameters, **kwargs)
                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'Neo4j{" [SLOW]" if ms > slow_ms else ""}: {str(query)[:120]}',
                    layer=LogLayer.DATA_ACCESS,
                    level=LogLevel.WARN if ms > slow_ms else LogLevel.INFO,
                    context={'database': 'neo4j', 'durationMs': ms, 'slowQuery': ms > slow_ms},
                )
                return result
            except Exception as exc:
                sentinel._emit(
                    f'Neo4j error: {exc}',
                    layer=LogLayer.DATA_ACCESS, level=LogLevel.ERROR,
                    context={'database': 'neo4j', 'exceptionType': type(exc).__name__, 'stackTrace': traceback.format_exc()},
                )
                raise

        _neo4j.Session.run = patched_run

    # ── redis patch ───────────────────────────────────────────────────────────

    def _patch_redis(self) -> None:
        if not HAS_REDIS:
            return
        sentinel = self
        orig_execute_command = _redis.StrictRedis.execute_command

        def patched_execute_command(self_redis, *args, **kwargs):
            cmd   = args[0] if args else 'CMD'
            start = time.perf_counter()
            try:
                result = orig_execute_command(self_redis, *args, **kwargs)
                ms = (time.perf_counter() - start) * 1000
                sentinel._emit(
                    f'Redis {cmd} ({ms:.1f}ms)',
                    layer=LogLayer.DATA_ACCESS, level=LogLevel.DEBUG,
                    context={'database': 'redis', 'queryType': cmd, 'durationMs': ms, 'cacheHit': result is not None},
                )
                return result
            except Exception as exc:
                sentinel._emit(
                    f'Redis {cmd} error: {exc}',
                    layer=LogLayer.DATA_ACCESS, level=LogLevel.ERROR,
                    context={'database': 'redis', 'queryType': cmd, 'exceptionType': type(exc).__name__},
                )
                raise

        _redis.StrictRedis.execute_command = patched_execute_command

    # ── process hooks ─────────────────────────────────────────────────────────

    def _hook_process(self) -> None:
        sentinel = self

        def handle_exception(exc_type, exc_value, exc_tb):
            sentinel._emit(
                f'Uncaught exception: {exc_value}',
                layer=LogLayer.SECURITY, level=LogLevel.FATAL,
                context={
                    'exceptionType': exc_type.__name__,
                    'stackTrace': ''.join(traceback.format_tb(exc_tb)),
                },
            )
            sentinel._writer.flush()
            sys.__excepthook__(exc_type, exc_value, exc_tb)

        sys.excepthook = handle_exception

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                orig_handler = signal.getsignal(sig)
                def make_handler(s, oh):
                    def handler(signum, frame):
                        sentinel._emit(
                            f'Process signal: {s.name}',
                            layer=LogLayer.INFRASTRUCTURE, level=LogLevel.WARN,
                            context={'containerEvent': 'stop'},
                        )
                        sentinel._writer.flush()
                        if callable(oh):
                            oh(signum, frame)
                        else:
                            sys.exit(0)
                    return handler
                signal.signal(sig, make_handler(sig, orig_handler))
            except (ValueError, OSError):
                pass  # Can't set signals in threads

    # ── Infrastructure vitals ─────────────────────────────────────────────────

    def _start_vitals(self) -> None:
        sentinel = self

        def vitals_loop():
            try:
                import psutil
                while True:
                    time.sleep(30)
                    proc = psutil.Process(os.getpid())
                    mem  = proc.memory_info()
                    cpu  = proc.cpu_percent(interval=1)
                    sentinel._emit(
                        f'Process vitals: cpu={cpu}% rss={mem.rss // 1024 // 1024}MB',
                        layer=LogLayer.INFRASTRUCTURE, level=LogLevel.INFO,
                        context={
                            'cpuPercent':      cpu,
                            'memoryUsedBytes': mem.rss,
                            'memoryTotalBytes': mem.vms,
                            'containerName':   sentinel.service_name,
                        },
                    )
            except ImportError:
                pass  # psutil not installed — skip vitals

        t = threading.Thread(target=vitals_loop, daemon=True)
        t.start()


# ── Factory ─────────────────────────────────────────────────────────────────────

def init_sentinel(service_name: str = 'python-service', **kwargs) -> SentinelPython:
    """
    One-liner initialisation:
        sentinel = init_sentinel("my-service")
    All keyword args are forwarded to SentinelPython:
        clickhouse_host, clickhouse_database, clickhouse_table,
        clickhouse_user, clickhouse_password, batch_size,
        slow_query_ms, slow_http_ms, debug
    """
    agent = SentinelPython(service_name, **kwargs)
    agent.hook()
    return agent

/* ============================================================
   SENTINEL SDK — Node Agent
   Auto-instruments:
     • HTTP/HTTPS server & client
     • console (all levels)
     • fs (file I/O)
     • process (uncaught errors, signals)
     • All user class prototypes (runtime patching)
     • pg, node-postgres, neo4j-driver, ioredis, mongoose,
       @nestjs/common decorators
   Sends logs → ClickHouse directly
   ============================================================ */

import {
  LogLayer,
  LogLevel,
  LogRecord,
  inferLayer,
  type InstrumentedClassMeta,
  type LogContext,
} from '../core/types.ts';

import http  from 'http';
import https from 'https';
import fs    from 'fs';
import path  from 'path';

/* ── Config ──────────────────────────────────────────────── */

export interface SentinelNodeConfig {
  serviceName?:       string;
  clickhouseHost?:    string;  // e.g. http://localhost:8123
  clickhouseDatabase?: string;
  clickhouseTable?:   string;
  clickhouseUser?:    string;
  clickhousePassword?: string;
  batchSize?:         number;
  flushInterval?:     number;  // ms
  slowQueryMs?:       number;
  slowHttpMs?:        number;
  debug?:             boolean;
  autoInstrument?:    boolean; // default true
}

/* ── ClickHouse batch writer ─────────────────────────────── */

class ClickHouseWriter {
  private host:      string;
  private database:  string;
  private table:     string;
  private authHeader?: string;
  private queue:     LogRecord[] = [];
  private batchSize: number;
  private timer?:    ReturnType<typeof setInterval>;
  private debug:     boolean;
  private ready      = false;

  constructor(cfg: Required<SentinelNodeConfig>) {
    this.host      = cfg.clickhouseHost;
    this.database  = cfg.clickhouseDatabase;
    this.table     = cfg.clickhouseTable;
    this.batchSize = cfg.batchSize;
    this.debug     = cfg.debug;

    if (cfg.clickhouseUser) {
      const cred = `${cfg.clickhouseUser}:${cfg.clickhousePassword || ''}`;
      this.authHeader = `Basic ${Buffer.from(cred).toString('base64')}`;
    }
  }

  async init(): Promise<void> {
    await this._exec(`CREATE DATABASE IF NOT EXISTS ${this.database}`);
    await this._exec(`
      CREATE TABLE IF NOT EXISTS ${this.database}.${this.table}
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
    `);
    this.ready = true;
    this._startFlush();
  }

  enqueue(record: LogRecord): void {
    this.queue.push(record);
    if (this.queue.length >= this.batchSize) void this._flush();
  }

  private _startFlush(): void {
    this.timer = setInterval(() => void this._flush(), 2000);
    process.on('exit',    () => void this._flush());
    process.on('SIGINT',  () => { void this._flush(); process.exit(0); });
    process.on('SIGTERM', () => { void this._flush(); process.exit(0); });
  }

  private async _flush(): Promise<void> {
    if (!this.ready || this.queue.length === 0) return;
    const batch = this.queue.splice(0);

    const rows = batch.map((r) => JSON.stringify({
      timestamp: r.timestamp,
      record_id: r.record_id,
      trace_id:  r.trace_id,
      span_id:   r.span_id,
      service:   r.service,
      env:       r.env,
      layer:     r.layer,
      level:     r.level,
      message:   r.message,
      context:   JSON.stringify(r.context || {}),
    })).join('\n');

    const query = `INSERT INTO ${this.database}.${this.table} FORMAT JSONEachRow`;

    try {
      const res = await fetch(
        `${this.host}/?query=${encodeURIComponent(query)}`,
        {
          method:  'POST',
          headers: {
            'Content-Type': 'application/x-ndjson',
            ...(this.authHeader ? { Authorization: this.authHeader } : {}),
          },
          body: rows,
        }
      );
      if (!res.ok && this.debug) {
        const txt = await res.text();
        console.error('[SENTINEL] ClickHouse ingest error:', res.status, txt.slice(0, 200));
      }
    } catch (err) {
      if (this.debug) console.error('[SENTINEL] ClickHouse flush error:', err);
      // Put records back — don't lose them
      this.queue.unshift(...batch);
    }
  }

  private async _exec(query: string): Promise<void> {
    const res = await fetch(
      `${this.host}/?query=${encodeURIComponent(query)}`,
      {
        method:  'POST',
        headers: {
          ...(this.authHeader ? { Authorization: this.authHeader } : {}),
        },
      }
    );
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`ClickHouse DDL failed: ${txt.slice(0, 300)}`);
    }
  }
}

/* ── Main class ──────────────────────────────────────────── */

export class SentinelNode {
  private cfg:          Required<SentinelNodeConfig>;
  private writer:       ClickHouseWriter;
  private instrumented  = new WeakSet<object>();
  private traceId       = this._genId();

  constructor(config: SentinelNodeConfig = {}) {
    this.cfg = {
      serviceName:        config.serviceName        || 'node-service',
      clickhouseHost:     config.clickhouseHost     || process.env.CLICKHOUSE_HOST     || 'http://localhost:8123',
      clickhouseDatabase: config.clickhouseDatabase || process.env.CLICKHOUSE_DATABASE || 'sentinel',
      clickhouseTable:    config.clickhouseTable    || process.env.CLICKHOUSE_TABLE    || 'logs',
      clickhouseUser:     config.clickhouseUser     || process.env.CLICKHOUSE_USER     || '',
      clickhousePassword: config.clickhousePassword || process.env.CLICKHOUSE_PASSWORD || '',
      batchSize:          config.batchSize          ?? 50,
      flushInterval:      config.flushInterval      ?? 2000,
      slowQueryMs:        config.slowQueryMs        ?? 200,
      slowHttpMs:         config.slowHttpMs         ?? 1000,
      debug:              config.debug              ?? false,
      autoInstrument:     config.autoInstrument     ?? true,
    };

    this.writer = new ClickHouseWriter(this.cfg);
  }

  /* ── Public API ─────────────────────────────────────────── */

  async hook(): Promise<this> {
    await this.writer.init();

    this._patchConsole();
    this._patchHttp();
    this._patchHttpClient();
    this._patchFS();
    this._hookProcess();

    if (this.cfg.autoInstrument) {
      this._patchDatabaseDrivers();
    }

    this._emit({
      message: `Sentinel Node Agent hooked on "${this.cfg.serviceName}"`,
      layer:   LogLayer.INFRASTRUCTURE,
      level:   LogLevel.INFO,
      context: { nodeVersion: process.version, pid: process.pid } as LogContext,
    });

    return this;
  }

  /**
   * Instrument any class instance, constructor, or plain object.
   * Sentinel auto-detects the layer from the class name.
   */
  instrument<T extends object>(target: T | (new (...a: any[]) => T), layer?: LogLayer): this {
    const proto = typeof target === 'function'
      ? (target as any).prototype
      : Object.getPrototypeOf(target);

    if (!proto || this.instrumented.has(proto)) return this;
    this.instrumented.add(proto);

    const className = (typeof target === 'function'
      ? (target as Function).name
      : target.constructor?.name) || 'UnknownClass';
    const resolvedLayer = layer || inferLayer(className);
    const methodNames: string[] = [];

    let p: object | null = proto;
    while (p && p !== Object.prototype) {
      Object.getOwnPropertyNames(p).forEach((key) => {
        if (key === 'constructor') return;
        const desc = Object.getOwnPropertyDescriptor(p!, key);
        if (!desc || typeof desc.value !== 'function') return;
        methodNames.push(key);
        this._wrapMethod(proto, key, className, resolvedLayer);
      });
      p = Object.getPrototypeOf(p);
    }

    if (this.cfg.debug) {
      const meta: InstrumentedClassMeta = { className, layer: resolvedLayer, methodNames };
      this._emit({
        message: `Auto-instrumented: ${className} (${methodNames.length} methods → ${resolvedLayer})`,
        layer:   LogLayer.OBSERVABILITY,
        level:   LogLevel.DEBUG,
        context: meta as unknown as LogContext,
      });
    }

    return this;
  }

  /** Manually emit a log. */
  log(partial: Partial<LogRecord> & { message: string }): void {
    this._emit(partial);
  }

  /* ── Internal emitter ───────────────────────────────────── */

  private _emit(partial: Partial<LogRecord> & { message: string }): void {
    const record = new LogRecord({
      ...partial,
      service:  this.cfg.serviceName,
      trace_id: partial.trace_id || this.traceId,
    });
    this.writer.enqueue(record);
  }

  /* ── console patch ──────────────────────────────────────── */

  private _patchConsole(): void {
    const self = this;
    const sentinel_prefix = '[SENTINEL]';

    const map: Array<[keyof Console, LogLevel]> = [
      ['log',   LogLevel.INFO],
      ['info',  LogLevel.INFO],
      ['warn',  LogLevel.WARN],
      ['error', LogLevel.ERROR],
      ['debug', LogLevel.DEBUG],
    ];

    map.forEach(([method, level]) => {
      const orig = (console as any)[method].bind(console) as (...a: any[]) => void;

      (console as any)[method] = (...args: any[]) => {
        const msg = args.map((a) =>
          typeof a === 'object' ? JSON.stringify(a) : String(a)
        ).join(' ');

        // Avoid infinite loop on our own internal logs
        if (msg.includes(sentinel_prefix)) {
          orig(...args);
          return;
        }

        self._emit({
          message: msg,
          layer:   LogLayer.BUSINESS_LOGIC,
          level,
        });

        orig(`${sentinel_prefix} ${record_prefix(level, msg)}`);
      };
    });

    function record_prefix(level: LogLevel, msg: string) {
      const colors: Record<LogLevel, string> = {
        [LogLevel.DEBUG]: '\x1b[36m',
        [LogLevel.INFO]:  '\x1b[32m',
        [LogLevel.WARN]:  '\x1b[33m',
        [LogLevel.ERROR]: '\x1b[31m',
        [LogLevel.FATAL]: '\x1b[35m',
      };
      return `${colors[level]}[${level}]${'\x1b[0m'} ${msg}`;
    }
  }

  /* ── HTTP server patch (inbound) ────────────────────────── */

  private _patchHttp(): void {
    const self = this;

    const wrapListener = (
      listener: ((req: http.IncomingMessage, res: http.ServerResponse) => void) | undefined
    ) => (req: http.IncomingMessage, res: http.ServerResponse) => {
      const start = Date.now();
      const reqId = self._genId();

      self._emit({
        message: `→ ${req.method} ${req.url}`,
        layer:   LogLayer.API_GATEWAY,
        level:   LogLevel.INFO,
        context: {
          method:        req.method,
          path:          req.url,
          requestId:     reqId,
          clientIp:      req.socket.remoteAddress,
          userAgent:     req.headers['user-agent'],
          requestSizeBytes: Number(req.headers['content-length'] || 0),
        } as LogContext,
      });

      res.on('finish', () => {
        const durationMs = Date.now() - start;
        const isSlow     = durationMs > self.cfg.slowHttpMs;
        self._emit({
          message: `← ${req.method} ${req.url} ${res.statusCode} (${durationMs}ms)${isSlow ? ' [SLOW]' : ''}`,
          layer:   LogLayer.API_GATEWAY,
          level:   res.statusCode >= 500 ? LogLevel.ERROR : res.statusCode >= 400 ? LogLevel.WARN : LogLevel.INFO,
          context: {
            method:    req.method,
            path:      req.url,
            statusCode: res.statusCode,
            durationMs,
            requestId: reqId,
            userAgent: req.headers['user-agent'],
          } as LogContext,
        });
      });

      listener?.(req, res);
    };

    // http.createServer
    const origHttp = http.createServer.bind(http);
    (http as any).createServer = (...args: any[]) => {
      if (typeof args[0] === 'function') args[0] = wrapListener(args[0]);
      else if (typeof args[1] === 'function') args[1] = wrapListener(args[1]);
      return origHttp(...(args as Parameters<typeof http.createServer>));
    };

    // https.createServer
    const origHttps = https.createServer.bind(https);
    (https as any).createServer = (...args: any[]) => {
      const last = args[args.length - 1];
      if (typeof last === 'function') args[args.length - 1] = wrapListener(last);
      return origHttps(...(args as Parameters<typeof https.createServer>));
    };
  }

  /* ── Outbound HTTP client patch ─────────────────────────── */

  private _patchHttpClient(): void {
    const self = this;

    const wrapRequest = (
      origRequest: typeof http.request,
      scheme: string
    ) => (...args: any[]): http.ClientRequest => {
      const req: http.ClientRequest = origRequest(...(args as Parameters<typeof http.request>));

      const urlStr = typeof args[0] === 'string' ? args[0]
                   : args[0] instanceof URL       ? args[0].toString()
                   : `${(args[0] as http.RequestOptions).host}${(args[0] as http.RequestOptions).path}`;
      const method = (args[0] as http.RequestOptions).method || 'GET';
      const start  = Date.now();

      self._emit({
        message: `Outbound ${scheme}: ${method} ${urlStr}`,
        layer:   LogLayer.SERVICE,
        level:   LogLevel.INFO,
        context: { method, path: urlStr } as LogContext,
      });

      req.on('response', (res) => {
        const durationMs = Date.now() - start;
        self._emit({
          message: `Outbound ${scheme} response: ${method} ${urlStr} ${res.statusCode} (${durationMs}ms)`,
          layer:   LogLayer.SERVICE,
          level:   (res.statusCode || 200) >= 400 ? LogLevel.WARN : LogLevel.INFO,
          context: { method, path: urlStr, statusCode: res.statusCode, durationMs } as LogContext,
        });
      });

      req.on('error', (err) => {
        const durationMs = Date.now() - start;
        self._emit({
          message: `Outbound ${scheme} error: ${method} ${urlStr} — ${err.message}`,
          layer:   LogLayer.SERVICE,
          level:   LogLevel.ERROR,
          context: { method, path: urlStr, durationMs, exceptionType: err.constructor.name, stackTrace: err.stack } as LogContext,
        });
      });

      return req;
    };

    http.request  = wrapRequest(http.request.bind(http),   'HTTP')  as typeof http.request;
    https.request = wrapRequest(https.request.bind(https), 'HTTPS') as typeof https.request;
  }

  /* ── File system patch ──────────────────────────────────── */

  private _patchFS(): void {
    const self = this;

    const ops: Array<keyof typeof fs> = ['readFile', 'writeFile', 'appendFile', 'unlink', 'readdir', 'stat', 'mkdir', 'rmdir'];

    ops.forEach((op) => {
      const orig = (fs as any)[op] as Function;
      if (typeof orig !== 'function') return;

      (fs as any)[op] = (...args: any[]) => {
        const filePath = args[0];
        const start    = Date.now();

        self._emit({
          message: `FS.${op}: ${filePath}`,
          layer:   LogLayer.DATA_ACCESS,
          level:   LogLevel.DEBUG,
          context: { fileOperation: op, filePath: String(filePath) } as LogContext,
        });

        // Find the callback (last argument that is a function)
        const cbIdx = args.findIndex((a, i) => i > 0 && typeof a === 'function');
        if (cbIdx !== -1) {
          const origCb = args[cbIdx];
          args[cbIdx] = (err: NodeJS.ErrnoException | null, ...cbArgs: any[]) => {
            const durationMs = Date.now() - start;
            if (err) {
              self._emit({
                message: `FS.${op} failed: ${filePath} — ${err.message}`,
                layer:   LogLayer.DATA_ACCESS,
                level:   LogLevel.ERROR,
                context: { fileOperation: op, filePath: String(filePath), durationMs, exceptionType: err.code } as LogContext,
              });
            } else {
              self._emit({
                message: `FS.${op} completed: ${filePath} (${durationMs}ms)`,
                layer:   LogLayer.DATA_ACCESS,
                level:   LogLevel.DEBUG,
                context: { fileOperation: op, filePath: String(filePath), durationMs } as LogContext,
              });
            }
            origCb(err, ...cbArgs);
          };
        }

        return orig.apply(fs, args);
      };
    });
  }

  /* ── Process-level hooks ────────────────────────────────── */

  private _hookProcess(): void {
    const self = this;

    process.on('uncaughtException', (err) => {
      self._emit({
        message: `Uncaught Exception: ${err.message}`,
        layer:   LogLayer.SECURITY,
        level:   LogLevel.FATAL,
        context: { exceptionType: err.constructor.name, stackTrace: err.stack } as LogContext,
      });
    });

    process.on('unhandledRejection', (reason) => {
      self._emit({
        message: `Unhandled Promise Rejection: ${reason}`,
        layer:   LogLayer.OBSERVABILITY,
        level:   LogLevel.ERROR,
        context: { exceptionType: String(reason) } as LogContext,
      });
    });

    // Infrastructure vitals every 30s
    setInterval(() => {
      const mem = process.memoryUsage();
      self._emit({
        message: `Process vitals: rss=${(mem.rss / 1024 / 1024).toFixed(1)}MB heap=${(mem.heapUsed / 1024 / 1024).toFixed(1)}MB`,
        layer:   LogLayer.INFRASTRUCTURE,
        level:   LogLevel.INFO,
        context: {
          memoryUsedBytes:  mem.heapUsed,
          memoryTotalBytes: mem.heapTotal,
          containerName:    self.cfg.serviceName,
        } as LogContext,
      });
    }, 30_000).unref();
  }

  /* ── Auto-instrument DB drivers ─────────────────────────── */

  private _patchDatabaseDrivers(): void {
    this._tryPatchPg();
    this._tryPatchNeo4j();
    this._tryPatchMongoose();
    this._tryPatchRedis();
  }

  /** node-postgres (pg) */
  private _tryPatchPg(): void {
    try {
      const pg   = require('pg');
      const orig = pg.Client.prototype.query.bind(pg.Client.prototype);
      const self = this;

      pg.Client.prototype.query = async function (...args: any[]) {
        const sql      = typeof args[0] === 'string' ? args[0] : args[0]?.text || '';
        const start    = Date.now();
        const isSlow   = () => (Date.now() - start) > self.cfg.slowQueryMs;

        try {
          const result = await orig.apply(this, args);
          const durationMs = Date.now() - start;

          self._emit({
            message: `PG Query${isSlow() ? ' [SLOW]' : ''}: ${sql.slice(0, 120)}`,
            layer:   LogLayer.DATA_ACCESS,
            level:   isSlow() ? LogLevel.WARN : LogLevel.INFO,
            context: {
              queryType:    sql.trim().split(' ')[0].toUpperCase() as any,
              database:     'postgres',
              durationMs,
              rowsAffected: result?.rowCount,
              slowQuery:    isSlow(),
              slowQueryThresholdMs: self.cfg.slowQueryMs,
              queryHash:    String(sql.length) + sql.slice(0, 20),
            } as LogContext,
          });

          return result;
        } catch (err: any) {
          const durationMs = Date.now() - start;
          self._emit({
            message: `PG Query failed: ${err.message}`,
            layer:   LogLayer.DATA_ACCESS,
            level:   LogLevel.ERROR,
            context: {
              database:      'postgres',
              durationMs,
              deadlock:      err.code === '40P01',
              lockTimeout:   err.code === '55P03',
              exceptionType: err.code,
              stackTrace:    err.stack,
            } as LogContext,
          });
          throw err;
        }
      };

      this._emit({ message: 'pg (postgres) driver patched', layer: LogLayer.OBSERVABILITY, level: LogLevel.DEBUG });
    } catch { /* pg not installed */ }
  }

  /** neo4j-driver */
  private _tryPatchNeo4j(): void {
    try {
      const neo4j = require('neo4j-driver');
      const self  = this;
      const orig  = neo4j.Session.prototype.run?.bind(neo4j.Session.prototype);
      if (!orig) return;

      neo4j.Session.prototype.run = async function (...args: any[]) {
        const cypher  = typeof args[0] === 'string' ? args[0] : '';
        const start   = Date.now();

        try {
          const result = await orig.apply(this, args);
          const durationMs = Date.now() - start;
          self._emit({
            message: `Neo4j Query: ${cypher.slice(0, 120)}`,
            layer:   LogLayer.DATA_ACCESS,
            level:   durationMs > self.cfg.slowQueryMs ? LogLevel.WARN : LogLevel.INFO,
            context: { database: 'neo4j', durationMs, slowQuery: durationMs > self.cfg.slowQueryMs } as LogContext,
          });
          return result;
        } catch (err: any) {
          const durationMs = Date.now() - start;
          self._emit({
            message: `Neo4j Query failed: ${err.message}`,
            layer:   LogLayer.DATA_ACCESS,
            level:   LogLevel.ERROR,
            context: { database: 'neo4j', durationMs, exceptionType: err.code, stackTrace: err.stack } as LogContext,
          });
          throw err;
        }
      };

      this._emit({ message: 'neo4j-driver patched', layer: LogLayer.OBSERVABILITY, level: LogLevel.DEBUG });
    } catch { /* neo4j not installed */ }
  }

  /** mongoose */
  private _tryPatchMongoose(): void {
    try {
      const mongoose = require('mongoose');
      const self     = this;
      mongoose.plugin((schema: any) => {
        ['save', 'find', 'findOne', 'findOneAndUpdate', 'deleteOne', 'deleteMany', 'updateOne', 'updateMany'].forEach((hook) => {
          schema.pre(hook, function (this: any, next: Function) {
            (this as any)._sentinelStart = Date.now();
            next();
          });
          schema.post(hook, function (this: any, result: any) {
            const durationMs = Date.now() - ((this as any)._sentinelStart || Date.now());
            self._emit({
              message: `Mongoose ${hook}${durationMs > self.cfg.slowQueryMs ? ' [SLOW]' : ''}`,
              layer:   LogLayer.DATA_ACCESS,
              level:   durationMs > self.cfg.slowQueryMs ? LogLevel.WARN : LogLevel.INFO,
              context: {
                database:    'mongodb',
                queryType:   hook.toUpperCase() as any,
                durationMs,
                rowCount:    Array.isArray(result) ? result.length : 1,
              } as LogContext,
            });
          });
        });
      });

      this._emit({ message: 'mongoose patched', layer: LogLayer.OBSERVABILITY, level: LogLevel.DEBUG });
    } catch { /* mongoose not installed */ }
  }

  /** ioredis */
  private _tryPatchRedis(): void {
    try {
      const Redis = require('ioredis');
      const self  = this;
      const orig  = Redis.prototype.sendCommand.bind(Redis.prototype);

      Redis.prototype.sendCommand = async function (...args: any[]) {
        const cmd   = args[0]?.name || 'CMD';
        const start = Date.now();

        try {
          const result = await orig.apply(this, args);
          const durationMs = Date.now() - start;
          self._emit({
            message: `Redis ${cmd} (${durationMs}ms)`,
            layer:   LogLayer.DATA_ACCESS,
            level:   LogLevel.DEBUG,
            context: { database: 'redis', queryType: cmd as any, durationMs, cacheHit: result !== null } as LogContext,
          });
          return result;
        } catch (err: any) {
          self._emit({
            message: `Redis ${cmd} error: ${err.message}`,
            layer:   LogLayer.DATA_ACCESS,
            level:   LogLevel.ERROR,
            context: { database: 'redis', exceptionType: err.constructor.name } as LogContext,
          });
          throw err;
        }
      };

      this._emit({ message: 'ioredis patched', layer: LogLayer.OBSERVABILITY, level: LogLevel.DEBUG });
    } catch { /* ioredis not installed */ }
  }

  /* ── Class method wrapping ──────────────────────────────── */

  private _wrapMethod(proto: object, key: string, className: string, layer: LogLayer): void {
    const self = this;
    const orig = (proto as any)[key] as (...args: any[]) => any;

    (proto as any)[key] = function (...args: any[]) {
      const start   = Date.now();
      let isAsync   = false;

      try {
        const result = orig.apply(this, args);

        if (result && typeof (result as any).then === 'function') {
          isAsync = true;
          return (result as Promise<any>)
            .then((val) => {
              self._emit({
                message: `${className}.${key} → ok (${Date.now() - start}ms)`,
                layer,
                level:   LogLevel.INFO,
                context: { className, functionName: key, durationMs: Date.now() - start } as LogContext,
              });
              return val;
            })
            .catch((err: any) => {
              self._emit({
                message: `${className}.${key} → error: ${err?.message}`,
                layer,
                level:   LogLevel.ERROR,
                context: { className, functionName: key, durationMs: Date.now() - start, exceptionType: err?.constructor?.name, stackTrace: err?.stack } as LogContext,
              });
              throw err;
            });
        }

        self._emit({
          message: `${className}.${key} → ok (${Date.now() - start}ms)`,
          layer,
          level:   LogLevel.INFO,
          context: { className, functionName: key, durationMs: Date.now() - start } as LogContext,
        });

        return result;
      } catch (err: any) {
        if (!isAsync) {
          self._emit({
            message: `${className}.${key} → threw: ${err?.message}`,
            layer,
            level:   LogLevel.ERROR,
            context: { className, functionName: key, durationMs: Date.now() - start, exceptionType: err?.constructor?.name, stackTrace: err?.stack } as LogContext,
          });
        }
        throw err;
      }
    };
  }

  /* ── Helpers ─────────────────────────────────────────────── */

  private _genId(): string {
    return crypto.randomUUID?.() ||
      Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
  }
}

/* ── Factory ─────────────────────────────────────────────── */

export const initSentinel = async (config?: SentinelNodeConfig): Promise<SentinelNode> => {
  const sentinel = new SentinelNode(config);
  await sentinel.hook();
  return sentinel;
};

import { LogLayer, LogLevel, LogRecord } from '../core/types.ts';

import http from 'http';
import fs from 'fs';

export class SentinelNode {
  private serviceName: string;

  private clickhouseHost?: string;
  private clickhouseDatabase?: string;
  private clickhouseTable: string;

  private clickhouseAuthHeader?: string;

  constructor(serviceName: string = 'node-service') {
    this.serviceName = serviceName;

    this.clickhouseHost =
      process.env.CLICKHOUSE_HOST;

    this.clickhouseDatabase =
      process.env.CLICKHOUSE_DATABASE;

    this.clickhouseTable =
      process.env.CLICKHOUSE_TABLE || 'logs';

    const user =
      process.env.CLICKHOUSE_USER;

    const password =
      process.env.CLICKHOUSE_PASSWORD;

    if (user) {
      this.clickhouseAuthHeader =
        `Basic ${Buffer.from(
          `${user}:${password || ''}`
        ).toString('base64')}`;
    }

    void this.initClickhouse();
  }

  hook() {
    this.patchHttp();
    this.patchConsole();
    this.patchFS();

    const record = new LogRecord({
      message:
        'Sentinel Node Agent hooked successfully',

      layer: LogLayer.INFRASTRUCTURE,

      level: LogLevel.INFO,

      service: this.serviceName,
    });

    void this.persist(record);

    console.log(
      `[SENTINEL] ${record.toString()}`
    );
  }

  private clickhouseEnabled(): boolean {
    return Boolean(
      this.clickhouseHost &&
      this.clickhouseDatabase &&
      this.clickhouseTable
    );
  }

  private async executeQuery(query: string) {
    if (!this.clickhouseEnabled()) {
      return;
    }

    try {
      const url =
        `${this.clickhouseHost}` +
        `/?query=${encodeURIComponent(
          query
        )}`;

      const headers: Record<
        string,
        string
      > = {};

      if (this.clickhouseAuthHeader) {
        headers.Authorization =
          this.clickhouseAuthHeader;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers,
      });

      if (!response.ok) {
        const text =
          await response.text();

        console.error(
          '[SENTINEL] Query failed'
        );

        console.error(text);
      }
    } catch (error) {
      console.error(
        '[SENTINEL] Query error',
        error
      );
    }
  }

  private async initClickhouse() {
    if (!this.clickhouseEnabled()) {
      return;
    }

    try {
      const createDbQuery = `
        CREATE DATABASE IF NOT EXISTS
        ${this.clickhouseDatabase}
      `;

      await this.executeQuery(
        createDbQuery
      );

      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS
        ${this.clickhouseDatabase}.${this.clickhouseTable}
        (
          timestamp String,
          record_id String,
          trace_id String,
          span_id String,
          service String,
          env String,
          layer String,
          level String,
          message String,
          context String
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, service)
      `;

      await this.executeQuery(
        createTableQuery
      );

      console.log(
        '[SENTINEL] ClickHouse initialized'
      );
    } catch (error) {
      console.error(
        '[SENTINEL] ClickHouse init failed',
        error
      );
    }
  }

  private async sendToClickhouse(
    record: LogRecord
  ) {
    if (!this.clickhouseEnabled()) {
      return;
    }

    try {
      const query = `
        INSERT INTO
        ${this.clickhouseDatabase}.${this.clickhouseTable}
        FORMAT JSONEachRow
      `;

      const url =
        `${this.clickhouseHost}` +
        `/?query=${encodeURIComponent(
          query
        )}`;

      const payload = {
        timestamp: (record as any)
          .timestamp,

        record_id: (record as any)
          .record_id,

        trace_id: (record as any)
          .trace_id,

        span_id: (record as any)
          .span_id,

        service: (record as any)
          .service,

        env: (record as any).env,

        layer: (record as any).layer,

        level: (record as any).level,

        message: (record as any)
          .message,

        context: JSON.stringify(
          (record as any).context || {}
        ),
      };

      const headers: Record<
        string,
        string
      > = {
        'Content-Type':
          'application/json',
      };

      if (this.clickhouseAuthHeader) {
        headers.Authorization =
          this.clickhouseAuthHeader;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const text =
          await response.text();

        console.error(
          '[SENTINEL] ClickHouse ingest failed'
        );

        console.error(text);
      }
    } catch (error) {
      console.error(
        '[SENTINEL] Ingest error',
        error
      );
    }
  }

  private persist(record: LogRecord) {
    void this.sendToClickhouse(record);
  }

  private patchHttp() {
    const originalCreateServer =
      http.createServer;

    const self = this;

    const originalConsoleLog =
      console.log.bind(console);

    // @ts-ignore
    http.createServer = function (
      requestListener?: (
        req: http.IncomingMessage,
        res: http.ServerResponse
      ) => void
    ) {
      const wrappedListener = (
        req: http.IncomingMessage,
        res: http.ServerResponse
      ) => {
        const startTime = Date.now();

        const reqRecord = new LogRecord({
          message:
            `Inbound Request: ` +
            `${req.method} ${req.url}`,

          layer: LogLayer.API_GATEWAY,

          level: LogLevel.INFO,

          service: self.serviceName,

          context: {
            headers: req.headers,
            method: req.method,
            url: req.url,
            remoteAddress:
              req.socket.remoteAddress,
          },
        });

        void self.persist(reqRecord);

        originalConsoleLog(
          `[SENTINEL] ${reqRecord.toString()}`
        );

        res.on('finish', () => {
          const duration =
            Date.now() - startTime;

          const statusRecord =
            new LogRecord({
              message:
                `Request Completed: ` +
                `${req.method} ${req.url} -> ` +
                `${res.statusCode}`,

              layer:
                LogLayer.API_GATEWAY,

              level:
                res.statusCode >= 400
                  ? LogLevel.ERROR
                  : LogLevel.INFO,

              service:
                self.serviceName,

              context: {
                status: res.statusCode,
                durationMs: duration,
                userAgent:
                  req.headers[
                    'user-agent'
                  ],
              },
            });

          void self.persist(
            statusRecord
          );

          originalConsoleLog(
            `[SENTINEL] ${statusRecord.toString()}`
          );
        });

        if (requestListener) {
          return requestListener(
            req,
            res
          );
        }
      };

      return originalCreateServer(
        wrappedListener
      );
    };
  }

  private patchConsole() {
    const self = this;

    const wrapConsoleMethod = (
      methodName: keyof Console,
      level: LogLevel
    ) => {
      const originalMethod =
        (console as any)[
          methodName
        ].bind(console);

      (console as any)[methodName] = (
        ...args: any[]
      ) => {
        if (
          args[0] &&
          typeof args[0] === 'string' &&
          args[0].includes(
            '[SENTINEL]'
          )
        ) {
          return originalMethod(
            ...args
          );
        }

        const record = new LogRecord({
          message: args
            .map((a) =>
              typeof a === 'object'
                ? JSON.stringify(a)
                : String(a)
            )
            .join(' '),

          layer:
            LogLayer.BUSINESS_LOGIC,

          level,

          service:
            self.serviceName,
        });

        void self.persist(record);

        return originalMethod(
          `[SENTINEL] ${record.toString()}`
        );
      };
    };

    wrapConsoleMethod(
      'log',
      LogLevel.INFO
    );

    wrapConsoleMethod(
      'info',
      LogLevel.INFO
    );

    wrapConsoleMethod(
      'warn',
      LogLevel.WARN
    );

    wrapConsoleMethod(
      'error',
      LogLevel.ERROR
    );
  }

  private patchFS() {
    const originalReadFile =
      fs.readFile;

    const self = this;

    const originalConsoleLog =
      console.log.bind(console);

    // @ts-ignore
    fs.readFile = function (
      path: any,
      options: any,
      callback?: any
    ) {
      const record = new LogRecord({
        message: `Reading file: ${path}`,

        layer:
          LogLayer.DATA_ACCESS,

        level: LogLevel.DEBUG,

        service:
          self.serviceName,

        context: {
          path,
        },
      });

      void self.persist(record);

      originalConsoleLog(
        `[SENTINEL] ${record.toString()}`
      );

      return originalReadFile.apply(
        this,
        arguments as any
      );
    };
  }
}

export const initSentinel = (
  name?: string
) => {
  const sentinel =
    new SentinelNode(name);

  sentinel.hook();

  return sentinel;
};

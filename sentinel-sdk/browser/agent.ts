import { LogLayer, LogLevel, LogRecord } from '../core/types.ts';

export class SentinelBrowser {
  private serviceName: string;

  private clickhouseHost: string;
  private clickhouseDatabase: string;
  private clickhouseTable: string;

  private clickhouseUser?: string;
  private clickhousePassword?: string;

  private authHeader?: string;

  constructor(serviceName: string = 'browser-app') {
    this.serviceName = serviceName;

    this.clickhouseHost =
      (window as any).__SENTINEL_CLICKHOUSE_HOST__ ||
      'http://localhost:8123';

    this.clickhouseDatabase =
      (window as any).__SENTINEL_CLICKHOUSE_DATABASE__ ||
      'sentinel';

    this.clickhouseTable =
      (window as any).__SENTINEL_CLICKHOUSE_TABLE__ ||
      'logs';

    this.clickhouseUser =
      (window as any).__SENTINEL_CLICKHOUSE_USER__;

    this.clickhousePassword =
      (window as any).__SENTINEL_CLICKHOUSE_PASSWORD__;

    if (this.clickhouseUser) {
      this.authHeader = `Basic ${btoa(
        `${this.clickhouseUser}:${this.clickhousePassword || ''}`
      )}`;
    }

    void this.initClickhouse();
  }

  hook() {
    this.patchFetch();
    this.hookEvents();
    this.hookErrors();
    this.monitorVitals();

    const record = new LogRecord({
      message: 'Sentinel Browser Agent hooked successfully',
      layer: LogLayer.PRESENTATION,
      level: LogLevel.INFO,
      service: this.serviceName,
    });

    this.log(record);
  }

  private async executeQuery(query: string) {
    try {
      const url = `${this.clickhouseHost}/?query=${encodeURIComponent(
        query
      )}`;

      const headers: Record<string, string> = {};

      if (this.authHeader) {
        headers.Authorization = this.authHeader;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers,
      });

      if (!response.ok) {
        console.error(
          '[SENTINEL] ClickHouse query failed:',
          response.status
        );
      }
    } catch (err) {
      console.error('[SENTINEL] Query error:', err);
    }
  }

  private async initClickhouse() {
    try {
      const createDbQuery = `
        CREATE DATABASE IF NOT EXISTS ${this.clickhouseDatabase}
      `;

      await this.executeQuery(createDbQuery);

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

      await this.executeQuery(createTableQuery);

      console.log(
        '[SENTINEL] Browser ClickHouse initialized'
      );
    } catch (err) {
      console.error(
        '[SENTINEL] ClickHouse init failed:',
        err
      );
    }
  }

  private async sendToClickhouse(record: LogRecord) {
    try {
      const query = `
        INSERT INTO
        ${this.clickhouseDatabase}.${this.clickhouseTable}
        FORMAT JSONEachRow
      `;

      const url = `${this.clickhouseHost}/?query=${encodeURIComponent(
        query
      )}`;

      const payload = {
        timestamp: (record as any).timestamp,
        record_id: (record as any).record_id,
        trace_id: (record as any).trace_id,
        span_id: (record as any).span_id,
        service: (record as any).service,
        env: (record as any).env,
        layer: (record as any).layer,
        level: (record as any).level,
        message: (record as any).message,
        context: JSON.stringify(
          (record as any).context || {}
        ),
      };

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      };

      if (this.authHeader) {
        headers.Authorization = this.authHeader;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        console.error(
          '[SENTINEL] ClickHouse ingest failed:',
          response.status
        );
      }
    } catch (err) {
      console.error(
        '[SENTINEL] Failed to write log:',
        err
      );
    }
  }

  private log(record: LogRecord) {
    console.log(record.toString());

    void this.sendToClickhouse(record);
  }

  private patchFetch() {
    const originalFetch = window.fetch.bind(window);

    const self = this;

    const wrappedFetch = async (...args: any[]) => {
      const [resource, config] = args;

      const startTime = performance.now();

      const record = new LogRecord({
        message: `Fetch Request: ${resource}`,
        layer: LogLayer.SERVICE,
        level: LogLevel.INFO,
        service: self.serviceName,
        context: {
          resource,
          method: config?.method || 'GET',
        },
      });

      self.log(record);

      try {
        const response = await originalFetch(...args);

        const duration =
          performance.now() - startTime;

        const resRecord = new LogRecord({
          message: `Fetch Completed: ${resource} -> ${response.status}`,

          layer: LogLayer.SERVICE,

          level: response.ok
            ? LogLevel.INFO
            : LogLevel.ERROR,

          service: self.serviceName,

          context: {
            status: response.status,
            durationMs: duration,
          },
        });

        self.log(resRecord);

        return response;
      } catch (error) {
        const errRecord = new LogRecord({
          message: `Fetch Failed: ${resource}`,

          layer: LogLayer.SERVICE,

          level: LogLevel.ERROR,

          service: self.serviceName,

          context: {
            error: String(error),
          },
        });

        self.log(errRecord);

        throw error;
      }
    };

    try {
      Object.defineProperty(window, 'fetch', {
        value: wrappedFetch,
        configurable: true,
        writable: true,
        enumerable: true,
      });
    } catch (e) {
      (window as any).fetch = wrappedFetch;
    }
  }

  private hookEvents() {
    const self = this;

    ['click', 'submit', 'scroll'].forEach(
      (eventType) => {
        window.addEventListener(
          eventType,
          (e) => {
            const target =
              e.target as HTMLElement;

            const record = new LogRecord({
              message: `User Interaction: ${eventType} on ${
                target.tagName || 'window'
              }`,

              layer: LogLayer.PRESENTATION,

              level: LogLevel.INFO,

              service: self.serviceName,

              context: {
                eventType,
                id: target.id,
                className: target.className,
                text: target.innerText?.substring(
                  0,
                  50
                ),
              },
            });

            self.log(record);
          },
          {
            capture: true,
            passive: true,
          }
        );
      }
    );
  }

  private hookErrors() {
    const self = this;

    window.onerror = (
      message,
      source,
      lineno,
      colno,
      error
    ) => {
      const record = new LogRecord({
        message: `Frontend Error: ${message}`,

        layer: LogLayer.SECURITY,

        level: LogLevel.FATAL,

        service: self.serviceName,

        context: {
          source,
          lineno,
          colno,
          stack: error?.stack,
        },
      });

      self.log(record);
    };

    window.onunhandledrejection = (
      event
    ) => {
      const record = new LogRecord({
        message: `Unhandled Promise Rejection: ${event.reason}`,

        layer: LogLayer.OBSERVABILITY,

        level: LogLevel.ERROR,

        service: self.serviceName,

        context: {
          reason: event.reason,
        },
      });

      self.log(record);
    };
  }

  private monitorVitals() {
    if ('PerformanceObserver' in window) {
      const observer = new PerformanceObserver(
        (list) => {
          list.getEntries().forEach((entry) => {
            const record = new LogRecord({
              message: `Web Vital: ${entry.name}`,

              layer: LogLayer.PRESENTATION,

              level: LogLevel.INFO,

              service: this.serviceName,

              context: {
                value:
                  (entry as any).value ||
                  (entry as any).startTime,

                entryType: entry.entryType,
              },
            });

            this.log(record);
          });
        }
      );

      observer.observe({
        entryTypes: [
          'paint',
          'largest-contentful-paint',
          'layout-shift',
          'navigation',
        ],
      });
    }
  }
}

export const initBrowserSentinel = (
  name?: string
) => {
  const sentinel = new SentinelBrowser(name);

  sentinel.hook();

  return sentinel;
};

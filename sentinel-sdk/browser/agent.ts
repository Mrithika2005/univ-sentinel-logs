/* ============================================================
   SENTINEL SDK — Browser Agent
   Auto-instruments: fetch, XHR, clicks, scroll, navigation,
   vitals, errors, class prototypes, Angular & React detection
   Sends logs → /sentinel/ingest relay (server.ts)
   ============================================================ */

import {
  LogLayer,
  LogLevel,
  LogRecord,
  inferLayer,
  type InstrumentedClassMeta,
  type LogContext,
} from '../core/types.ts';

/* ── Config ──────────────────────────────────────────────── */

export interface SentinelBrowserConfig {
  serviceName?:  string;
  relayUrl?:     string;   // default: /sentinel/ingest
  batchSize?:    number;   // default: 20
  flushInterval?: number;  // ms, default: 3000
  slowFetchMs?:  number;   // default: 1000
  debug?:        boolean;
  traceId?:      string;   // optional distributed trace id
}

/* ── Main class ──────────────────────────────────────────── */

export class SentinelBrowser {
  private cfg:         Required<SentinelBrowserConfig>;
  private queue:       LogRecord[] = [];
  private flushTimer?: ReturnType<typeof setInterval>;
  private pageStart    = performance.now();
  private navStart     = Date.now();
  private instrumented = new WeakSet<object>();

  constructor(config: SentinelBrowserConfig = {}) {
    this.cfg = {
      serviceName:   config.serviceName   || 'browser-app',
      relayUrl:      config.relayUrl      || '/sentinel/ingest',
      batchSize:     config.batchSize     || 20,
      flushInterval: config.flushInterval || 3000,
      slowFetchMs:   config.slowFetchMs   || 1000,
      debug:         config.debug         || false,
      traceId:       config.traceId       || this._genTraceId(),
    };
  }

  /* ── Public API ─────────────────────────────────────────── */

  hook(): this {
    this._patchFetch();
    this._patchXHR();
    this._hookNavigation();
    this._hookInteractions();
    this._hookErrors();
    this._monitorVitals();
    this._startFlushLoop();
    this._detectFramework();

    this._emit({
      message: `Sentinel Browser Agent hooked on "${this.cfg.serviceName}"`,
      layer:   LogLayer.INFRASTRUCTURE,
      level:   LogLevel.INFO,
      context: { userAgent: navigator.userAgent, url: location.href },
    });

    return this;
  }

  /**
   * Manually instrument any class instance or constructor.
   * Call this on your own classes for zero-effort domain tracing:
   *   sentinel.instrument(myServiceInstance);
   *   sentinel.instrument(MyClass);
   */
  instrument<T extends object>(target: T | (new (...a: any[]) => T), layer?: LogLayer): this {
    const proto = typeof target === 'function' ? target.prototype : Object.getPrototypeOf(target);
    if (!proto || this.instrumented.has(proto)) return this;
    this.instrumented.add(proto);

    const className  = (typeof target === 'function' ? target.name : target.constructor?.name) || 'UnknownClass';
    const resolvedLayer = layer || inferLayer(className);
    const methodNames: string[] = [];

    // Walk the prototype chain (but stop at Object.prototype)
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

    const meta: InstrumentedClassMeta = { className, layer: resolvedLayer, methodNames };
    this._emit({
      message: `Auto-instrumented class: ${className} (${methodNames.length} methods)`,
      layer:   LogLayer.OBSERVABILITY,
      level:   LogLevel.DEBUG,
      context: meta as unknown as LogContext,
    });

    return this;
  }

  /**
   * Auto-scan window for Angular services / React components etc.
   * and instrument everything found.
   */
  autoDiscover(): this {
    // Angular: scan ng module registry
    this._discoverAngular();
    // Generic: scan window for class-like objects
    this._discoverWindowGlobals();
    return this;
  }

  /** Manually emit a structured log. */
  log(partial: Partial<LogRecord> & { message: string }): void {
    this._emit(partial);
  }

  /** Force-flush the queue immediately. */
  flush(): Promise<void> {
    return this._flush();
  }

  /* ── Internal emitter ───────────────────────────────────── */

  private _emit(partial: Partial<LogRecord> & { message: string }): void {
    const record = new LogRecord({
      ...partial,
      service:  this.cfg.serviceName,
      trace_id: partial.trace_id || this.cfg.traceId,
    });

    if (this.cfg.debug) {
      // Use a colour per level in console
      const lvlMap: Record<LogLevel, 'log' | 'warn' | 'error' | 'debug'> = {
        [LogLevel.DEBUG]: 'debug',
        [LogLevel.INFO]:  'log',
        [LogLevel.WARN]:  'warn',
        [LogLevel.ERROR]: 'error',
        [LogLevel.FATAL]: 'error',
      };
      console[lvlMap[record.level]](`[SENTINEL]`, record.to_dict());
    }

    this.queue.push(record);

    if (this.queue.length >= this.cfg.batchSize) {
      void this._flush();
    }
  }

  private async _flush(): Promise<void> {
    if (this.queue.length === 0) return;
    const batch = this.queue.splice(0, this.queue.length);

    try {
      const res = await fetch(this.cfg.relayUrl, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'X-Sentinel': '1' },
        body:    JSON.stringify(batch.map((r) => r.to_dict())),
        // keepalive lets the request survive page unload
        keepalive: true,
      });

      if (!res.ok && this.cfg.debug) {
        console.warn('[SENTINEL] relay rejected batch:', res.status);
      }
    } catch (err) {
      if (this.cfg.debug) console.error('[SENTINEL] flush error:', err);
      // Put them back so we don't lose logs on a network blip
      this.queue.unshift(...batch);
    }
  }

  private _startFlushLoop(): void {
    this.flushTimer = setInterval(() => void this._flush(), this.cfg.flushInterval);

    // Flush on tab close / navigate away
    window.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'hidden') void this._flush();
    });
    window.addEventListener('beforeunload', () => void this._flush());
  }

  /* ── Fetch patch ────────────────────────────────────────── */

  private _patchFetch(): void {
    const orig = window.fetch.bind(window);
    const self  = this;

    const wrapped = async (...args: Parameters<typeof fetch>): Promise<Response> => {
      const [resource, init] = args;

      // Don't intercept our own relay calls
      const url = typeof resource === 'string' ? resource : (resource as Request).url;
      if (url.includes(self.cfg.relayUrl) || url.includes('X-Sentinel')) {
        return orig(...args);
      }

      const method    = init?.method || 'GET';
      const startTime = performance.now();

      self._emit({
        message: `→ ${method} ${url}`,
        layer:   LogLayer.API_GATEWAY,
        level:   LogLevel.INFO,
        context: {
          method,
          path:                url,
          requestSizeBytes:    typeof init?.body === 'string' ? init.body.length : 0,
        } as LogContext,
      });

      try {
        const response = await orig(...args);
        const durationMs = performance.now() - startTime;
        const isError    = !response.ok;
        const isSlow     = durationMs > self.cfg.slowFetchMs;

        self._emit({
          message: `← ${method} ${url} ${response.status} (${durationMs.toFixed(1)}ms)${isSlow ? ' [SLOW]' : ''}`,
          layer:   LogLayer.API_GATEWAY,
          level:   isError ? LogLevel.ERROR : isSlow ? LogLevel.WARN : LogLevel.INFO,
          context: {
            method,
            path:       url,
            statusCode: response.status,
            durationMs,
            slowQuery:  isSlow,
            slowQueryThresholdMs: self.cfg.slowFetchMs,
          } as LogContext,
        });

        return response;
      } catch (err) {
        const durationMs = performance.now() - startTime;
        self._emit({
          message: `✗ ${method} ${url} — network error after ${durationMs.toFixed(1)}ms`,
          layer:   LogLayer.API_GATEWAY,
          level:   LogLevel.ERROR,
          context: { method, path: url, durationMs, exceptionType: String(err) } as LogContext,
        });
        throw err;
      }
    };

    Object.defineProperty(window, 'fetch', { value: wrapped, configurable: true, writable: true });
  }

  /* ── XHR patch ──────────────────────────────────────────── */

  private _patchXHR(): void {
    const OrigXHR = window.XMLHttpRequest;
    const self    = this;

    class SentinelXHR extends OrigXHR {
      private _method = 'GET';
      private _url    = '';
      private _start  = 0;

      open(method: string, url: string | URL, ...rest: any[]): void {
        this._method = method;
        this._url    = String(url);
        super.open(method, url, ...rest);
      }

      send(body?: Document | XMLHttpRequestBodyInit | null): void {
        this._start = performance.now();
        self._emit({
          message: `XHR → ${this._method} ${this._url}`,
          layer:   LogLayer.API_GATEWAY,
          level:   LogLevel.INFO,
          context: { method: this._method, path: this._url } as LogContext,
        });

        this.addEventListener('loadend', () => {
          const durationMs = performance.now() - this._start;
          self._emit({
            message: `XHR ← ${this._method} ${this._url} ${this.status} (${durationMs.toFixed(1)}ms)`,
            layer:   LogLayer.API_GATEWAY,
            level:   this.status >= 400 ? LogLevel.ERROR : LogLevel.INFO,
            context: { method: this._method, path: this._url, statusCode: this.status, durationMs } as LogContext,
          });
        });

        super.send(body);
      }
    }

    (window as any).XMLHttpRequest = SentinelXHR;
  }

  /* ── Navigation ─────────────────────────────────────────── */

  private _hookNavigation(): void {
    const self = this;

    // Page load time
    window.addEventListener('load', () => {
      const loadTimeMs = performance.now();
      self._emit({
        message: `Page loaded: ${location.pathname} in ${loadTimeMs.toFixed(1)}ms`,
        layer:   LogLayer.PRESENTATION,
        level:   loadTimeMs > 3000 ? LogLevel.WARN : LogLevel.INFO,
        context: { page: location.pathname, renderTimeMs: loadTimeMs } as LogContext,
      });
    });

    // SPA navigation via History API
    const origPushState    = history.pushState.bind(history);
    const origReplaceState = history.replaceState.bind(history);

    history.pushState = (...args) => {
      origPushState(...args);
      self._onNavigate('pushState');
    };
    history.replaceState = (...args) => {
      origReplaceState(...args);
      self._onNavigate('replaceState');
    };

    window.addEventListener('popstate', () => self._onNavigate('popstate'));
  }

  private _onNavigate(trigger: string): void {
    const sessionDuration = (Date.now() - this.navStart) / 1000;
    this._emit({
      message: `Navigation: ${trigger} → ${location.pathname}`,
      layer:   LogLayer.PRESENTATION,
      level:   LogLevel.INFO,
      context: { page: location.pathname, sessionDuration, interactionType: 'navigate' } as LogContext,
    });
  }

  /* ── Interaction events ─────────────────────────────────── */

  private _hookInteractions(): void {
    const self = this;

    // Click
    window.addEventListener('click', (e) => {
      const t = e.target as HTMLElement;
      self._emit({
        message: `Click: <${t.tagName?.toLowerCase()}>${t.id ? '#' + t.id : ''}`,
        layer:   LogLayer.PRESENTATION,
        level:   LogLevel.INFO,
        context: {
          interactionType: 'click',
          elementTag:  t.tagName,
          elementId:   t.id,
          elementText: t.innerText?.slice(0, 60),
        } as LogContext,
      });
    }, { capture: true, passive: true });

    // Scroll depth (debounced)
    let maxScroll = 0;
    let scrollTimer: ReturnType<typeof setTimeout>;
    window.addEventListener('scroll', () => {
      clearTimeout(scrollTimer);
      scrollTimer = setTimeout(() => {
        const depth = Math.round(
          ((window.scrollY + window.innerHeight) / document.documentElement.scrollHeight) * 100
        );
        if (depth > maxScroll) {
          maxScroll = depth;
          self._emit({
            message: `Scroll depth: ${depth}%`,
            layer:   LogLayer.PRESENTATION,
            level:   LogLevel.INFO,
            context: { interactionType: 'scroll', scrollDepthPercent: depth } as LogContext,
          });
        }
      }, 500);
    }, { passive: true });

    // Form submit
    window.addEventListener('submit', (e) => {
      const t   = e.target as HTMLFormElement;
      const id  = t.id || t.getAttribute('name') || 'unknown-form';
      self._emit({
        message: `Form submitted: ${id}`,
        layer:   LogLayer.PRESENTATION,
        level:   LogLevel.INFO,
        context: { interactionType: 'submit', elementId: id, elementTag: 'FORM' } as LogContext,
      });
    }, { capture: true });
  }

  /* ── Errors ─────────────────────────────────────────────── */

  private _hookErrors(): void {
    const self = this;

    window.addEventListener('error', (e) => {
      // Asset load failures (img, script, link)
      if (e.target && (e.target as HTMLElement).tagName) {
        const t = e.target as HTMLElement;
        self._emit({
          message: `Asset load failure: ${(t as any).src || (t as any).href}`,
          layer:   LogLayer.PRESENTATION,
          level:   LogLevel.ERROR,
          context: { elementTag: t.tagName, assetUrl: (t as any).src || (t as any).href, errorType: 'asset_load' } as LogContext,
        });
        return;
      }

      // JS errors
      self._emit({
        message: `JS Error: ${e.message}`,
        layer:   LogLayer.SECURITY,
        level:   LogLevel.FATAL,
        context: {
          errorType:  'js_error',
          assetUrl:   e.filename,
          stackTrace: e.error?.stack,
        } as LogContext,
      });
    }, true);

    window.addEventListener('unhandledrejection', (e) => {
      self._emit({
        message: `Unhandled Promise Rejection: ${e.reason}`,
        layer:   LogLayer.OBSERVABILITY,
        level:   LogLevel.ERROR,
        context: { errorType: 'unhandled_rejection', exceptionType: String(e.reason) } as LogContext,
      });
    });
  }

  /* ── Web Vitals ─────────────────────────────────────────── */

  private _monitorVitals(): void {
    if (!('PerformanceObserver' in window)) return;
    const self = this;

    const vitals = ['paint', 'largest-contentful-paint', 'layout-shift', 'navigation', 'resource', 'longtask'];

    vitals.forEach((type) => {
      try {
        const obs = new PerformanceObserver((list) => {
          list.getEntries().forEach((entry) => {
            const value = (entry as any).value ?? (entry as any).duration ?? entry.startTime;
            const isSlow = type === 'longtask' || (type === 'largest-contentful-paint' && value > 2500);

            self._emit({
              message: `Web Vital [${entry.name || type}]: ${value.toFixed(2)}ms`,
              layer:   LogLayer.PRESENTATION,
              level:   isSlow ? LogLevel.WARN : LogLevel.INFO,
              context: {
                metricName:  entry.name || type,
                metricValue: value,
                metricUnit:  'ms',
                renderTimeMs: type === 'navigation' ? value : undefined,
              } as LogContext,
            });

            // Resource failures
            if (type === 'resource' && (entry as PerformanceResourceTiming).responseStatus >= 400) {
              self._emit({
                message: `Asset load failure (status ${(entry as PerformanceResourceTiming).responseStatus}): ${entry.name}`,
                layer:   LogLayer.PRESENTATION,
                level:   LogLevel.ERROR,
                context: { assetUrl: entry.name, statusCode: (entry as PerformanceResourceTiming).responseStatus } as LogContext,
              });
            }
          });
        });
        obs.observe({ type, buffered: true } as any);
      } catch {
        // Some browsers don't support all entry types — silently skip
      }
    });
  }

  /* ── Auto-instrument class methods ─────────────────────── */

  private _wrapMethod(proto: object, key: string, className: string, layer: LogLayer): void {
    const self   = this;
    const orig   = (proto as any)[key] as (...args: any[]) => any;

    (proto as any)[key] = function (...args: any[]) {
      const start = performance.now();
      let isAsync = false;

      try {
        const result = orig.apply(this, args);

        if (result && typeof result.then === 'function') {
          isAsync = true;
          return result
            .then((val: any) => {
              const durationMs = performance.now() - start;
              self._emit({
                message: `${className}.${key} completed (async, ${durationMs.toFixed(1)}ms)`,
                layer,
                level:   LogLevel.INFO,
                context: { className, functionName: key, durationMs } as LogContext,
              });
              return val;
            })
            .catch((err: any) => {
              const durationMs = performance.now() - start;
              self._emit({
                message: `${className}.${key} failed (async): ${err?.message || err}`,
                layer,
                level:   LogLevel.ERROR,
                context: { className, functionName: key, durationMs, exceptionType: err?.constructor?.name, stackTrace: err?.stack } as LogContext,
              });
              throw err;
            });
        }

        const durationMs = performance.now() - start;
        self._emit({
          message: `${className}.${key} completed (${durationMs.toFixed(1)}ms)`,
          layer,
          level:   LogLevel.INFO,
          context: { className, functionName: key, durationMs } as LogContext,
        });

        return result;
      } catch (err: any) {
        if (!isAsync) {
          const durationMs = performance.now() - start;
          self._emit({
            message: `${className}.${key} threw: ${err?.message || err}`,
            layer,
            level:   LogLevel.ERROR,
            context: { className, functionName: key, durationMs, exceptionType: err?.constructor?.name, stackTrace: err?.stack } as LogContext,
          });
        }
        throw err;
      }
    };
  }

  /* ── Framework detection ────────────────────────────────── */

  private _detectFramework(): void {
    // React DevTools hook
    if ((window as any).__REACT_DEVTOOLS_GLOBAL_HOOK__) {
      this._emit({
        message: 'React detected — patching component error boundaries',
        layer:   LogLayer.OBSERVABILITY,
        level:   LogLevel.DEBUG,
        context: { component: 'React' } as LogContext,
      });
      this._hookReact();
    }

    // Angular
    if ((window as any).ng) {
      this._emit({
        message: 'Angular detected — scanning for services',
        layer:   LogLayer.OBSERVABILITY,
        level:   LogLevel.DEBUG,
        context: { component: 'Angular' } as LogContext,
      });
      this._discoverAngular();
    }
  }

  private _hookReact(): void {
    const hook = (window as any).__REACT_DEVTOOLS_GLOBAL_HOOK__;
    if (!hook) return;
    const orig = hook.onCommitFiberRoot?.bind(hook);
    if (!orig) return;
    const self = this;

    hook.onCommitFiberRoot = (...args: any[]) => {
      try {
        const root = args[1];
        const name = root?.current?.type?.displayName || root?.current?.type?.name;
        if (name) {
          self._emit({
            message: `React render: <${name}>`,
            layer:   LogLayer.PRESENTATION,
            level:   LogLevel.DEBUG,
            context: { component: name, renderTimeMs: 0 } as LogContext,
          });
        }
      } catch { /* ignore fiber walk errors */ }
      return orig(...args);
    };
  }

  private _discoverAngular(): void {
    // Angular exposes its DI via ng.probe or via platform injector
    try {
      const ng = (window as any).ng;
      if (!ng) return;
      const root = document.querySelector('[ng-version]') || document.querySelector('app-root');
      if (!root) return;
      const ctx = ng.getContext?.(root) || ng.probe?.(root)?.componentInstance;
      if (ctx) this.instrument(ctx);
    } catch { /* Angular not ready yet */ }
  }

  private _discoverWindowGlobals(): void {
    // Look for class instances attached to window (common in IoT / legacy apps)
    Object.keys(window).forEach((key) => {
      try {
        const val = (window as any)[key];
        if (
          val &&
          typeof val === 'object' &&
          val.constructor &&
          val.constructor !== Object &&
          val.constructor !== Array &&
          val.constructor !== Function &&
          !this.instrumented.has(Object.getPrototypeOf(val))
        ) {
          this.instrument(val);
        }
      } catch { /* some window properties throw on access */ }
    });
  }

  /* ── Helpers ─────────────────────────────────────────────── */

  private _genTraceId(): string {
    return Array.from({ length: 16 }, () => Math.floor(Math.random() * 16).toString(16)).join('');
  }
}

/* ── Factory ─────────────────────────────────────────────── */

export const initBrowserSentinel = (config?: SentinelBrowserConfig): SentinelBrowser => {
  const sentinel = new SentinelBrowser(config);
  sentinel.hook();
  return sentinel;
};

/**
 * Sentinel Core Types & LogRecord Schema
 */

export enum LogLayer {
  PRESENTATION = 'presentation',
  API_GATEWAY = 'api_gateway',
  BUSINESS_LOGIC = 'business_logic',
  DATA_ACCESS = 'data_access',
  SERVICE = 'service',
  SECURITY = 'security',
  OBSERVABILITY = 'observability',
  INFRASTRUCTURE = 'infrastructure',
}

export enum LogLevel {
  INFO = 'INFO',
  WARN = 'WARN',
  ERROR = 'ERROR',
  DEBUG = 'DEBUG',
  FATAL = 'FATAL',
}

export interface LogContext {
  [key: string]: any;
}

export class LogRecord {
  message: string;
  level: LogLevel;
  layer: LogLayer;
  timestamp: string;
  record_id: string;
  trace_id: string;
  span_id: string;
  service: string;
  env: string;
  context: LogContext;

  constructor(data: Partial<LogRecord>) {
    this.message = data.message || '';
    this.level = data.level || LogLevel.INFO;
    this.layer = data.layer || LogLayer.BUSINESS_LOGIC;
    this.timestamp = data.timestamp || new Date().toISOString();
    this.record_id = data.record_id || Math.random().toString(36).substring(2, 15);
    this.trace_id = data.trace_id || 'untracked';
    this.span_id = data.span_id || 'untracked';
    this.service = data.service || 'unknown-service';
    this.env = data.env || process.env.NODE_ENV || 'development';
    this.context = data.context || {};
  }

  enrich(extra: LogContext): this {
    this.context = { ...this.context, ...extra };
    return this;
  }

  to_dict(): object {
    return {
      message: this.message,
      level: this.level,
      layer: this.layer,
      timestamp: this.timestamp,
      record_id: this.record_id,
      trace_id: this.trace_id,
      span_id: this.span_id,
      service: this.service,
      env: this.env,
      context: this.context,
    };
  }

  toString(): string {
    const color = this.level === LogLevel.ERROR ? '\x1b[31m' : this.level === LogLevel.WARN ? '\x1b[33m' : '\x1b[32m';
    const reset = '\x1b[0m';
    return `${color}[${this.timestamp}] [${this.layer.toUpperCase()}] [${this.level}] ${this.message}${reset}`;
  }
}

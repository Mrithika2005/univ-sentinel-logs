/* ============================================================
   SENTINEL SDK — Core Types & LogRecord Schema
   Supports: React, Angular, Node, Python (via Python agent)
   Layers: All 8 production layers, fully typed
   ============================================================ */

export enum LogLayer {
  PRESENTATION   = 'presentation',    // Frontend / UI
  API_GATEWAY    = 'api_gateway',     // HTTP gateway, routing
  BUSINESS_LOGIC = 'business_logic',  // App-level logic
  DATA_ACCESS    = 'data_access',     // DB, cache, persistence
  SERVICE        = 'service',         // Third-party / internal services
  SECURITY       = 'security',        // Auth, WAF, compliance
  OBSERVABILITY  = 'observability',   // Traces, metrics, alerts
  INFRASTRUCTURE = 'infrastructure',  // CPU, memory, containers
  DOMAIN         = 'domain',          // DDD aggregates, sagas, events
}

export enum LogLevel {
  DEBUG = 'DEBUG',
  INFO  = 'INFO',
  WARN  = 'WARN',
  ERROR = 'ERROR',
  FATAL = 'FATAL',
}

/* ── Per-layer rich context shapes ───────────────────────── */

export interface PresentationContext {
  page?: string;
  component?: string;
  sessionDuration?: number;
  renderTimeMs?: number;
  interactionType?: 'click' | 'scroll' | 'submit' | 'navigate' | 'focus' | string;
  elementId?: string;
  elementTag?: string;
  elementText?: string;
  featureFlag?: string;
  flagValue?: boolean | string;
  assetUrl?: string;
  errorType?: string;
  accessibilityIssue?: string;
  scrollDepthPercent?: number;
  cacheHit?: boolean;
  [key: string]: any;
}

export interface ApiGatewayContext {
  method?: string;
  path?: string;
  statusCode?: number;
  durationMs?: number;
  requestId?: string;
  clientIp?: string;
  geoRegion?: string;
  userAgent?: string;
  userId?: string;
  sessionId?: string;
  requestSizeBytes?: number;
  responseSizeBytes?: number;
  tlsVersion?: string;
  tlsHandshakeMs?: number;
  upstreamService?: string;
  rateLimitHit?: boolean;
  rateLimitRemaining?: number;
  corsViolation?: boolean;
  botSignal?: boolean;
  authEvent?: 'login' | 'logout' | 'token_refresh' | 'denied' | string;
  [key: string]: any;
}

export interface BusinessLogicContext {
  functionName?: string;
  className?: string;
  module?: string;
  durationMs?: number;
  inputSummary?: string;
  outputSummary?: string;
  cacheHit?: boolean;
  cacheMiss?: boolean;
  featureFlag?: string;
  flagValue?: boolean | string;
  jobId?: string;
  jobName?: string;
  circuitBreakerState?: 'open' | 'closed' | 'half-open';
  thirdPartyService?: string;
  queueName?: string;
  queueAction?: 'publish' | 'consume';
  configKey?: string;
  configValue?: string;
  fileOperation?: string;
  filePath?: string;
  exceptionType?: string;
  stackTrace?: string;
  [key: string]: any;
}

export interface DataAccessContext {
  queryType?: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'MERGE' | string;
  table?: string;
  collection?: string;
  database?: string;
  durationMs?: number;
  rowsAffected?: number;
  rowCount?: number;
  slowQuery?: boolean;
  slowQueryThresholdMs?: number;
  deadlock?: boolean;
  lockTimeout?: boolean;
  replicationLagMs?: number;
  indexMiss?: boolean;
  migrationName?: string;
  cacheEviction?: boolean;
  storageUsedBytes?: number;
  storageCapacityBytes?: number;
  connectionPoolSize?: number;
  connectionPoolUsed?: number;
  backupStatus?: 'started' | 'completed' | 'failed';
  transactionAction?: 'commit' | 'rollback';
  queryHash?: string;
  [key: string]: any;
}

export interface DomainContext {
  aggregateType?: string;
  aggregateId?: string;
  eventType?: string;
  eventVersion?: number;
  previousState?: string;
  newState?: string;
  policyName?: string;
  policyResult?: boolean | string;
  invariantName?: string;
  invariantViolated?: boolean;
  riskScore?: number;
  fraudSignal?: string;
  discountCode?: string;
  discountAmount?: number;
  entityType?: string;
  entityId?: string;
  consentType?: string;
  consentGranted?: boolean;
  sagaId?: string;
  sagaStep?: string;
  sagaStatus?: 'started' | 'completed' | 'compensating' | 'failed';
  slaBreach?: boolean;
  slaThresholdMs?: number;
  auditUserId?: string;
  auditAction?: string;
  [key: string]: any;
}

export interface ObservabilityContext {
  alertName?: string;
  alertStatus?: 'fired' | 'resolved';
  traceId?: string;
  spanId?: string;
  parentSpanId?: string;
  metricName?: string;
  metricValue?: number;
  metricUnit?: string;
  metricIngestionLagMs?: number;
  oncallTeam?: string;
  logVolumeSpike?: boolean;
  samplingDecision?: 'sampled' | 'dropped';
  samplingRate?: number;
  sloBurnRate?: number;
  sloName?: string;
  syntheticCheckName?: string;
  syntheticCheckPassed?: boolean;
  errorRatePercent?: number;
  anomalyType?: string;
  runbookUrl?: string;
  dashboardQueryMs?: number;
  [key: string]: any;
}

export interface SecurityContext {
  userId?: string;
  username?: string;
  authResult?: 'success' | 'failure' | 'mfa_required';
  failureReason?: string;
  privilegeFrom?: string;
  privilegeTo?: string;
  wafRuleId?: string;
  wafRuleName?: string;
  intrusionSignal?: string;
  vulnerabilityId?: string;
  vulnerabilitySeverity?: 'low' | 'medium' | 'high' | 'critical';
  tokenId?: string;
  tokenAction?: 'issued' | 'revoked' | 'expired';
  complianceFramework?: string;
  complianceCheckPassed?: boolean;
  lockoutReason?: string;
  secretName?: string;
  secretAccessedBy?: string;
  exfiltrationSignal?: string;
  firewallRuleId?: string;
  firewallAction?: 'allow' | 'block';
  gdprDataSubject?: string;
  gdprLegalBasis?: string;
  ipAddress?: string;
  geoCountry?: string;
  [key: string]: any;
}

export interface InfrastructureContext {
  cpuPercent?: number;
  memoryUsedBytes?: number;
  memoryTotalBytes?: number;
  networkInBytes?: number;
  networkOutBytes?: number;
  containerId?: string;
  containerName?: string;
  containerEvent?: 'start' | 'stop' | 'restart' | 'oom' | string;
  nodeId?: string;
  nodeStatus?: 'healthy' | 'degraded' | 'down';
  osKernelEvent?: string;
  cloudProvider?: string;
  cloudRegion?: string;
  cloudSpendAnomaly?: boolean;
  estimatedCostUsd?: number;
  spotPreemption?: boolean;
  diskReadBytes?: number;
  diskWriteBytes?: number;
  diskIoWaitMs?: number;
  autoScaleEvent?: 'scale_out' | 'scale_in';
  autoScaleReason?: string;
  certDomain?: string;
  certExpiryDays?: number;
  hardwareFault?: string;
  [key: string]: any;
}

/* ── Union context type ───────────────────────────────────── */

export type LogContext =
  | PresentationContext
  | ApiGatewayContext
  | BusinessLogicContext
  | DataAccessContext
  | DomainContext
  | ObservabilityContext
  | SecurityContext
  | InfrastructureContext
  | { [key: string]: any };

/* ── Auto-instrumentation metadata ───────────────────────── */

export interface InstrumentedClassMeta {
  className: string;
  layer: LogLayer;
  methodNames: string[];
  detectedDomain?: string;
}

/* ── LogRecord ────────────────────────────────────────────── */

export class LogRecord {
  message:   string;
  level:     LogLevel;
  layer:     LogLayer;
  timestamp: string;
  record_id: string;
  trace_id:  string;
  span_id:   string;
  service:   string;
  env:       string;
  context:   LogContext;

  constructor(data: Partial<LogRecord> & { message: string }) {
    this.message   = data.message;
    this.level     = data.level     || LogLevel.INFO;
    this.layer     = data.layer     || LogLayer.BUSINESS_LOGIC;
    this.timestamp = data.timestamp || new Date().toISOString();
    this.record_id = data.record_id || this._genId();
    this.trace_id  = data.trace_id  || 'untracked';
    this.span_id   = data.span_id   || 'untracked';
    this.service   = data.service   || 'unknown-service';
    this.env       = data.env       || (typeof process !== 'undefined' ? process.env.NODE_ENV || 'development' : 'browser');
    this.context   = data.context   || {};
  }

  private _genId(): string {
    if (typeof crypto !== 'undefined' && crypto.randomUUID) {
      return crypto.randomUUID();
    }
    return Math.random().toString(36).substring(2, 15) +
           Math.random().toString(36).substring(2, 15);
  }

  enrich(extra: LogContext): this {
    this.context = { ...this.context, ...extra };
    return this;
  }

  to_dict(): Record<string, unknown> {
    return {
      message:   this.message,
      level:     this.level,
      layer:     this.layer,
      timestamp: this.timestamp,
      record_id: this.record_id,
      trace_id:  this.trace_id,
      span_id:   this.span_id,
      service:   this.service,
      env:       this.env,
      context:   this.context,
    };
  }

  toString(): string {
    const colors: Record<LogLevel, string> = {
      [LogLevel.DEBUG]: '\x1b[36m',  // cyan
      [LogLevel.INFO]:  '\x1b[32m',  // green
      [LogLevel.WARN]:  '\x1b[33m',  // yellow
      [LogLevel.ERROR]: '\x1b[31m',  // red
      [LogLevel.FATAL]: '\x1b[35m',  // magenta
    };
    const reset = '\x1b[0m';
    const color = colors[this.level] || '\x1b[32m';
    return `${color}[${this.timestamp}] [${this.layer.toUpperCase()}] [${this.level}] ${this.message}${reset}`;
  }
}

/* ── Layer heuristics for auto-instrumentation ───────────── */

/**
 * Infers the best LogLayer for a class/function name automatically.
 * This is used by both browser and node agents for zero-config instrumentation.
 */
export function inferLayer(name: string): LogLayer {
  const n = name.toLowerCase();

  // Security
  if (/auth|jwt|token|oauth|permission|acl|rbac|guard|firewall|waf|encrypt|decrypt|password|credential|session|csrf|cors/.test(n))
    return LogLayer.SECURITY;

  // Data access
  if (/repo|repository|dao|database|db|query|migration|schema|cache|redis|mongo|postgres|sql|neo4j|orm|entity|store|persist|storage/.test(n))
    return LogLayer.DATA_ACCESS;

  // API gateway
  if (/controller|router|route|middleware|gateway|proxy|handler|endpoint|api|rest|graphql|grpc|webhook|interceptor/.test(n))
    return LogLayer.API_GATEWAY;

  // Domain / business
  if (/service|saga|aggregate|domain|policy|rule|event|command|workflow|process|saga|pricing|discount|fraud|risk|consent/.test(n))
    return LogLayer.DOMAIN;

  // Infrastructure
  if (/infra|worker|job|cron|queue|kafka|rabbit|bull|pubsub|container|health|monitor|metric|cpu|memory|disk/.test(n))
    return LogLayer.INFRASTRUCTURE;

  // Observability
  if (/trace|span|log|alert|metric|telemetry|observer|slo|sla|alarm/.test(n))
    return LogLayer.OBSERVABILITY;

  // Presentation
  if (/component|page|view|ui|render|form|modal|widget|screen|layout|theme/.test(n))
    return LogLayer.PRESENTATION;

  return LogLayer.BUSINESS_LOGIC;
}

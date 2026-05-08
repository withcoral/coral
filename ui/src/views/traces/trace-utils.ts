import { TraceStatus, type TraceSpan, type TraceSummary } from '@/generated/coral/v1/traces_pb'

export type JsonObject = Record<string, unknown>

export function nanosToMs(nanos: string | bigint | number): number {
  const value = typeof nanos === 'bigint' ? nanos : BigInt(nanos || 0)
  return Number(value / 1_000_000n)
}

export function startMs(trace: TraceSummary): number {
  return nanosToMs(trace.startTimeUnixNanos)
}

export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '—'
  if (ms < 1000) return `${Math.max(1, Math.round(ms))}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

export function formatDurationFromNanos(nanos: string): string {
  return formatDuration(nanosToMs(nanos))
}

export function formatTimestamp(timestamp: number): string {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return 'Unknown time'
  return new Date(timestamp).toLocaleString(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium',
  })
}

export function timeAgo(timestamp: number): string {
  const diff = Math.floor((Date.now() - timestamp) / 1000)
  if (diff < 5) return 'Just now'
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

export function formatRows(trace: TraceSummary): string {
  return trace.rowCountRecorded ? trace.rowCount.toString() : '—'
}

export function statusLabel(status: TraceStatus): string {
  if (status === TraceStatus.OK) return 'done'
  if (status === TraceStatus.ERROR) return 'error'
  return 'unknown'
}

export function statusTone(status: TraceStatus): 'ok' | 'error' | 'running' {
  if (status === TraceStatus.OK) return 'ok'
  if (status === TraceStatus.ERROR) return 'error'
  return 'running'
}

export function durationClass(nanos: string, warningClass: string, defaultClass: string): string {
  return nanosToMs(nanos) > 1000 ? warningClass : defaultClass
}

export function formatTraceError(message: string): string {
  const normalized = message.toLowerCase()
  if (normalized.includes('unimplemented') || normalized.includes('http 404')) {
    return 'Trace storage is not enabled for this Coral server. Enable [local_traces].enabled = true, restart the Coral server, then run a query.'
  }
  return message
}

export function parseJsonObject(json: string): JsonObject {
  if (!json) return {}
  try {
    const parsed = JSON.parse(json)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

function attrFrom(attrs: JsonObject, name: string): string | undefined {
  const value = attrs[name]
  if (value === undefined || value === null) return undefined
  return String(value)
}

export function attr(span: TraceSpan, name: string): string | undefined {
  return attrFrom(parseJsonObject(span.attributesJson), name)
}

export function spanSource(span: TraceSpan): string {
  const attrs = parseJsonObject(span.attributesJson)
  return attrFrom(attrs, 'coral.source') ?? attrFrom(attrs, 'db.system') ?? span.scopeName ?? 'coral'
}

function endpointPath(url: string): string {
  if (!url) return ''
  try {
    const parsed = new URL(url, 'http://coral.local')
    if (parsed.hostname !== 'coral.local' && parsed.pathname === '/') return parsed.hostname
    return parsed.pathname
  } catch {
    return url.replace(/^https?:\/\//, '')
  }
}

export function spanOperation(span: TraceSpan): string {
  const attrs = parseJsonObject(span.attributesJson)
  const method = attrFrom(attrs, 'http.request.method')
  const table = attrFrom(attrs, 'coral.table')
  if (method && table) return `${method} ${table}`
  if (method) return method
  return table ?? span.name
}

export function spanDisplayLabel(span: TraceSpan): string {
  const attrs = parseJsonObject(span.attributesJson)
  const method = attrFrom(attrs, 'http.request.method')
  const source = attrFrom(attrs, 'coral.source')
  const table = attrFrom(attrs, 'coral.table')
  const url = attrFrom(attrs, 'url.full') ?? attrFrom(attrs, 'http.url') ?? ''

  if (method && source && table) return `${method} ${source}.${table}`
  if (method && table) return `${method} ${table}`
  if (method && url) return `${method} ${endpointPath(url)}`
  if (source && table) return `${source}.${table}`
  if (table) return table
  if (span.name === 'coral.query') return 'Query'
  return span.name || span.scopeName || 'span'
}

export function spanDisplayMeta(span: TraceSpan, label = spanDisplayLabel(span)): string {
  const status = spanStatusCode(span)
  const parts = [
    span.kind,
    span.statusMessage,
    ['ok', 'done', 'unknown'].includes(status.toLowerCase()) ? undefined : status,
    span.scopeName && span.scopeName !== label ? span.scopeName : undefined,
  ].filter((part): part is string => typeof part === 'string' && part.length > 0)

  return [...new Set(parts)].join(' · ')
}

export function spanUrl(span: TraceSpan): string {
  const attrs = parseJsonObject(span.attributesJson)
  return attrFrom(attrs, 'url.full') ?? attrFrom(attrs, 'http.url') ?? ''
}

export function spanStatusCode(span: TraceSpan): string {
  return attrFrom(parseJsonObject(span.attributesJson), 'http.response.status_code') ?? statusLabel(span.status)
}

export function isHttpSpan(span: TraceSpan): boolean {
  const attrs = parseJsonObject(span.attributesJson)
  return span.name.startsWith('http.') || 'url.full' in attrs || 'http.request.method' in attrs
}

export function sortedSpans(spans: TraceSpan[]): TraceSpan[] {
  return [...spans].sort((a, b) => {
    const aStart = BigInt(a.startTimeUnixNanos || 0)
    const bStart = BigInt(b.startTimeUnixNanos || 0)
    if (aStart < bStart) return -1
    if (aStart > bStart) return 1
    return 0
  })
}

export function isQueryTrace(trace: TraceSummary): boolean {
  return trace.name === 'coral.query' || trace.query.trim().length > 0
}

export function sourceNames(spans: TraceSpan[]): string[] {
  const names = new Set<string>()
  for (const span of spans) {
    const source = attrFrom(parseJsonObject(span.attributesJson), 'coral.source')
    if (source) names.add(source)
  }
  return [...names]
}

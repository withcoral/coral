import {
  TraceStatus,
  type GetTraceResponse,
  type TraceSpan,
  type TraceSummary,
} from '@/generated/coral/v1/traces_pb'

export type TraceStatusView = 'error' | 'ok' | 'unknown'

export type TraceAttributes = Record<string, unknown>

export interface TraceSummaryView {
  durationNanos: string
  name: string
  query: string
  rootSpanId: string
  rowCount: string
  rowCountRecorded: boolean
  startTimeUnixNanos: string
  status: TraceStatusView
  traceId: string
}

export interface TraceSpanView {
  attributes: TraceAttributes
  durationNanos: string
  kind: string
  name: string
  parentSpanId: string
  scopeName: string
  spanId: string
  startTimeUnixNanos: string
  status: TraceStatusView
  statusMessage: string
}

export interface TraceDetailView {
  spans: TraceSpanView[]
  summary?: TraceSummaryView
}

function stringifyNanos(value: bigint | number | string | undefined): string {
  if (value === undefined || value === null || value === '') return '0'
  return value.toString()
}

export function mapTraceStatus(status: TraceStatus): TraceStatusView {
  if (status === TraceStatus.OK) return 'ok'
  if (status === TraceStatus.ERROR) return 'error'
  return 'unknown'
}

function parseTraceAttributes(attributesJson: string): TraceAttributes {
  if (!attributesJson) return {}
  try {
    const parsed = JSON.parse(attributesJson)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

export function mapTraceSummary(summary: TraceSummary): TraceSummaryView {
  return {
    durationNanos: stringifyNanos(summary.durationNanos),
    name: summary.name,
    query: summary.query,
    rootSpanId: summary.rootSpanId,
    rowCount: summary.rowCount.toString(),
    rowCountRecorded: summary.rowCountRecorded,
    startTimeUnixNanos: stringifyNanos(summary.startTimeUnixNanos),
    status: mapTraceStatus(summary.status),
    traceId: summary.traceId,
  }
}

export function mapTraceSpan(span: TraceSpan): TraceSpanView {
  return {
    attributes: parseTraceAttributes(span.attributesJson),
    durationNanos: stringifyNanos(span.durationNanos),
    kind: span.kind,
    name: span.name,
    parentSpanId: span.parentSpanId,
    scopeName: span.scopeName,
    spanId: span.spanId,
    startTimeUnixNanos: stringifyNanos(span.startTimeUnixNanos),
    status: mapTraceStatus(span.status),
    statusMessage: span.statusMessage,
  }
}

export function mapTraceDetail(response: GetTraceResponse): TraceDetailView {
  return {
    spans: response.spans.map(mapTraceSpan),
    summary: response.summary ? mapTraceSummary(response.summary) : undefined,
  }
}

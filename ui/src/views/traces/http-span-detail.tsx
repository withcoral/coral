import { useEffect, useState } from 'react'
import classNames from 'classnames'

import * as Button from '@/wax/components/button'
import { Typography } from '@/wax/components/typography'
import type { TraceSpan } from '@/generated/coral/v1/traces_pb'

import * as s from '../traces-page.css'
import { formatDuration, formatDurationFromNanos, parseJsonObject, spanOperation, spanUrl } from './trace-utils'

type JsonValue = Record<string, unknown> | unknown[] | string | number | boolean | null
type HttpDetailTab = 'params' | 'request' | 'response'
type CopyKind = 'formatted' | 'raw'

const REQUEST_BODY_ATTR = 'coral.http.request.body'
const RESPONSE_BODY_ATTR = 'coral.http.response.body'
const REQUEST_BODY_TRUNCATED_ATTR = 'coral.http.request.body.truncated'
const RESPONSE_BODY_TRUNCATED_ATTR = 'coral.http.response.body.truncated'
const REQUEST_BODY_PRESENT_ATTR = 'http.request.body.present'
const REQUEST_BODY_SIZE_ATTR = 'http.request.body.size'
const RESPONSE_BODY_SIZE_ATTR = 'http.response.body.size'
const BODY_ATTRIBUTE_KEYS = new Set([
  REQUEST_BODY_ATTR,
  RESPONSE_BODY_ATTR,
])

function looksLikeJson(value: string) {
  const trimmed = value.trim()
  return (trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))
}

function parseMaybeJson(value: unknown): JsonValue {
  if (typeof value !== 'string') return value as JsonValue
  try {
    const parsed = JSON.parse(value) as JsonValue
    if (typeof parsed === 'string' && looksLikeJson(parsed)) {
      return JSON.parse(parsed) as JsonValue
    }
    return parsed
  } catch {
    return value
  }
}

function requestParams(url: string): Record<string, string | string[]> {
  if (!url) return {}
  try {
    const params = new URL(url, 'http://coral.local').searchParams
    const result: Record<string, string | string[]> = {}
    for (const [key, value] of params.entries()) {
      const current = result[key]
      if (current === undefined) result[key] = value
      else if (Array.isArray(current)) current.push(value)
      else result[key] = [current, value]
    }
    return result
  } catch {
    return {}
  }
}

function formatDetailValue(value: JsonValue | undefined): string {
  if (value === undefined || value === null || value === '') return ''
  if (typeof value === 'string') {
    const parsedValue = parseMaybeJson(value)
    return typeof parsedValue === 'string' ? value : JSON.stringify(parsedValue, null, 2)
  }
  return JSON.stringify(value, null, 2)
}

function formatRawValue(value: unknown, formatted: string): string {
  if (value === undefined || value === null || value === '') return ''
  return typeof value === 'string' ? value : formatted
}

function DetailPre({ emptyText = 'Not recorded', value }: { emptyText?: string; value: JsonValue | undefined }) {
  if (value === undefined || value === null || value === '') {
    return <Typography.BodySmall variant="tertiary">{emptyText}</Typography.BodySmall>
  }
  return <pre className={s.detailsPre}>{formatDetailValue(value)}</pre>
}

function attrBool(value: unknown): boolean {
  return value === true || value === 'true'
}

function attrText(value: unknown): string | undefined {
  if (value === undefined || value === null || value === '') return undefined
  return String(value)
}

function formatBytes(value: unknown): string | undefined {
  const raw = attrText(value)
  if (!raw) return undefined
  const bytes = Number(raw)
  if (!Number.isFinite(bytes)) return raw
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function bodyEmptyText(kind: 'request' | 'response', attrs: Record<string, unknown>, truncated: boolean) {
  const label = kind === 'request' ? 'Request body' : 'Response body'
  const size = formatBytes(attrs[kind === 'request' ? REQUEST_BODY_SIZE_ATTR : RESPONSE_BODY_SIZE_ATTR])
  const present = kind === 'request' ? attrBool(attrs[REQUEST_BODY_PRESENT_ATTR]) : Boolean(size)

  if (truncated) return `${label} was truncated${size ? ` (${size})` : ''}, but no preview was recorded.`
  if (present) return `${label} was present${size ? ` (${size})` : ''}, but content was not captured.`
  return `No ${kind} body was recorded for this request.`
}

function metaChip(label: string, value: React.ReactNode) {
  return (
    <span className={s.httpMetaChip} key={label}>
      <Typography.BodySmall as="span" variant="tertiary">{label}</Typography.BodySmall>
      <Typography.BodySmallStrong as="span">{value}</Typography.BodySmallStrong>
    </span>
  )
}

export function HttpSpanDetail({ span, traceStart }: { span: TraceSpan; traceStart: bigint }) {
  const [activeTab, setActiveTab] = useState<HttpDetailTab>('response')
  const [copyState, setCopyState] = useState<CopyKind | 'failed' | 'idle'>('idle')
  const attrs = parseJsonObject(span.attributesJson)
  const url = spanUrl(span)
  const params = requestParams(url)
  const rawRequestBody = attrs[REQUEST_BODY_ATTR]
  const rawResponseBody = attrs[RESPONSE_BODY_ATTR]
  const requestBody = parseMaybeJson(rawRequestBody)
  const responseBody = parseMaybeJson(rawResponseBody)
  const requestBodyTruncated = attrBool(attrs[REQUEST_BODY_TRUNCATED_ATTR])
  const responseBodyTruncated = attrBool(attrs[RESPONSE_BODY_TRUNCATED_ATTR])
  const paramsValue = Object.keys(params).length ? params : undefined
  const preferredTab: HttpDetailTab = responseBody ? 'response' : requestBody ? 'request' : paramsValue ? 'params' : 'response'
  const tabs: Array<{ id: HttpDetailTab; label: string }> = [
    { id: 'params', label: 'Params' },
    { id: 'request', label: `Request body${requestBodyTruncated ? ' (truncated)' : ''}` },
    { id: 'response', label: `Response body${responseBodyTruncated ? ' (truncated)' : ''}` },
  ]
  const activeValue = activeTab === 'params'
    ? paramsValue
    : activeTab === 'request'
      ? requestBody
      : responseBody
  const activeRawValue = activeTab === 'params'
    ? paramsValue
    : activeTab === 'request'
      ? rawRequestBody
      : rawResponseBody
  const activeEmptyText = activeTab === 'params'
    ? 'No query parameters were recorded for this request.'
    : activeTab === 'request'
      ? bodyEmptyText('request', attrs, requestBodyTruncated)
      : bodyEmptyText('response', attrs, responseBodyTruncated)
  const copyValue = formatDetailValue(activeValue)
  const rawCopyValue = formatRawValue(activeRawValue, copyValue)
  const hasSeparateRawCopy = Boolean(rawCopyValue && rawCopyValue !== copyValue)
  const visibleAttrs = Object.fromEntries(Object.entries(attrs).filter(([key]) => !BODY_ATTRIBUTE_KEYS.has(key)))
  const offsetMs = Math.max(0, Number((BigInt(span.startTimeUnixNanos || 0) - traceStart) / 1_000_000n))
  const statusCode = attrText(attrs['http.response.status_code'])
  const requestId = attrText(attrs['coral.http.request_id'])
  const attempt = attrText(attrs['coral.http.attempt'])
  const source = attrText(attrs['coral.source'])
  const table = attrText(attrs['coral.table'])

  useEffect(() => setActiveTab(preferredTab), [preferredTab, span.spanId])
  useEffect(() => setCopyState('idle'), [activeTab, span.spanId])
  useEffect(() => {
    if (copyState === 'idle') return
    const timeout = window.setTimeout(() => setCopyState('idle'), 1800)
    return () => window.clearTimeout(timeout)
  }, [copyState])

  async function copyValueToClipboard(value: string, kind: CopyKind) {
    if (!value) return
    try {
      await navigator.clipboard.writeText(value)
      setCopyState(kind)
    } catch {
      setCopyState('failed')
    }
  }

  return (
    <div className={s.waterfallHttpDetail} onClick={(event) => event.stopPropagation()}>
      <div className={s.requestUrlRow}>
        <Typography.CodeSmallInline as="span" className={s.methodBadge}>{spanOperation(span)}</Typography.CodeSmallInline>
        <Typography.Body as="span" variant="tertiary" className={s.requestUrl}>{url || 'No URL recorded'}</Typography.Body>
      </div>
      <div className={s.httpMetaRow}>
        {statusCode && metaChip('Status', statusCode)}
        {metaChip('Duration', formatDurationFromNanos(span.durationNanos))}
        {metaChip('Start', `+${formatDuration(offsetMs)}`)}
        {requestId && metaChip('Request', `#${requestId}`)}
        {attempt && metaChip('Attempt', attempt)}
        {source && metaChip('Source', table ? `${source}.${table}` : source)}
      </div>
      <div className={s.waterfallHttpTabRow}>
        <div className={s.tabList} role="tablist" aria-label="HTTP span details">
          {tabs.map((tab) => (
            <button
              aria-controls={`http-detail-${span.spanId}-${tab.id}`}
              aria-selected={activeTab === tab.id}
              className={classNames(s.tabTrigger, { [s.tabTriggerActive]: activeTab === tab.id })}
              id={`http-detail-tab-${span.spanId}-${tab.id}`}
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              role="tab"
              type="button"
            >
              <Typography.BodySmallStrong as="span">{tab.label}</Typography.BodySmallStrong>
            </button>
          ))}
        </div>
        <div className={s.copyButtonGroup}>
          {hasSeparateRawCopy && (
            <Button.TextButton disabled={!rawCopyValue} onClick={() => copyValueToClipboard(rawCopyValue, 'raw')} size="22" variant="secondary">
              {copyState === 'raw' ? 'Raw copied' : 'Copy raw'}
            </Button.TextButton>
          )}
          <Button.TextButton disabled={!copyValue} onClick={() => copyValueToClipboard(copyValue, 'formatted')} size="22" variant="secondary">
            {copyState === 'formatted' ? 'Copied' : copyState === 'failed' ? 'Copy failed' : 'Copy formatted'}
          </Button.TextButton>
        </div>
      </div>
      <section
        aria-labelledby={`http-detail-tab-${span.spanId}-${activeTab}`}
        className={s.waterfallHttpDetailSection}
        id={`http-detail-${span.spanId}-${activeTab}`}
        role="tabpanel"
      >
        <DetailPre emptyText={activeEmptyText} value={activeValue} />
      </section>
      <details>
        <summary className={s.detailsSummary}><Typography.Body as="span" variant="tertiary">Span attributes</Typography.Body></summary>
        <pre className={s.detailsPre}>{JSON.stringify(visibleAttrs, null, 2)}</pre>
      </details>
    </div>
  )
}

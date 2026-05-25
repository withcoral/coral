import { useEffect, useState } from 'react'
import classNames from 'classnames'

import * as Button from '@/wax/components/button'
import * as ScrollArea from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'
import type { TraceSpan } from '@/generated/coral/v1/traces_pb'

import * as s from '../traces-page.css'
import {
  formatDuration,
  formatDurationFromNanos,
  parseJsonObject,
  spanOperation,
  spanUrl,
} from './trace-utils'

type JsonValue = Record<string, unknown> | unknown[] | string | number | boolean | null
type HttpDetailTab = 'params' | 'request' | 'response'
type CopyKind = 'formatted' | 'raw'

const REQUEST_BODY_ATTR = 'coral.http.request.body'
const RESPONSE_BODY_ATTR = 'coral.http.response.body'
const REQUEST_BODY_TRUNCATED_ATTR = 'coral.http.request.body.truncated'
const RESPONSE_BODY_TRUNCATED_ATTR = 'coral.http.response.body.truncated'
const REQUEST_BODY_PRESENT_ATTR = 'http.request.body.present'
const RESPONSE_BODY_PRESENT_ATTR = 'http.response.body.present'
const REQUEST_BODY_SIZE_ATTR = 'http.request.body.size'
const RESPONSE_BODY_SIZE_ATTR = 'http.response.body.size'
const BODY_ATTRIBUTE_KEYS = new Set([REQUEST_BODY_ATTR, RESPONSE_BODY_ATTR])

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function looksLikeJson(value: string) {
  const trimmed = value.trim()
  return (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  )
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

function formatJsonValue(value: JsonValue): string {
  return JSON.stringify(value, null, 2)
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
    return typeof parsedValue === 'string' ? value : formatJsonValue(parsedValue)
  }
  return formatJsonValue(value)
}

function formatRawValue(value: unknown, formatted: string): string {
  if (value === undefined || value === null || value === '') return ''
  return typeof value === 'string' ? value : formatted
}

type BodyKind = 'request' | 'response'

interface GraphqlBodyPreview {
  bodyKind: BodyKind
  operationName?: string
  operationType?: string
  query?: string
  variables?: JsonValue
  data?: JsonValue
  errors?: JsonValue
}

interface BodyPreview {
  formattedText: string
  graphql?: GraphqlBodyPreview
  rawText: string
}

function inferGraphqlOperationType(query: string | undefined): string | undefined {
  if (!query) return undefined
  const match = query.trim().match(/^(query|mutation|subscription)\b/i)
  return match?.[1]?.toLowerCase() ?? 'query'
}

function detectGraphqlBody(bodyKind: BodyKind, value: JsonValue): GraphqlBodyPreview | undefined {
  if (!isPlainObject(value)) return undefined

  const query = typeof value.query === 'string' ? value.query : undefined
  const operationName = typeof value.operationName === 'string' ? value.operationName : undefined
  const variables = value.variables as JsonValue | undefined
  const data = value.data as JsonValue | undefined
  const errors = value.errors as JsonValue | undefined
  const hasRequestShape = Boolean(query || operationName || variables !== undefined)
  const hasResponseShape = Boolean(data !== undefined || errors !== undefined)

  if (bodyKind === 'request' && !hasRequestShape) return undefined
  if (bodyKind === 'response' && !hasResponseShape) return undefined

  return {
    bodyKind,
    operationName,
    operationType: inferGraphqlOperationType(query),
    query,
    variables,
    data,
    errors,
  }
}

function bodyPreview(kind: BodyKind, value: unknown, rawValue: unknown): BodyPreview | undefined {
  if (value === undefined || value === null || value === '') return undefined

  if (typeof value === 'string') {
    const parsedValue = parseMaybeJson(value)
    if (typeof parsedValue === 'string') {
      return {
        formattedText: value,
        rawText: typeof rawValue === 'string' ? rawValue : value,
      }
    }
    const formattedText = formatJsonValue(parsedValue)
    return {
      formattedText,
      graphql: detectGraphqlBody(kind, parsedValue),
      rawText: typeof rawValue === 'string' ? rawValue : formattedText,
    }
  }

  const formattedText = formatJsonValue(value as JsonValue)
  return {
    formattedText,
    graphql: detectGraphqlBody(kind, value as JsonValue),
    rawText: typeof rawValue === 'string' ? rawValue : formattedText,
  }
}

function BodySection({ children, label }: { children: React.ReactNode; label: string }) {
  return (
    <section className={s.bodyViewerSection}>
      <Typography.BodySmallStrong as="span" className={s.bodyViewerSectionLabel}>
        {label}
      </Typography.BodySmallStrong>
      {children}
    </section>
  )
}

function BodyViewer({
  emptyText,
  kind,
  rawValue,
  value,
}: {
  emptyText: string
  kind: BodyKind
  rawValue: unknown
  value: unknown
}) {
  const preview = bodyPreview(kind, value, rawValue)

  if (!preview) {
    return <Typography.BodySmall variant="tertiary">{emptyText}</Typography.BodySmall>
  }

  const rawBodyDetails =
    preview.rawText !== preview.formattedText ? (
      <details className={s.bodyViewerRawDetails}>
        <summary className={s.detailsSummary}>
          <Typography.Body as="span" variant="tertiary">
            Raw body
          </Typography.Body>
        </summary>
        <pre className={s.detailsPre}>{preview.rawText}</pre>
      </details>
    ) : null

  if (!preview.graphql) {
    return (
      <div className={s.bodyViewer}>
        <pre className={s.detailsPre}>{preview.formattedText}</pre>
        {rawBodyDetails}
      </div>
    )
  }

  const { bodyKind, data, errors, operationName, operationType, query, variables } = preview.graphql

  return (
    <div className={s.bodyViewer}>
      <div className={s.bodyViewerHeader}>
        <Typography.BodySmallStrong as="span">
          {bodyKind === 'request' ? 'GraphQL request' : 'GraphQL response'}
        </Typography.BodySmallStrong>
        <div className={s.bodyMetaRow}>
          {operationName && metaChip('Operation', operationName)}
          {operationType && metaChip('Type', operationType)}
          {variables !== undefined &&
            metaChip('Variables', Array.isArray(variables) ? `${variables.length}` : 'present')}
          {Array.isArray(errors)
            ? metaChip('Errors', `${errors.length}`)
            : errors !== undefined
              ? metaChip('Errors', 'present')
              : null}
          {data !== undefined &&
            metaChip('Data', Array.isArray(data) ? `${data.length}` : 'present')}
        </div>
      </div>
      {query !== undefined && (
        <BodySection label="Query">
          <pre className={s.detailsPre}>{query}</pre>
        </BodySection>
      )}
      {variables !== undefined && (
        <BodySection label="Variables">
          <pre className={s.detailsPre}>{formatDetailValue(variables)}</pre>
        </BodySection>
      )}
      {data !== undefined && (
        <BodySection label="Data">
          <pre className={s.detailsPre}>{formatDetailValue(data)}</pre>
        </BodySection>
      )}
      {errors !== undefined && (
        <BodySection label="Errors">
          <pre className={s.detailsPre}>{formatDetailValue(errors)}</pre>
        </BodySection>
      )}
      {rawBodyDetails}
    </div>
  )
}

interface ActiveBodyState {
  emptyText: string
  kind: BodyKind
  rawValue: unknown
  value: JsonValue | undefined
}

function activeBodyState(
  activeTab: HttpDetailTab,
  attrs: Record<string, unknown>,
  paramsValue: Record<string, string | string[]> | undefined,
  requestBody: JsonValue,
  rawRequestBody: unknown,
  requestBodyTruncated: boolean,
  responseBody: JsonValue,
  rawResponseBody: unknown,
  responseBodyTruncated: boolean,
): ActiveBodyState {
  switch (activeTab) {
    case 'params':
      return {
        emptyText: 'No query parameters were recorded for this request.',
        kind: 'response',
        rawValue: paramsValue,
        value: paramsValue,
      }
    case 'request':
      return {
        emptyText: bodyEmptyText('request', attrs, requestBodyTruncated),
        kind: 'request',
        rawValue: rawRequestBody,
        value: requestBody,
      }
    case 'response':
      return {
        emptyText: bodyEmptyText('response', attrs, responseBodyTruncated),
        kind: 'response',
        rawValue: rawResponseBody,
        value: responseBody,
      }
  }
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

function bodyEmptyText(
  kind: 'request' | 'response',
  attrs: Record<string, unknown>,
  truncated: boolean,
) {
  const label = kind === 'request' ? 'Request body' : 'Response body'
  const size = formatBytes(
    attrs[kind === 'request' ? REQUEST_BODY_SIZE_ATTR : RESPONSE_BODY_SIZE_ATTR],
  )
  const present =
    kind === 'request'
      ? attrBool(attrs[REQUEST_BODY_PRESENT_ATTR])
      : attrBool(attrs[RESPONSE_BODY_PRESENT_ATTR]) || Boolean(size)

  if (truncated)
    return `${label} was truncated${size ? ` (${size})` : ''}, but no preview was recorded.`
  if (present)
    return `${label} was present${size ? ` (${size})` : ''}, but content was not captured.`
  return `No ${kind} body was recorded for this request.`
}

function metaChip(label: string, value: React.ReactNode) {
  return (
    <span className={s.httpMetaChip} key={label}>
      <Typography.BodySmall as="span" variant="tertiary">
        {label}
      </Typography.BodySmall>
      <Typography.BodySmallStrong as="span">{value}</Typography.BodySmallStrong>
    </span>
  )
}

export function HttpSpanDetail({
  canSelectNextSpan,
  canSelectPreviousSpan,
  onClose,
  onSelectNextSpan,
  onSelectPreviousSpan,
  span,
  traceStart,
}: {
  canSelectNextSpan: boolean
  canSelectPreviousSpan: boolean
  onClose: () => void
  onSelectNextSpan: () => void
  onSelectPreviousSpan: () => void
  span: TraceSpan
  traceStart: bigint
}) {
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
  const preferredTab: HttpDetailTab = responseBody
    ? 'response'
    : requestBody
      ? 'request'
      : paramsValue
        ? 'params'
        : 'response'
  const tabs: Array<{ id: HttpDetailTab; label: string }> = [
    { id: 'params', label: 'Params' },
    { id: 'request', label: `Request body${requestBodyTruncated ? ' (truncated)' : ''}` },
    { id: 'response', label: `Response body${responseBodyTruncated ? ' (truncated)' : ''}` },
  ]
  const activeBody = activeBodyState(
    activeTab,
    attrs,
    paramsValue,
    requestBody,
    rawRequestBody,
    requestBodyTruncated,
    responseBody,
    rawResponseBody,
    responseBodyTruncated,
  )
  const copyValue = formatDetailValue(activeBody.value)
  const rawCopyValue = formatRawValue(activeBody.rawValue, copyValue)
  const hasSeparateRawCopy = Boolean(rawCopyValue && rawCopyValue !== copyValue)
  const visibleAttrs = Object.fromEntries(
    Object.entries(attrs).filter(([key]) => !BODY_ATTRIBUTE_KEYS.has(key)),
  )
  const offsetMs = Math.max(
    0,
    Number((BigInt(span.startTimeUnixNanos || 0) - traceStart) / 1_000_000n),
  )
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
    <div
      className={s.waterfallHttpDetail}
      data-span-inspector="true"
      onClick={(event) => event.stopPropagation()}
    >
      <div className={s.waterfallHttpDetailHeader}>
        <div className={s.waterfallHttpDetailTitle}>
          <Typography.BodySmallStrong as="span">Span details</Typography.BodySmallStrong>
          <Typography.BodySmall as="span" variant="tertiary" truncate>
            {spanOperation(span)}
          </Typography.BodySmall>
        </div>
        <div className={s.waterfallHttpDetailHeaderActions}>
          <Button.IconButton
            disabled={!canSelectPreviousSpan}
            name="ArrowUp"
            onClick={onSelectPreviousSpan}
            size="32"
            tooltipText="Previous span"
            variant="bare"
          />
          <Button.IconButton
            disabled={!canSelectNextSpan}
            name="ArrowDown"
            onClick={onSelectNextSpan}
            size="32"
            tooltipText="Next span"
            variant="bare"
          />
          <Button.IconButton
            name="X"
            onClick={onClose}
            size="32"
            tooltipText="Close span details"
            variant="bare"
          />
        </div>
      </div>
      <ScrollArea.Container
        className={s.waterfallHttpDetailScroll}
        constrainWidth
        fade="bottom"
        height="100%"
      >
        <div className={s.waterfallHttpDetailContent}>
          <div className={s.requestUrlRow}>
            <Typography.CodeSmallInline as="span" className={s.methodBadge}>
              {spanOperation(span)}
            </Typography.CodeSmallInline>
            <Typography.Body as="span" variant="tertiary" className={s.requestUrl}>
              {url || 'No URL recorded'}
            </Typography.Body>
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
                  className={classNames(s.tabTrigger, {
                    [s.tabTriggerActive]: activeTab === tab.id,
                  })}
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
                <Button.TextButton
                  disabled={!rawCopyValue}
                  onClick={() => copyValueToClipboard(rawCopyValue, 'raw')}
                  size="22"
                  variant="secondary"
                >
                  {copyState === 'raw' ? 'Raw copied' : 'Copy raw'}
                </Button.TextButton>
              )}
              <Button.TextButton
                disabled={!copyValue}
                onClick={() => copyValueToClipboard(copyValue, 'formatted')}
                size="22"
                variant="secondary"
              >
                {copyState === 'formatted'
                  ? 'Copied'
                  : copyState === 'failed'
                    ? 'Copy failed'
                    : 'Copy formatted'}
              </Button.TextButton>
            </div>
          </div>
          <section
            aria-labelledby={`http-detail-tab-${span.spanId}-${activeTab}`}
            className={s.waterfallHttpDetailSection}
            id={`http-detail-${span.spanId}-${activeTab}`}
            role="tabpanel"
          >
            <BodyViewer
              emptyText={activeBody.emptyText}
              kind={activeBody.kind}
              rawValue={activeBody.rawValue}
              value={activeBody.value}
            />
          </section>
          <details>
            <summary className={s.detailsSummary}>
              <Typography.Body as="span" variant="tertiary">
                Span attributes
              </Typography.Body>
            </summary>
            <pre className={s.detailsPre}>{JSON.stringify(visibleAttrs, null, 2)}</pre>
          </details>
        </div>
      </ScrollArea.Container>
    </div>
  )
}

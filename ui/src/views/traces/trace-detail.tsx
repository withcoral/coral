import { useCallback, useEffect, useMemo, useState } from 'react'
import classNames from 'classnames'

import * as Button from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { Typography } from '@/wax/components/typography'
import { getTrace } from '@/lib/coral-traces-client'
import { TraceStatus, type GetTraceResponse, type TraceSpan } from '@/generated/coral/v1/traces_pb'

import * as s from '../traces-page.css'
import { EmptyState } from './empty-state'
import { HttpSpanDetail } from './http-span-detail'
import { PageHeader } from './page-header'
import { SqlCode } from './sql-code'
import { useTimelineTree, type TimelineRow } from './use-timeline-tree'
import {
  formatDuration,
  formatDurationFromNanos,
  formatRows,
  formatTraceError,
  isHttpSpan,
  nanosToMs,
  spanDisplayLabel,
  spanDisplayMeta,
  sourceNames,
  statusLabel,
  statusTone,
} from './trace-utils'

export type DetailTab = 'timeline' | 'api'
type WaterfallTone = 'query' | 'http' | 'span' | 'error'

const INDENT_PX = 14
const NOISY_INTERNAL_SPAN_MAX_MS = 1

export interface ExtraDetailTab {
  id: string
  label: string
  content: React.ReactNode
  show?: boolean
}

function useTraceDetail(traceId: string | null) {
  const [detail, setDetail] = useState<GetTraceResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!traceId) {
      setDetail(null)
      setError(null)
      return
    }
    let stale = false
    setDetail(null)
    setLoading(true)
    setError(null)
    getTrace(traceId)
      .then((response) => { if (!stale) setDetail(response) })
      .catch((err) => { if (!stale) setError(formatTraceError(err instanceof Error ? err.message : String(err))) })
      .finally(() => { if (!stale) setLoading(false) })
    return () => { stale = true }
  }, [traceId])

  return { detail, error, loading }
}

function useProMode() {
  const [proMode, setProMode] = useState(false)

  useEffect(() => {
    const readProMode = () => setProMode(new URLSearchParams(window.location.search).has('pro'))
    readProMode()
    window.addEventListener('popstate', readProMode)
    window.addEventListener('hashchange', readProMode)
    return () => {
      window.removeEventListener('popstate', readProMode)
      window.removeEventListener('hashchange', readProMode)
    }
  }, [])

  return proMode
}

function StatCard({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className={s.statCard}>
      <Typography.Body variant="tertiary">{label}</Typography.Body>
      <Typography.BodyLargeStrong>{value}</Typography.BodyLargeStrong>
    </div>
  )
}

function WaterfallBar({ left, tone, width, label }: { left: number; tone: WaterfallTone; width: number; label: string }) {
  const clampedLeft = Math.max(0, Math.min(100, left))
  const clampedWidth = Math.max(0.5, Math.min(100 - clampedLeft, width))
  const narrow = clampedWidth < 6
  const outsideLabelLeft = clampedLeft + clampedWidth + 0.5
  const outsideLabelStyle = outsideLabelLeft > 86
    ? { right: `${Math.max(0, 100 - clampedLeft)}%` }
    : { left: `${outsideLabelLeft}%` }

  return (
    <div className={s.waterfallBarArea}>
      <div className={s.waterfallBar} data-tone={tone} style={{ left: `${clampedLeft}%`, width: `${clampedWidth}%` }}>
        {!narrow && <span className={s.waterfallBarLabel}>{label}</span>}
      </div>
      {narrow && <span className={s.waterfallBarLabelOutside} data-align={outsideLabelLeft > 86 ? 'end' : 'start'} style={outsideLabelStyle}>{label}</span>}
    </div>
  )
}

function WaterfallTickRow({ durationMs }: { durationMs: number }) {
  const ticks = useMemo(() => Array.from({ length: 5 }, (_, index) => (index / 4) * durationMs), [durationMs])

  return (
    <div className={s.waterfallTickRow} role="presentation">
      <div className={s.waterfallLabel} />
      <div className={s.waterfallTimeline}>
        {ticks.map((tick, index) => {
          const pct = (tick / durationMs) * 100
          const style = index === 0 ? { left: 0 } : index === ticks.length - 1 ? { right: 0 } : { left: `${pct}%`, transform: 'translateX(-50%)' }
          return <span className={s.waterfallTick} key={`${tick}-${index}`} style={style}>{formatDuration(tick)}</span>
        })}
      </div>
    </div>
  )
}

function spanTone(span: TraceSpan): WaterfallTone {
  if (span.status === TraceStatus.ERROR) return 'error'
  if (isHttpSpan(span)) return 'http'
  if (span.name === 'coral.query') return 'query'
  return 'span'
}

function WaterfallSpanLabel({
  active,
  childCount,
  collapsed,
  depth,
  label,
  meta,
  onToggle,
  spanId,
  tone,
}: {
  active: boolean
  childCount: number
  collapsed: boolean
  depth: number
  label: string
  meta: string
  onToggle: (spanId: string) => void
  spanId: string
  tone: WaterfallTone
}) {
  return (
    <div className={classNames(s.waterfallSpanLabel, { [s.waterfallSpanLabelActive]: active })} style={{ paddingInlineStart: depth * INDENT_PX }}>
      {depth > 0 && <span className={s.waterfallTreeGuide} aria-hidden />}
      {childCount > 0 ? (
        <button
          aria-label={`${collapsed ? 'Expand' : 'Collapse'} ${label}, ${childCount} child${childCount === 1 ? '' : 'ren'}`}
          aria-expanded={!collapsed}
          className={s.waterfallTreeToggle}
          onClick={(event) => {
            event.stopPropagation()
            onToggle(spanId)
          }}
          type="button"
        >
          <Icon name={collapsed ? 'ChevronRight' : 'ChevronDown'} size="14" color="secondary" />
          <span className={s.waterfallChildCountChip}>{childCount}</span>
        </button>
      ) : (
        <span className={s.waterfallTreeTogglePlaceholder} />
      )}
      <span className={s.waterfallPluginPill}>
        <span className={s.waterfallPluginDot} data-tone={tone} />
        <span className={s.waterfallLabelText}>
          <Typography.BodySmallStrong as="span" truncate>{label}</Typography.BodySmallStrong>
          {meta && <Typography.BodySmall as="span" variant="tertiary" truncate>{meta}</Typography.BodySmall>}
        </span>
      </span>
    </div>
  )
}

function WaterfallBarSlot({
  active = false,
  durationMs,
  hovered,
  onToggleExpanded,
  onHover,
  row,
  traceStart,
}: {
  active?: boolean
  durationMs: number
  hovered: boolean
  onToggleExpanded: (spanId: string) => void
  onHover: (spanId: string | null) => void
  row: TimelineRow
  traceStart: bigint
}) {
  const { span } = row
  const offsetMs = Number((BigInt(span.startTimeUnixNanos || 0) - traceStart) / 1_000_000n)
  const left = (Math.max(0, offsetMs) / durationMs) * 100
  const width = (nanosToMs(span.durationNanos) / durationMs) * 100
  const tone = spanTone(span)
  const canExpandHttp = isHttpSpan(span)

  return (
    <div
      className={classNames(s.waterfallBarSlot, { [s.waterfallRowHover]: hovered, [s.waterfallBarSlotActive]: active })}
      onMouseEnter={() => onHover(span.spanId)}
      onMouseLeave={() => onHover(null)}
      onClick={() => canExpandHttp && onToggleExpanded(span.spanId)}
      onKeyDown={(event) => {
        if (!canExpandHttp || (event.key !== 'Enter' && event.key !== ' ')) return
        event.preventDefault()
        onToggleExpanded(span.spanId)
      }}
      role={canExpandHttp ? 'button' : undefined}
      tabIndex={canExpandHttp ? 0 : undefined}
    >
      <WaterfallBar left={left} tone={tone} width={width} label={formatDurationFromNanos(span.durationNanos)} />
    </div>
  )
}

function WaterfallRow({
  collapsed,
  expanded,
  hovered,
  onToggle,
  onToggleExpanded,
  onHover,
  row,
}: {
  collapsed: boolean
  expanded: boolean
  hovered: boolean
  onToggle: (spanId: string) => void
  onToggleExpanded: (spanId: string) => void
  onHover: (spanId: string | null) => void
  row: TimelineRow
}) {
  const { childCount, depth, span } = row
  const tone = spanTone(span)
  const label = spanDisplayLabel(span)
  const meta = spanDisplayMeta(span, label)
  const isNoisyInternalSpan = tone === 'span' && span.kind === 'internal' && nanosToMs(span.durationNanos) <= NOISY_INTERNAL_SPAN_MAX_MS
  const canExpandHttp = isHttpSpan(span)

  return (
    <div
      aria-expanded={childCount > 0 ? !collapsed : undefined}
      aria-level={depth + 1}
      className={s.waterfallRowShell}
      role="treeitem"
    >
      <div
        aria-expanded={canExpandHttp ? expanded : undefined}
        className={classNames(s.waterfallRowButton, { [s.waterfallRowHover]: hovered })}
        data-noisy={isNoisyInternalSpan || undefined}
        onMouseEnter={() => onHover(span.spanId)}
        onMouseLeave={() => onHover(null)}
        onClick={() => canExpandHttp && onToggleExpanded(span.spanId)}
        onKeyDown={(event) => {
          if (!canExpandHttp || (event.key !== 'Enter' && event.key !== ' ')) return
          event.preventDefault()
          onToggleExpanded(span.spanId)
        }}
        role={canExpandHttp ? 'button' : undefined}
        tabIndex={canExpandHttp ? 0 : undefined}
      >
        <WaterfallSpanLabel
          childCount={childCount}
          active={expanded}
          collapsed={collapsed}
          depth={depth}
          label={label}
          meta={meta}
          onToggle={onToggle}
          spanId={span.spanId}
          tone={tone}
        />
      </div>
    </div>
  )
}

function TimelineWaterfall({ expandedHttpSpanId, onExpandedHttpSpanIdChange, onNavigableSpanIdsChange, spans, summary }: {
  expandedHttpSpanId: string | null
  onExpandedHttpSpanIdChange: (spanId: string | null | ((current: string | null) => string | null)) => void
  onNavigableSpanIdsChange: (spanIds: string[]) => void
  spans: TraceSpan[]
  summary?: GetTraceResponse['summary']
}) {
  const proMode = useProMode()
  const timelineSpans = useMemo(() => proMode ? spans : spans.filter(isHttpSpan), [proMode, spans])
  const { collapsedSpanIds, rows, toggleSpan } = useTimelineTree(timelineSpans, summary?.rootSpanId, summary?.traceId)
  const [hoveredSpanId, setHoveredSpanId] = useState<string | null>(null)
  useEffect(() => onExpandedHttpSpanIdChange(null), [onExpandedHttpSpanIdChange, summary?.traceId])
  useEffect(() => setHoveredSpanId(null), [summary?.traceId])
  useEffect(() => {
    onNavigableSpanIdsChange(rows.filter((row) => isHttpSpan(row.span)).map((row) => row.span.spanId))
  }, [onNavigableSpanIdsChange, rows])
  if (rows.length === 0) {
    return (
      <EmptyState
        title="No spans for this trace"
        details="This trace did not record any spans that match the current view."
      />
    )
  }
  const traceStart = BigInt(summary?.startTimeUnixNanos || rows[0]?.span.startTimeUnixNanos || 0)
  const durationMs = Math.max(nanosToMs(summary?.durationNanos || '0'), 1)
  const expandedHttpRow = rows.find((row) => row.span.spanId === expandedHttpSpanId)
  const expandedHttpSpan = expandedHttpRow?.span

  return (
    <div className={s.waterfallRoot} role="tree">
      <WaterfallTickRow durationMs={durationMs} />
      <div className={s.waterfallLabelsColumn}>
        {rows.map((row) => (
          <WaterfallRow
            collapsed={collapsedSpanIds.has(row.span.spanId)}
            expanded={expandedHttpSpanId === row.span.spanId}
            hovered={hoveredSpanId === row.span.spanId}
            key={row.span.spanId}
            onHover={setHoveredSpanId}
            onToggle={toggleSpan}
            onToggleExpanded={(spanId) => onExpandedHttpSpanIdChange((current) => current === spanId ? null : spanId)}
            row={row}
          />
        ))}
      </div>
      <div className={s.waterfallTimelineBody}>
        {expandedHttpRow && expandedHttpSpan ? (
          <>
            <WaterfallBarSlot
              active
              durationMs={durationMs}
              hovered
              key={`${expandedHttpSpan.spanId}-selected-bar`}
              onHover={setHoveredSpanId}
              onToggleExpanded={(spanId) => onExpandedHttpSpanIdChange((current) => current === spanId ? null : spanId)}
              row={expandedHttpRow}
              traceStart={traceStart}
            />
            <HttpSpanDetail span={expandedHttpSpan} traceStart={traceStart} />
          </>
        ) : (
          rows.map((row) => (
            <WaterfallBarSlot
              durationMs={durationMs}
              hovered={hoveredSpanId === row.span.spanId}
              key={row.span.spanId}
              onHover={setHoveredSpanId}
              onToggleExpanded={(spanId) => onExpandedHttpSpanIdChange((current) => current === spanId ? null : spanId)}
              row={row}
              traceStart={traceStart}
            />
          ))
        )}
      </div>
    </div>
  )
}

function DetailTabs({ activeTab, extraTabs, onTab }: { activeTab: string; extraTabs?: ExtraDetailTab[]; onTab: (tab: string) => void }) {
  const tabs = [
    { id: 'timeline', label: 'Trace', show: true },
    ...(extraTabs ?? []).map((tab) => ({ id: tab.id, label: tab.label, show: tab.show ?? true })),
  ]
  return (
    <div className={s.tabList}>
      {tabs.filter((tab) => tab.show).map((tab) => (
        <button className={classNames(s.tabTrigger, { [s.tabTriggerActive]: activeTab === tab.id })} key={tab.id} onClick={() => onTab(tab.id)} type="button">
          <Typography.BodySmallStrong as="span">{tab.label}</Typography.BodySmallStrong>
        </button>
      ))}
    </div>
  )
}

export function TraceDetail({
  extraTabs,
  newerTraceId,
  olderTraceId,
  onClose,
  onSelectTrace,
  traceId,
}: {
  extraTabs?: (detail: GetTraceResponse) => ExtraDetailTab[]
  newerTraceId?: string | null
  olderTraceId?: string | null
  onClose: () => void
  onSelectTrace?: (traceId: string) => void
  traceId: string
}) {
  const { detail, error, loading } = useTraceDetail(traceId)
  const [activeTab, setActiveTab] = useState<string>('timeline')
  const [expandedHttpSpanId, setExpandedHttpSpanId] = useState<string | null>(null)
  const [navigableSpanIds, setNavigableSpanIds] = useState<string[]>([])
  useEffect(() => setActiveTab('timeline'), [traceId])

  const selectAdjacentSpan = useCallback((direction: -1 | 1) => {
    if (!expandedHttpSpanId) return
    const currentIndex = navigableSpanIds.indexOf(expandedHttpSpanId)
    const nextSpanId = currentIndex >= 0 ? navigableSpanIds[currentIndex + direction] : null
    if (nextSpanId) setExpandedHttpSpanId(nextSpanId)
  }, [expandedHttpSpanId, navigableSpanIds])

  const handleEscapeShortcut = useCallback((event: KeyboardEvent) => {
    if (expandedHttpSpanId) {
      event.preventDefault()
      setExpandedHttpSpanId(null)
      return
    }
    onClose()
  }, [expandedHttpSpanId, onClose])

  const handlePreviousSpanShortcut = useCallback((event: KeyboardEvent) => {
    if (!expandedHttpSpanId) return
    event.preventDefault()
    selectAdjacentSpan(-1)
  }, [expandedHttpSpanId, selectAdjacentSpan])

  const handleNextSpanShortcut = useCallback((event: KeyboardEvent) => {
    if (!expandedHttpSpanId) return
    event.preventDefault()
    selectAdjacentSpan(1)
  }, [expandedHttpSpanId, selectAdjacentSpan])
  const summary = detail?.summary
  const httpSpans = useMemo(() => detail?.spans.filter(isHttpSpan) ?? [], [detail?.spans])
  const sources = useMemo(() => sourceNames(detail?.spans ?? []), [detail?.spans])
  const resolvedExtraTabs = useMemo(() => (detail ? extraTabs?.(detail) ?? [] : []), [detail, extraTabs])

  if (loading && !detail) return <div className={s.detailEmpty}><Icon name="Loader" className={s.spinner} color="tertiary" /><Typography.Body>Loading trace…</Typography.Body></div>
  if (error) return <div className={s.detailEmpty}><EmptyState error={error} /></div>
  if (!detail || !summary) {
    return (
      <div className={s.detailEmpty}>
        <EmptyState
          title="No spans for this trace"
          details="This trace did not include a query summary or spans to display."
        />
      </div>
    )
  }

  const activeExtraTab = resolvedExtraTabs.find((tab) => tab.id === activeTab)

  return (
    <div className={s.detailRoot}>
      <KeyboardShortcut handler={handlePreviousSpanShortcut} shortcut="ArrowUp" />
      <KeyboardShortcut handler={handleNextSpanShortcut} shortcut="ArrowDown" />
      <PageHeader title={<><Button.TextButton onClick={onClose} size="22" variant="linkSubtle"><Typography.BodyStrong as="span" variant="tertiary">Query stream</Typography.BodyStrong></Button.TextButton><Typography.BodyStrong as="span" variant="tertiary">/</Typography.BodyStrong><Typography.BodyStrong as="span" variant="secondary">Query details</Typography.BodyStrong></>}>
        <div className={s.detailHeaderActions}>
          <span className={s.statusBadge} data-tone={statusTone(summary.status)}>{statusLabel(summary.status)}</span>
          <Button.IconButton disabled={!newerTraceId} name="ArrowUp" onClick={() => newerTraceId && onSelectTrace?.(newerTraceId)} size="32" tooltipText="Newer query" variant="bare" />
          <Button.IconButton disabled={!olderTraceId} name="ArrowDown" onClick={() => olderTraceId && onSelectTrace?.(olderTraceId)} size="32" tooltipText="Older query" variant="bare" />
          <KeyboardShortcut
            handler={handleEscapeShortcut}
            shortcut="Escape"
            tooltipContent={expandedHttpSpanId ? 'Close span inspector' : 'Close query details'}
            tooltipSide="bottom"
          >
            <Button.IconButton ariaLabel="Close query details" name="X" onClick={onClose} size="32" variant="bare" />
          </KeyboardShortcut>
        </div>
      </PageHeader>
      <div className={s.scrollBody}>
        <div className={s.content}>
          <div className={s.sqlBlock}><pre><SqlCode sql={summary.query || 'No SQL recorded for this trace.'} /></pre></div>
          <div className={s.statGrid}>
            <StatCard label="Duration" value={formatDurationFromNanos(summary.durationNanos)} />
            <StatCard label="Rows" value={formatRows(summary)} />
            <StatCard label="Table scans" value={sources.length} />
            <StatCard label="API requests" value={httpSpans.length} />
          </div>
          <DetailTabs activeTab={activeTab} extraTabs={resolvedExtraTabs} onTab={setActiveTab} />
          <div className={s.tabContent}>
            {activeTab === 'timeline' && (
              <TimelineWaterfall
                expandedHttpSpanId={expandedHttpSpanId}
                onExpandedHttpSpanIdChange={setExpandedHttpSpanId}
                onNavigableSpanIdsChange={setNavigableSpanIds}
                spans={detail.spans}
                summary={summary}
              />
            )}
            {activeExtraTab?.content}
          </div>
        </div>
      </div>
    </div>
  )
}

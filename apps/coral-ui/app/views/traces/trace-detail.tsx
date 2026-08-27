import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import classNames from 'classnames'
import { useLocation, useNavigate, useOutletContext } from 'react-router'

import { Button, ScrollArea } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'
import { EmptyPage } from '@/components/empty-page'
import {
  QueryDetailSummary,
  SearchResponseResults,
  mapTraceSearchResponse,
  searchResultsTabLabel,
  type QueryDetailStat,
} from '@/components/query-detail'
import { TraceStatus } from '@/generated/coral/v1/traces_pb'
import { formatDuration, formatDurationFromNanos, nanosToMs } from '@/utils/format-time'

import * as s from './traces.css'
import { HttpSpanDetail } from './http-span-detail'
import { traceLocation } from './trace-location'
import type { TracesOutletContext } from './traces-index'
import { routePath } from '@/routing/routemap'
import { useTimelineTree, type TimelineRow } from './use-timeline-tree'
import { timelineShortcutsEnabled, traceDetailPolicy } from './trace-detail-policy'
import {
  formatInvocation,
  formatRows,
  isHttpSpan,
  isSearchOperation,
  operationCodeLanguage,
  operationDetailLabel,
  operationDetailText,
  spanDisplayLabel,
  spanDisplayMeta,
  sourceNames,
  statusLabel,
  statusTone,
  type TraceDetailData,
  type TraceSpanData,
  type TraceSummaryData,
} from './trace-utils'

export type DetailTab = 'results' | 'timeline' | 'api'
type WaterfallTone = 'query' | 'http' | 'span' | 'error'

const WATERFALL_LABEL_PADDING_INLINE_PX = 10
const INDENT_PX = 14
const DETAIL_PANEL_DEFAULT_RATIO = 0.4
const DETAIL_PANEL_MIN_RATIO = 0.28
const DETAIL_PANEL_MAX_RATIO = 0.6
const DETAIL_PANEL_KEYBOARD_STEP = 0.05
const DETAIL_PANEL_ANIMATION_MS = 180
const NOISY_INTERNAL_SPAN_MAX_MS = 1

function clampDetailPanelRatio(ratio: number) {
  return Math.max(DETAIL_PANEL_MIN_RATIO, Math.min(DETAIL_PANEL_MAX_RATIO, ratio))
}

function focusSpanRow(spanId: string) {
  const escapedSpanId = spanId.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
  const row = document.querySelector<HTMLElement>(`[data-span-row-id="${escapedSpanId}"]`)
  if (!row) return
  row.scrollIntoView({ block: 'nearest' })
  const focusTarget = row.querySelector<HTMLElement>('[role="button"]')
  focusTarget?.focus({ preventScroll: true })
}

export interface ExtraDetailTab {
  description?: string
  id: string
  label: string
  content: React.ReactNode
  show?: boolean
}

interface DetailTabPresentation {
  description?: string
  id: string
  label: string
  show?: boolean
}

function useProMode() {
  return new URLSearchParams(useLocation().search).has('pro')
}

function WaterfallBar({
  left,
  tone,
  width,
  label,
}: {
  left: number
  tone: WaterfallTone
  width: number
  label: string
}) {
  const clampedLeft = Math.max(0, Math.min(100, left))
  const clampedWidth = Math.max(0.5, Math.min(100 - clampedLeft, width))
  const narrow = clampedWidth < 6
  const outsideLabelLeft = clampedLeft + clampedWidth + 0.5
  const outsideLabelStyle =
    outsideLabelLeft > 86
      ? { right: `${Math.max(0, 100 - clampedLeft)}%` }
      : { left: `${outsideLabelLeft}%` }

  return (
    <div className={s.waterfallBarArea}>
      <div
        className={s.waterfallBar}
        data-tone={tone}
        style={{ left: `${clampedLeft}%`, width: `${clampedWidth}%` }}
      >
        {!narrow && <span className={s.waterfallBarLabel}>{label}</span>}
      </div>
      {narrow && (
        <span
          className={s.waterfallBarLabelOutside}
          data-align={outsideLabelLeft > 86 ? 'end' : 'start'}
          style={outsideLabelStyle}
        >
          {label}
        </span>
      )}
    </div>
  )
}

function spanTiming(span: TraceSpanData, traceStart: bigint, durationMs: number) {
  const offsetMs = Number((BigInt(span.startTimeUnixNanos || 0) - traceStart) / 1_000_000n)
  return {
    left: (Math.max(0, offsetMs) / durationMs) * 100,
    width: (nanosToMs(span.durationNanos) / durationMs) * 100,
  }
}

function SpanTimingBar({
  durationMs,
  span,
  traceStart,
}: {
  durationMs: number
  span: TraceSpanData
  traceStart: bigint
}) {
  const timing = spanTiming(span, traceStart, durationMs)
  return (
    <WaterfallBar
      left={timing.left}
      tone={spanTone(span)}
      width={timing.width}
      label={formatDurationFromNanos(span.durationNanos)}
    />
  )
}

function WaterfallTickRow({ durationMs }: { durationMs: number }) {
  const ticks = useMemo(
    () => Array.from({ length: 5 }, (_, index) => (index / 4) * durationMs),
    [durationMs],
  )

  return (
    <div className={s.waterfallTickRow} role="presentation">
      <div className={s.waterfallLabel} />
      <div className={s.waterfallTimeline}>
        {ticks.map((tick, index) => {
          const pct = (tick / durationMs) * 100
          const style =
            index === 0
              ? { left: 0 }
              : index === ticks.length - 1
                ? { right: 0 }
                : { left: `${pct}%`, transform: 'translateX(-50%)' }
          return (
            <span className={s.waterfallTick} key={`${tick}-${index}`} style={style}>
              {formatDuration(tick)}
            </span>
          )
        })}
      </div>
    </div>
  )
}

function spanTone(span: TraceSpanData): WaterfallTone {
  if (span.status === TraceStatus.ERROR) return 'error'
  if (isHttpSpan(span)) return 'http'
  if (span.name === 'coral.query' || span.name === 'coral.search') return 'query'
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
  reserveToggleSpace,
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
  reserveToggleSpace: boolean
  spanId: string
  tone: WaterfallTone
}) {
  return (
    <div
      className={classNames(s.waterfallSpanLabel, { [s.waterfallSpanLabelActive]: active })}
      style={{ paddingInlineStart: WATERFALL_LABEL_PADDING_INLINE_PX + depth * INDENT_PX }}
    >
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
      ) : reserveToggleSpace ? (
        <span aria-hidden className={s.waterfallTreeTogglePlaceholder} />
      ) : null}
      <span className={s.waterfallPluginPill}>
        <span className={s.waterfallPluginDot} data-tone={tone} />
        <span className={s.waterfallLabelText}>
          <Typography.BodySmallStrong as="span" truncate>
            {label}
          </Typography.BodySmallStrong>
          {meta && (
            <Typography.BodySmall as="span" variant="tertiary" truncate>
              {meta}
            </Typography.BodySmall>
          )}
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
  const canExpandHttp = isHttpSpan(span)

  return (
    <div
      className={classNames(s.waterfallBarSlot, {
        [s.waterfallRowHover]: hovered,
        [s.waterfallBarSlotActive]: active,
      })}
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
      <SpanTimingBar durationMs={durationMs} span={span} traceStart={traceStart} />
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
  reserveToggleSpace,
  row,
  showMeta,
}: {
  collapsed: boolean
  expanded: boolean
  hovered: boolean
  onToggle: (spanId: string) => void
  onToggleExpanded: (spanId: string) => void
  onHover: (spanId: string | null) => void
  reserveToggleSpace: boolean
  row: TimelineRow
  showMeta: boolean
}) {
  const { childCount, depth, span } = row
  const tone = spanTone(span)
  const label = spanDisplayLabel(span)
  const meta = showMeta ? spanDisplayMeta(span, label) : ''
  const isNoisyInternalSpan =
    tone === 'span' &&
    span.kind === 'internal' &&
    nanosToMs(span.durationNanos) <= NOISY_INTERNAL_SPAN_MAX_MS
  const canExpandHttp = isHttpSpan(span)

  return (
    <div
      aria-expanded={childCount > 0 ? !collapsed : undefined}
      aria-level={depth + 1}
      className={s.waterfallRowShell}
      data-span-row-id={span.spanId}
      role="treeitem"
    >
      <div
        aria-expanded={canExpandHttp ? expanded : undefined}
        className={classNames(s.waterfallRowButton, {
          [s.waterfallRowHover]: hovered,
          [s.waterfallRowActive]: expanded,
        })}
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
          reserveToggleSpace={reserveToggleSpace}
          spanId={span.spanId}
          tone={tone}
        />
      </div>
    </div>
  )
}

function TimelineWaterfall({
  expandedHttpSpanId,
  onExpandedHttpSpanIdChange,
  onNavigableSpanIdsChange,
  proMode,
  spans,
  summary,
}: {
  expandedHttpSpanId: string | null
  onExpandedHttpSpanIdChange: (
    spanId: string | null | ((current: string | null) => string | null),
  ) => void
  onNavigableSpanIdsChange: (spanIds: string[]) => void
  proMode: boolean
  spans: TraceSpanData[]
  summary?: TraceSummaryData
}) {
  const timelineSpans = useMemo(
    () => (proMode ? spans : spans.filter(isHttpSpan)),
    [proMode, spans],
  )
  const { collapsedSpanIds, rows, toggleSpan } = useTimelineTree(
    timelineSpans,
    summary?.rootSpanId,
    summary?.traceId,
  )
  const hasRenderedDetailPanel = useRef(false)
  const panelAnimationFrame = useRef<number | null>(null)
  const rootRef = useRef<HTMLDivElement>(null)
  const [hoveredSpanId, setHoveredSpanId] = useState<string | null>(null)
  const [detailPanelRatio, setDetailPanelRatio] = useState(DETAIL_PANEL_DEFAULT_RATIO)
  const [animatedDetailPanelRatio, setAnimatedDetailPanelRatio] = useState(
    expandedHttpSpanId ? DETAIL_PANEL_DEFAULT_RATIO : 0,
  )
  const [isResizingDetailPanel, setIsResizingDetailPanel] = useState(false)
  const [renderedHttpSpanId, setRenderedHttpSpanId] = useState<string | null>(expandedHttpSpanId)
  const [detailPanelVisible, setDetailPanelVisible] = useState(Boolean(expandedHttpSpanId))
  const [detailPanelSettled, setDetailPanelSettled] = useState(Boolean(expandedHttpSpanId))
  useEffect(() => onExpandedHttpSpanIdChange(null), [onExpandedHttpSpanIdChange, summary?.traceId])
  useEffect(() => setHoveredSpanId(null), [summary?.traceId])
  useEffect(() => setDetailPanelRatio(DETAIL_PANEL_DEFAULT_RATIO), [summary?.traceId])
  useEffect(() => {
    if (panelAnimationFrame.current !== null) {
      window.cancelAnimationFrame(panelAnimationFrame.current)
      panelAnimationFrame.current = null
    }

    const animatePanelRatio = (from: number, to: number, onDone?: () => void) => {
      const start = performance.now()
      const step = (now: number) => {
        const progress = Math.min(1, (now - start) / DETAIL_PANEL_ANIMATION_MS)
        const eased = 1 - Math.pow(1 - progress, 3)
        setAnimatedDetailPanelRatio(from + (to - from) * eased)
        if (progress < 1) {
          panelAnimationFrame.current = window.requestAnimationFrame(step)
        } else {
          panelAnimationFrame.current = null
          onDone?.()
        }
      }
      setAnimatedDetailPanelRatio(from)
      panelAnimationFrame.current = window.requestAnimationFrame(step)
    }

    if (expandedHttpSpanId) {
      const shouldAnimateOpen = !hasRenderedDetailPanel.current
      hasRenderedDetailPanel.current = true
      setRenderedHttpSpanId(expandedHttpSpanId)
      if (!shouldAnimateOpen) {
        setDetailPanelVisible(true)
        setDetailPanelSettled(true)
        setAnimatedDetailPanelRatio(detailPanelRatio)
        return
      }

      setDetailPanelVisible(true)
      setDetailPanelSettled(false)
      animatePanelRatio(0, detailPanelRatio, () => setDetailPanelSettled(true))
      return () => {
        if (panelAnimationFrame.current !== null)
          window.cancelAnimationFrame(panelAnimationFrame.current)
      }
    }

    hasRenderedDetailPanel.current = false
    setDetailPanelVisible(false)
    setDetailPanelSettled(false)
    animatePanelRatio(animatedDetailPanelRatio, 0, () => setRenderedHttpSpanId(null))
    return () => {
      if (panelAnimationFrame.current !== null)
        window.cancelAnimationFrame(panelAnimationFrame.current)
    }
    // Keep the animation target stable during a single open/close transition.
    // Drag resizing updates both detailPanelRatio and animatedDetailPanelRatio directly.
    // oxlint-disable-next-line react-hooks/exhaustive-deps
  }, [expandedHttpSpanId])
  const navigableSpanIds = useMemo(
    () => rows.filter((row) => isHttpSpan(row.span)).map((row) => row.span.spanId),
    [rows],
  )
  useEffect(() => {
    onNavigableSpanIdsChange(navigableSpanIds)
  }, [onNavigableSpanIdsChange, navigableSpanIds])
  const traceStart = BigInt(summary?.startTimeUnixNanos || rows[0]?.span.startTimeUnixNanos || 0)
  const durationMs = Math.max(nanosToMs(summary?.durationNanos || '0'), 1)
  const renderedHttpSpanIndex = renderedHttpSpanId
    ? navigableSpanIds.indexOf(renderedHttpSpanId)
    : -1
  const renderedHttpRow = rows.find((row) => row.span.spanId === renderedHttpSpanId)
  const renderedHttpSpan = renderedHttpRow?.span

  const selectAdjacentSpan = useCallback(
    (direction: -1 | 1) => {
      if (renderedHttpSpanIndex < 0) return
      const nextSpanId = navigableSpanIds[renderedHttpSpanIndex + direction]
      if (!nextSpanId) return
      onExpandedHttpSpanIdChange(nextSpanId)
      window.requestAnimationFrame(() => focusSpanRow(nextSpanId))
    },
    [renderedHttpSpanIndex, navigableSpanIds, onExpandedHttpSpanIdChange],
  )

  const resizeDetailPanel = useCallback((clientX: number) => {
    const root = rootRef.current
    if (!root) return
    const rect = root.getBoundingClientRect()
    const distanceFromRight = rect.right - clientX
    const nextRatio = clampDetailPanelRatio(distanceFromRight / rect.width)
    setDetailPanelRatio(nextRatio)
    setAnimatedDetailPanelRatio(nextRatio)
  }, [])

  const handleResizePointerDown = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      event.preventDefault()
      setIsResizingDetailPanel(true)
      event.currentTarget.setPointerCapture(event.pointerId)
      resizeDetailPanel(event.clientX)
    },
    [resizeDetailPanel],
  )

  const handleResizePointerEnd = useCallback(() => {
    setIsResizingDetailPanel(false)
  }, [])

  const handleResizePointerMove = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      if (!event.currentTarget.hasPointerCapture(event.pointerId)) return
      resizeDetailPanel(event.clientX)
    },
    [resizeDetailPanel],
  )

  const handleResizeKeyDown = useCallback((event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'ArrowLeft') {
      event.preventDefault()
      setDetailPanelRatio((ratio) => clampDetailPanelRatio(ratio + DETAIL_PANEL_KEYBOARD_STEP))
    } else if (event.key === 'ArrowRight') {
      event.preventDefault()
      setDetailPanelRatio((ratio) => clampDetailPanelRatio(ratio - DETAIL_PANEL_KEYBOARD_STEP))
    } else if (event.key === 'Home') {
      event.preventDefault()
      setDetailPanelRatio(DETAIL_PANEL_MIN_RATIO)
    } else if (event.key === 'End') {
      event.preventDefault()
      setDetailPanelRatio(DETAIL_PANEL_MAX_RATIO)
    }
  }, [])

  if (rows.length === 0) {
    return (
      <EmptyPage
        title="No spans for this trace"
        description="This trace did not record any spans that match the current view."
        iconName="Activity"
      />
    )
  }

  return (
    <div className={s.waterfallRoot} ref={rootRef}>
      <div className={s.waterfallTimelinePane}>
        <ScrollArea.Container className={s.waterfallRowsViewport} constrainWidth>
          <div className={s.waterfallRowsGrid} role="tree">
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
                  onToggleExpanded={(spanId) =>
                    onExpandedHttpSpanIdChange((current) => (current === spanId ? null : spanId))
                  }
                  reserveToggleSpace={proMode}
                  row={row}
                  showMeta={proMode}
                />
              ))}
            </div>
            <div className={s.waterfallTimelineBody}>
              {rows.map((row) => (
                <WaterfallBarSlot
                  active={expandedHttpSpanId === row.span.spanId}
                  durationMs={durationMs}
                  hovered={hoveredSpanId === row.span.spanId}
                  key={row.span.spanId}
                  onHover={setHoveredSpanId}
                  onToggleExpanded={(spanId) =>
                    onExpandedHttpSpanIdChange((current) => (current === spanId ? null : spanId))
                  }
                  row={row}
                  traceStart={traceStart}
                />
              ))}
            </div>
          </div>
        </ScrollArea.Container>
      </div>
      {renderedHttpSpan && (
        <>
          <div
            aria-label="Resize span detail panel"
            aria-orientation="vertical"
            aria-valuemax={Math.round(DETAIL_PANEL_MAX_RATIO * 100)}
            aria-valuemin={Math.round(DETAIL_PANEL_MIN_RATIO * 100)}
            aria-valuenow={Math.round(detailPanelRatio * 100)}
            className={s.waterfallResizeHandle}
            data-open={detailPanelVisible || undefined}
            data-resizing={isResizingDetailPanel || undefined}
            onKeyDown={handleResizeKeyDown}
            onLostPointerCapture={handleResizePointerEnd}
            onPointerDown={handleResizePointerDown}
            onPointerMove={handleResizePointerMove}
            onPointerUp={handleResizePointerEnd}
            role="separator"
            tabIndex={0}
          />
          <aside
            className={s.waterfallSidePanel}
            data-open={detailPanelVisible || undefined}
            data-resizing={isResizingDetailPanel || undefined}
            style={{
              flexBasis: `${animatedDetailPanelRatio * 100}%`,
              minWidth: detailPanelSettled ? 320 : 0,
            }}
          >
            <HttpSpanDetail
              bodySpans={spans}
              canSelectNextSpan={renderedHttpSpanIndex < navigableSpanIds.length - 1}
              canSelectPreviousSpan={renderedHttpSpanIndex > 0}
              onClose={() => onExpandedHttpSpanIdChange(null)}
              onSelectNextSpan={() => selectAdjacentSpan(1)}
              onSelectPreviousSpan={() => selectAdjacentSpan(-1)}
              span={renderedHttpSpan}
              traceStart={traceStart}
            />
          </aside>
        </>
      )}
    </div>
  )
}

function tabDomId(tabId: string): string {
  return tabId.replace(/[^a-zA-Z0-9_-]/g, '-')
}

function DetailTabs({
  activeTab,
  idPrefix,
  onTab,
  tabs,
}: {
  activeTab: string
  idPrefix: string
  onTab: (tab: string) => void
  tabs: DetailTabPresentation[]
}) {
  const visibleTabs = tabs.filter((tab) => tab.show ?? true)
  const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return

    const tabButtons = Array.from(
      event.currentTarget.querySelectorAll<HTMLButtonElement>('[role="tab"]'),
    )
    const currentIndex = tabButtons.indexOf(document.activeElement as HTMLButtonElement)
    if (currentIndex < 0) return

    let nextIndex = currentIndex
    if (event.key === 'ArrowLeft') {
      nextIndex = (currentIndex - 1 + visibleTabs.length) % visibleTabs.length
    } else if (event.key === 'ArrowRight') {
      nextIndex = (currentIndex + 1) % visibleTabs.length
    } else if (event.key === 'Home') {
      nextIndex = 0
    } else if (event.key === 'End') {
      nextIndex = visibleTabs.length - 1
    }

    event.preventDefault()
    onTab(visibleTabs[nextIndex].id)
    tabButtons[nextIndex].focus()
  }

  return (
    <div
      aria-label="Operation details"
      className={s.tabList}
      onKeyDown={handleKeyDown}
      role="tablist"
    >
      {visibleTabs.map((tab) => (
        <div className={s.tabItem} key={tab.id} role="presentation">
          <button
            aria-controls={`${idPrefix}-panel-${tabDomId(tab.id)}`}
            aria-describedby={
              tab.description ? `${idPrefix}-description-${tabDomId(tab.id)}` : undefined
            }
            aria-selected={activeTab === tab.id}
            className={classNames(s.tabTrigger, { [s.tabTriggerActive]: activeTab === tab.id })}
            id={`${idPrefix}-tab-${tabDomId(tab.id)}`}
            onClick={() => onTab(tab.id)}
            role="tab"
            tabIndex={activeTab === tab.id ? 0 : -1}
            type="button"
          >
            <Typography.BodySmallStrong as="span">{tab.label}</Typography.BodySmallStrong>
          </button>
          {tab.description ? (
            <>
              <Tooltip content={tab.description} side="bottom">
                <span aria-hidden="true" className={s.tabInfo}>
                  <Icon name="Info" size="16" />
                </span>
              </Tooltip>
              <span className={s.visuallyHidden} id={`${idPrefix}-description-${tabDomId(tab.id)}`}>
                {tab.description}
              </span>
            </>
          ) : null}
        </div>
      ))}
    </div>
  )
}

function TraceDetailContent({
  detail,
  extraTabs,
  initialSummary,
  loadError,
  newerTraceId,
  olderTraceId,
  onClose,
  onSelectTrace,
  traceId,
}: {
  detail: TraceDetailData | null
  extraTabs?: (detail: TraceDetailData) => ExtraDetailTab[]
  initialSummary?: TraceSummaryData
  loadError: string | null
  newerTraceId?: string | null
  olderTraceId?: string | null
  onClose: () => void
  onSelectTrace?: (traceId: string) => void
  traceId: string
}) {
  const proMode = useProMode()
  const summary = detail?.summary ?? initialSummary
  const searchResultsView = useMemo(
    () => mapTraceSearchResponse(detail?.searchResponse),
    [detail?.searchResponse],
  )
  const searchOperation = Boolean(summary && isSearchOperation(summary))
  const showSearchDiagnosticSpans = Boolean(
    searchOperation &&
    (summary?.status === TraceStatus.ERROR ||
      (detail !== null && searchResultsView.state !== 'available')),
  )
  const showSearchTrace = Boolean(
    searchOperation && (proMode || showSearchDiagnosticSpans || detail?.spans.some(isHttpSpan)),
  )
  const detailPolicy = summary
    ? traceDetailPolicy(summary, searchResultsTabLabel(searchResultsView), showSearchTrace)
    : null
  const defaultTab = detailPolicy?.defaultTab ?? 'timeline'
  const [activeTab, setActiveTab] = useState<string>(() => defaultTab)
  const activeTabChangedByUser = useRef(false)
  const [expandedHttpSpanId, setExpandedHttpSpanId] = useState<string | null>(null)
  const [navigableSpanIds, setNavigableSpanIds] = useState<string[]>([])
  const tabIdPrefix = `trace-detail-${tabDomId(traceId)}`

  const selectAdjacentSpan = useCallback(
    (direction: -1 | 1) => {
      if (!expandedHttpSpanId) return
      const currentIndex = navigableSpanIds.indexOf(expandedHttpSpanId)
      const nextSpanId = currentIndex >= 0 ? navigableSpanIds[currentIndex + direction] : null
      if (!nextSpanId) return
      setExpandedHttpSpanId(nextSpanId)
      window.requestAnimationFrame(() => focusSpanRow(nextSpanId))
    },
    [expandedHttpSpanId, navigableSpanIds],
  )

  const handleNewerTraceShortcut = useCallback(
    (event: KeyboardEvent) => {
      if (!newerTraceId) return
      event.preventDefault()
      onSelectTrace?.(newerTraceId)
    },
    [newerTraceId, onSelectTrace],
  )

  const handleOlderTraceShortcut = useCallback(
    (event: KeyboardEvent) => {
      if (!olderTraceId) return
      event.preventDefault()
      onSelectTrace?.(olderTraceId)
    },
    [olderTraceId, onSelectTrace],
  )

  const handleEscapeShortcut = useCallback(
    (event: KeyboardEvent) => {
      if (activeTab === 'timeline' && expandedHttpSpanId) {
        event.preventDefault()
        setExpandedHttpSpanId(null)
        return
      }
      onClose()
    },
    [activeTab, expandedHttpSpanId, onClose],
  )

  const focusFirstSpan = useCallback(
    (direction: -1 | 1) => {
      if (navigableSpanIds.length === 0) return false
      const firstSpanId =
        direction === 1 ? navigableSpanIds[0] : navigableSpanIds[navigableSpanIds.length - 1]
      setExpandedHttpSpanId(firstSpanId)
      window.requestAnimationFrame(() => focusSpanRow(firstSpanId))
      return true
    },
    [navigableSpanIds],
  )

  const handleSpanArrowShortcut = useCallback(
    (direction: -1 | 1) => (event: KeyboardEvent) => {
      const target = event.target
      if (
        target instanceof HTMLElement &&
        (target.isContentEditable || target.matches('input, textarea, select, [role="textbox"]'))
      )
        return
      if (!expandedHttpSpanId) {
        if (!focusFirstSpan(direction)) return
        event.preventDefault()
        return
      }
      event.preventDefault()
      selectAdjacentSpan(direction)
    },
    [expandedHttpSpanId, focusFirstSpan, selectAdjacentSpan],
  )

  const handlePreviousSpanShortcut = useMemo(
    () => handleSpanArrowShortcut(-1),
    [handleSpanArrowShortcut],
  )
  const handleNextSpanShortcut = useMemo(
    () => handleSpanArrowShortcut(1),
    [handleSpanArrowShortcut],
  )
  const httpSpans = useMemo(() => detail?.spans.filter(isHttpSpan) ?? [], [detail?.spans])
  const sources = useMemo(() => sourceNames(detail?.spans ?? []), [detail?.spans])
  const resolvedExtraTabs = useMemo(
    () => (detail ? (extraTabs?.(detail) ?? []) : []),
    [detail, extraTabs],
  )
  const activeTabAvailable = Boolean(
    detailPolicy?.primaryTabs.some((tab) => tab.id === activeTab) ||
    resolvedExtraTabs.some((tab) => (tab.show ?? true) && tab.id === activeTab),
  )
  useEffect(() => {
    if (!activeTabAvailable) {
      activeTabChangedByUser.current = false
      setActiveTab(defaultTab)
      return
    }
    if (!activeTabChangedByUser.current) setActiveTab(defaultTab)
  }, [activeTabAvailable, defaultTab])

  if (loadError)
    return (
      <div className={s.detailEmpty}>
        <KeyboardShortcut handler={handleEscapeShortcut} shortcut="Escape" />
        <EmptyPage description={loadError} iconName="CircleAlert" title="Tracing unavailable" />
        <Button.TextButton onClick={onClose} variant="secondary">
          Back to query stream
        </Button.TextButton>
      </div>
    )
  if (!summary) {
    return (
      <div className={s.detailEmpty}>
        <KeyboardShortcut handler={handleEscapeShortcut} shortcut="Escape" />
        <EmptyPage
          title="No spans for this trace"
          description="This trace did not include an operation summary or spans to display."
          iconName="Activity"
        />
        <Button.TextButton onClick={onClose} variant="secondary">
          Back to query stream
        </Button.TextButton>
      </div>
    )
  }

  const primaryTabs = detailPolicy?.primaryTabs ?? [{ id: 'timeline', label: 'Trace' }]
  const tabs: DetailTabPresentation[] = [
    ...primaryTabs,
    ...resolvedExtraTabs.map((tab) => ({
      description: tab.description,
      id: tab.id,
      label: tab.label,
      show: tab.show,
    })),
  ]
  const visibleTabs = tabs.filter((tab) => tab.show ?? true)
  const activePanelTab =
    visibleTabs.find((tab) => tab.id === activeTab) ??
    visibleTabs.find((tab) => tab.id === defaultTab) ??
    visibleTabs[0]
  const showTabList = !isSearchOperation(summary) || visibleTabs.length > 1
  const sharedTraceStats: QueryDetailStat[] = [
    { label: 'Duration', value: formatDurationFromNanos(summary.durationNanos) },
    { label: 'Rows', value: formatRows(summary) },
    { label: 'Table scans', value: detail ? sources.length : '—' },
    { label: 'API requests', value: detail ? httpSpans.length : '—' },
  ]
  const searchTraceStats: QueryDetailStat[] = [
    { label: 'Duration', value: formatDurationFromNanos(summary.durationNanos) },
    { label: 'Invocation', value: formatInvocation(summary.invocationKind) },
    { label: 'Table scans', value: detail ? sources.length : '—' },
    { label: 'API requests', value: detail ? httpSpans.length : '—' },
  ]
  const handleTab = (nextTab: string) => {
    activeTabChangedByUser.current = true
    setActiveTab(nextTab)
  }
  const timelineWaterfall = detail ? (
    <TimelineWaterfall
      expandedHttpSpanId={expandedHttpSpanId}
      onExpandedHttpSpanIdChange={setExpandedHttpSpanId}
      onNavigableSpanIdsChange={setNavigableSpanIds}
      proMode={proMode || showSearchDiagnosticSpans}
      spans={detail.spans}
      summary={summary}
    />
  ) : null

  return (
    <QueryDetailSummary
      codeLanguage={operationCodeLanguage(summary)}
      actions={
        <>
          <KeyboardShortcut
            handler={handleNewerTraceShortcut}
            shortcut="$mod+ArrowUp"
            tooltipContent="Newer operation"
            tooltipSide="bottom"
          >
            <Button.IconButton
              ariaLabel="Newer operation"
              disabled={!newerTraceId}
              name="ArrowUp"
              onClick={() => newerTraceId && onSelectTrace?.(newerTraceId)}
              size="32"
              variant="bare"
            />
          </KeyboardShortcut>
          <KeyboardShortcut
            handler={handleOlderTraceShortcut}
            shortcut="$mod+ArrowDown"
            tooltipContent="Older operation"
            tooltipSide="bottom"
          >
            <Button.IconButton
              ariaLabel="Older operation"
              disabled={!olderTraceId}
              name="ArrowDown"
              onClick={() => olderTraceId && onSelectTrace?.(olderTraceId)}
              size="32"
              variant="bare"
            />
          </KeyboardShortcut>
          <KeyboardShortcut
            handler={handleEscapeShortcut}
            shortcut="Escape"
            tooltipContent={
              activeTab === 'timeline' && expandedHttpSpanId
                ? 'Close span inspector'
                : 'Close operation details'
            }
            tooltipSide="bottom"
          >
            <Button.IconButton
              ariaLabel="Close operation details"
              name="X"
              onClick={onClose}
              size="32"
              variant="bare"
            />
          </KeyboardShortcut>
        </>
      }
      shortcuts={
        timelineShortcutsEnabled(summary, activeTab) ? (
          <>
            <KeyboardShortcut handler={handlePreviousSpanShortcut} shortcut="ArrowUp" />
            <KeyboardShortcut handler={handleNextSpanShortcut} shortcut="ArrowDown" />
          </>
        ) : undefined
      }
      sql={operationDetailText(summary)}
      stats={isSearchOperation(summary) ? searchTraceStats : sharedTraceStats}
      statusLabel={statusLabel(summary.status)}
      statusTone={statusTone(summary.status)}
      title={
        <>
          <Button.TextButton onClick={onClose} size="22" variant="linkSubtle">
            <Typography.BodyStrong as="span" variant="tertiary">
              Query stream
            </Typography.BodyStrong>
          </Button.TextButton>
          <Typography.BodyStrong as="span" variant="tertiary">
            /
          </Typography.BodyStrong>
          <Typography.BodyStrong as="span" variant="secondary">
            {operationDetailLabel(summary)}
          </Typography.BodyStrong>
        </>
      }
    >
      {showTabList ? (
        <DetailTabs
          activeTab={activePanelTab?.id ?? activeTab}
          idPrefix={tabIdPrefix}
          onTab={handleTab}
          tabs={tabs}
        />
      ) : null}
      {visibleTabs.map((tab) => {
        const isActive = tab.id === activePanelTab?.id
        const extraTab = resolvedExtraTabs.find((candidate) => candidate.id === tab.id)

        return (
          <div
            aria-label={showTabList ? undefined : tab.label}
            aria-labelledby={showTabList ? `${tabIdPrefix}-tab-${tabDomId(tab.id)}` : undefined}
            className={s.tabContent}
            hidden={!isActive}
            id={`${tabIdPrefix}-panel-${tabDomId(tab.id)}`}
            key={tab.id}
            role={showTabList ? 'tabpanel' : 'region'}
          >
            {isActive ? (
              <>
                {tab.id === 'results' ? <SearchResponseResults view={searchResultsView} /> : null}
                {tab.id === 'timeline' ? timelineWaterfall : null}
                {extraTab?.content}
              </>
            ) : null}
          </div>
        )
      })}
    </QueryDetailSummary>
  )
}

export function TraceDetail({
  detail,
  extraTabs,
  loadError,
  traceId,
}: {
  detail: TraceDetailData | null
  extraTabs?: (detail: TraceDetailData) => ExtraDetailTab[]
  loadError: string | null
  traceId: string
}) {
  const { traces, workspaceId } = useOutletContext<TracesOutletContext>()
  const location = useLocation()
  const navigate = useNavigate()
  const selectedIndex = traces.findIndex((trace) => trace.traceId === traceId)
  const newerTraceId = selectedIndex > 0 ? traces[selectedIndex - 1].traceId : null
  const olderTraceId =
    selectedIndex >= 0 && selectedIndex < traces.length - 1
      ? traces[selectedIndex + 1].traceId
      : null
  const initialSummary = selectedIndex >= 0 ? traces[selectedIndex] : detail?.summary

  return (
    <div className={s.detailOverlay}>
      <TraceDetailContent
        key={traceId}
        detail={detail}
        extraTabs={extraTabs}
        initialSummary={initialSummary}
        loadError={loadError}
        newerTraceId={newerTraceId}
        olderTraceId={olderTraceId}
        onClose={() =>
          navigate({
            pathname: routePath('workspaceTraces', { workspaceId }),
            search: location.search,
          })
        }
        onSelectTrace={(nextTraceId) =>
          navigate(traceLocation(workspaceId, nextTraceId, location.search))
        }
        traceId={traceId}
      />
    </div>
  )
}

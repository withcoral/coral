import classNames from 'classnames'
import { Link, useLocation, useParams } from 'react-router'

import { HighlightedCode } from '@/components/code-block'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as s from './traces.css'
import { rootSpanIdFromSearch, traceLocation } from './trace-location'
import {
  durationClass,
  formatDurationFromNanos,
  formatTimestamp,
  startMs,
  statusTone,
  timeAgo,
  traceEntryKey,
  traceRowId,
  type TraceSummaryData,
} from './trace-utils'

function TraceRow({
  active,
  referenceTimeMs,
  search,
  selected,
  trace,
  workspaceId,
}: {
  active: boolean
  referenceTimeMs: number
  search: string
  selected: boolean
  trace: TraceSummaryData
  workspaceId: string
}) {
  return (
    <Link
      aria-current={selected ? 'page' : undefined}
      className={s.fullRow}
      data-active={active || undefined}
      data-trace-row-id={traceRowId(trace)}
      to={traceLocation(workspaceId, trace.traceId, search, trace.rootSpanId)}
    >
      <span className={s.statusDot} data-tone={statusTone(trace.status)} />
      <div className={classNames(s.cell, s.cellTimestamp)}>
        <Tooltip content={formatTimestamp(startMs(trace))} side="right">
          <Typography.Body as="span" variant="tertiary">
            {timeAgo(startMs(trace), referenceTimeMs)}
          </Typography.Body>
        </Tooltip>
      </div>
      <div className={s.sqlPreview}>
        <HighlightedCode
          className={s.sqlInlineCode}
          code={trace.query || trace.name || trace.traceId}
          language="sql"
        />
      </div>
      <div
        className={classNames(
          s.cell,
          s.cellDuration,
          durationClass(trace.durationNanos, s.durationWarning, s.durationDefault),
        )}
      >
        <Typography.Body as="span">{formatDurationFromNanos(trace.durationNanos)}</Typography.Body>
      </div>
    </Link>
  )
}

export function TraceList({
  activeTraceKey,
  referenceTimeMs,
  traces,
  workspaceId,
}: {
  activeTraceKey?: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
  workspaceId: string
}) {
  const location = useLocation()
  const { traceId: selectedTraceId } = useParams()
  const selectedRootSpanId = rootSpanIdFromSearch(location.search)
  const selectedTrace = traces.find(
    (trace) =>
      trace.traceId === selectedTraceId &&
      (!selectedRootSpanId || trace.rootSpanId === selectedRootSpanId),
  )
  const selectedTraceKey = selectedTrace ? traceEntryKey(selectedTrace) : null
  return (
    <div className={s.traceList}>
      {traces.map((trace) => (
        <TraceRow
          active={traceEntryKey(trace) === activeTraceKey}
          key={traceEntryKey(trace)}
          referenceTimeMs={referenceTimeMs}
          search={location.search}
          selected={traceEntryKey(trace) === selectedTraceKey}
          trace={trace}
          workspaceId={workspaceId}
        />
      ))}
    </div>
  )
}

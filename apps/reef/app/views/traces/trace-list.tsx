import classNames from 'classnames'
import { NavLink, useLocation } from 'react-router'

import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as s from './traces.css'
import { SqlCode } from './sql-code'
import {
  durationClass,
  formatDurationFromNanos,
  formatTimestamp,
  startMs,
  statusTone,
  timeAgo,
  type TraceSummaryData,
} from './trace-utils'

function TraceRow({
  active,
  referenceTimeMs,
  search,
  trace,
}: {
  active: boolean
  referenceTimeMs: number
  search: string
  trace: TraceSummaryData
}) {
  return (
    <NavLink
      className={s.fullRow}
      data-active={active || undefined}
      data-trace-row-id={trace.traceId}
      to={{ pathname: `/traces/${encodeURIComponent(trace.traceId)}`, search }}
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
        <SqlCode inline sql={trace.query || trace.name || trace.traceId} />
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
    </NavLink>
  )
}

export function TraceList({
  activeTraceId,
  referenceTimeMs,
  traces,
}: {
  activeTraceId?: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
}) {
  const location = useLocation()
  return (
    <div className={s.traceList}>
      {traces.map((trace) => (
        <TraceRow
          active={trace.traceId === activeTraceId}
          key={trace.traceId}
          referenceTimeMs={referenceTimeMs}
          search={location.search}
          trace={trace}
        />
      ))}
    </div>
  )
}

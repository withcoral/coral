import classNames from 'classnames'
import { NavLink, useLocation } from 'react-router'

import { HighlightedCode } from '@/components/code-block'
import { routePath } from '@/routing/routemap'
import { formatDurationFromNanos, formatTimestamp, timeAgo } from '@/utils/format-time'
import { useNow } from '@/utils/use-now'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'
import { Avatar } from '@/wax/components/avatar'

import * as s from './traces.css'
import {
  durationClass,
  operationCodeLanguage,
  operationPreview,
  startMs,
  statusTone,
  traceUserLabel,
  type TraceSummaryData,
} from './trace-utils'

/** How recent the newest trace must be for a label to count seconds. */
const SECONDS_LABEL_WINDOW_MS = 60_000

function TraceRow({
  active,
  nowMs,
  search,
  trace,
  workspaceId,
}: {
  active: boolean
  nowMs: number
  search: string
  trace: TraceSummaryData
  workspaceId: string
}) {
  return (
    <NavLink
      className={s.fullRow}
      data-active={active || undefined}
      data-trace-row-id={trace.traceId}
      to={{
        pathname: routePath('workspaceTrace', { traceId: trace.traceId, workspaceId }),
        search,
      }}
    >
      <span className={s.statusDot} data-tone={statusTone(trace.status)} />
      <div className={classNames(s.cell, s.cellTimestamp)}>
        <Tooltip content={formatTimestamp(startMs(trace))} side="right">
          <Typography.Body as="span" variant="tertiary">
            {timeAgo(startMs(trace), nowMs)}
          </Typography.Body>
        </Tooltip>
      </div>
      <div className={s.sqlPreview}>
        <HighlightedCode
          className={s.sqlInlineCode}
          code={operationPreview(trace)}
          language={operationCodeLanguage(trace)}
        />
      </div>
      {trace.user && (
        <div className={classNames(s.cell, s.cellUser)}>
          <Avatar name={traceUserLabel(trace.user)} seed={trace.user.id} />
          <Typography.Body as="span" className={s.userLabel} variant="secondary">
            {traceUserLabel(trace.user)}
          </Typography.Body>
        </div>
      )}
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
  workspaceId,
}: {
  activeTraceId?: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
  workspaceId: string
}) {
  const location = useLocation()
  // The list revalidates every 30s, which refreshes referenceTimeMs and so keeps
  // minute-scale labels current on its own. Tick only for second-scale labels.
  const newestStartMs = Math.max(0, ...traces.map(startMs))
  const shouldCountSeconds = referenceTimeMs - newestStartMs < SECONDS_LABEL_WINDOW_MS
  const { now } = useNow({
    refreshAfterMs: shouldCountSeconds ? 1_000 : undefined,
    seedMs: referenceTimeMs,
  })

  return (
    <div className={s.traceList}>
      {traces.map((trace) => (
        <TraceRow
          active={trace.traceId === activeTraceId}
          key={trace.traceId}
          nowMs={now.getTime()}
          search={location.search}
          trace={trace}
          workspaceId={workspaceId}
        />
      ))}
    </div>
  )
}

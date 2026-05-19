import classNames from 'classnames'

import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'
import type { TraceSummary } from '@/generated/coral/v1/traces_pb'

import * as s from '../traces-page.css'
import { SqlCode } from './sql-code'
import { durationClass, formatDurationFromNanos, formatTimestamp, startMs, statusTone, timeAgo } from './trace-utils'

function TraceRow({ onSelect, trace }: { onSelect: () => void; trace: TraceSummary }) {
  return (
    <button className={s.fullRow} onClick={onSelect} type="button">
      <span className={s.statusDot} data-tone={statusTone(trace.status)} />
      <div className={classNames(s.cell, s.cellTimestamp)}>
        <Tooltip content={formatTimestamp(startMs(trace))} side="right">
          <Typography.Body as="span" variant="tertiary">{timeAgo(startMs(trace))}</Typography.Body>
        </Tooltip>
      </div>
      <div className={s.sqlPreview}><SqlCode inline sql={trace.query || trace.name || trace.traceId} /></div>
      <div className={classNames(s.cell, s.cellDuration, durationClass(trace.durationNanos, s.durationWarning, s.durationDefault))}>
        <Typography.Body as="span">{formatDurationFromNanos(trace.durationNanos)}</Typography.Body>
      </div>
    </button>
  )
}

export function TraceList({ traces, onSelect }: { traces: TraceSummary[]; onSelect: (traceId: string) => void }) {
  return <div className={s.traceList}>{traces.map((trace) => <TraceRow key={trace.traceId} onSelect={() => onSelect(trace.traceId)} trace={trace} />)}</div>
}

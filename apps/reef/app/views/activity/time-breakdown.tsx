import { useMemo, useState } from 'react'
import { traceChildren, type ActivityTraceStore } from '@/lib/activity-trace'
import { Typography } from '@/wax/components'
import { Meter } from '@/wax/components/charts/meter'
import { Skeleton, SkeletonText } from '@/wax/components/skeleton'
import { InfoTip } from '@/wax/components/tooltip'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { preciseDuration } from './duration'
import { share } from './summary/format'
import { isOngoing } from './trace'
import { timeSummary, type TimeSummary } from './trace-summary'
import type { ActivityRow } from './types'
import * as styles from './time-breakdown.css'

const timeParts = [
  ['calls', 'Tool calls', 'From the dispatch of each provider call to its end.'],
  ['waiting', 'Waiting', 'A call waits before dispatch, such as for an approval.'],
  [
    'programs',
    'Programs',
    'A program or the code of a tool runs with no provider call in progress.',
  ],
  ['discovery', 'Search and describe', 'A task tool call that is not a run.'],
  [
    'agentModel',
    'Agent model',
    'The time that no recorded activity covers. It is not a direct measurement of the model.',
  ],
] as const

/** What the Agent model part can and cannot say, from the info control beside its name. */
const agentModelLimits =
  'This is the time that no recorded activity covers. It is not a direct measurement of the model. It can also include time that Coral did not record, such as client work, network delay or idle time.'
type TimePart = (typeof timeParts)[number]

/**
 * The parts that show. A search or describe outside a Run is rare, so its part shows only
 * for a Task that has one. The Task's own records say so before the rest loads.
 */
const shownParts = (taskTools: boolean) =>
  timeParts.filter(([key]) => key !== 'discovery' || taskTools)

/**
 * One part: its name, its value, its share, and a bar of that share. Without a value, a
 * placeholder stands in each place, which no screen reader reads as a number.
 */
function Part({
  part: [key, label, meaning],
  value,
  total,
}: {
  part: TimePart
  value?: number
  total?: number
}) {
  return (
    <li className={styles.part}>
      {key === 'agentModel' ? (
        // Its limits open from a button that Tab, a click or a touch reaches.
        <span className={styles.label}>
          <Typography.BodySmall>{label}</Typography.BodySmall>
          <InfoTip ariaLabel="About Agent model">{agentModelLimits}</InfoTip>
        </span>
      ) : (
        <Typography.BodySmall>{label}</Typography.BodySmall>
      )}
      {value === undefined || total === undefined ? (
        <>
          <span className={styles.pendingNumber} aria-hidden>
            <SkeletonText variant="bodySmall" width="48px" />
          </span>
          <span className={styles.pendingNumber} aria-hidden>
            <SkeletonText variant="bodySmall" width="28px" />
          </span>
          <Skeleton height={4} width="100%" borderRadius={4} />
        </>
      ) : (
        <>
          <Typography.CodeSmallInline variant="primary" className={styles.number}>
            {preciseDuration(value)}
          </Typography.CodeSmallInline>
          <Typography.CodeSmallInline variant="tertiary" className={styles.number}>
            {share(value, total)}
          </Typography.CodeSmallInline>
          <Meter value={value} max={total} size="small" />
        </>
      )}
      {/* What the part measures, for screen readers. No visible control shows it. */}
      <span className={visuallyHidden}>{meaning}</span>
    </li>
  )
}

const ongoingNote = 'Work is in progress. These values change.'

/**
 * What `count` records with no recorded end mean for the parts. Each counts up to its last
 * recorded activity. Only an ended Task's time goes on after that, and Agent model can then
 * hold it; under any other root the time measured stops there too.
 */
const noEndNote = (count: number, endedTask: boolean) => {
  const one = count === 1
  const records = one ? '1 record has' : `${count} records have`
  return endedTask
    ? `${records} no recorded end. Agent model can include time after ${one ? 'its' : 'their'} last recorded activity.`
    : `${records} no recorded end. ${one ? 'It counts' : 'They count'} up to ${one ? 'its' : 'their'} last recorded activity.`
}

/** A note that showed for the root and does not now: it keeps its place, unseen. */
const HeldNote = ({ text }: { text: string }) => (
  <Typography.BodySmall className={styles.placeholder} aria-hidden>
    {text}
  </Typography.BodySmall>
)

/**
 * Where the time of one root went, part by part, with each part's share of the whole.
 * While records load, each part keeps its place with a placeholder for its value. A note
 * that showed for the root keeps its place, unseen, once it goes, and a root that goes on
 * holds the place of its note of work in progress from the start. So the table under the
 * breakdown stays put while Live reads the root again.
 */
export function TimeBreakdown({
  root,
  summary,
  taskTools,
  rootRow,
  announced,
}: {
  /** The root the breakdown is of. The places of its notes hold while it shows. */
  root: string
  summary: TimeSummary
  /** Whether the root is a Task with a tool call of its own, outside its Runs. */
  taskTools: boolean
  /** The root's own record: whether it goes on, and whether it is a Task that ended. */
  rootRow?: ActivityRow
  /** Whether another status already says that records load. */
  announced: boolean
}) {
  const ready = summary.state === 'ready'
  // A root that goes on can have work in progress.
  const live = Boolean(rootRow && rootRow.kind !== 'output' && isOngoing(rootRow))
  const endedTask = rootRow?.kind === 'task' && Boolean(rootRow.endedAt)
  const noEnd = ready && summary.unknownEnds > 0 ? noEndNote(summary.unknownEnds, endedTask) : null
  // The notes the root has shown, or will: they keep their places while it shows.
  const [held, setHeld] = useState({ root, ongoing: false, noEnd: null as string | null })
  const before = held.root === root ? held : { root, ongoing: false, noEnd: null }
  const notes = {
    root,
    ongoing: before.ongoing || live || (ready && summary.ongoing),
    noEnd: noEnd ?? before.noEnd,
  }
  if (notes.root !== held.root || notes.ongoing !== held.ongoing || notes.noEnd !== held.noEnd)
    setHeld(notes)
  const parts = shownParts(taskTools)
  if (summary.state === 'failed' || summary.state === 'unavailable')
    return (
      <div className={styles.content}>
        <Typography.BodySmall variant="secondary">
          {summary.state === 'failed'
            ? 'Time is unavailable. The records did not load.'
            : 'Time is unavailable. Some records are unavailable.'}
        </Typography.BodySmall>
      </div>
    )
  return (
    <div className={styles.content}>
      {summary.state === 'ready' ? (
        <ul className={styles.parts} aria-label="Time by part">
          {parts.map((part) => (
            <Part key={part[0]} part={part} value={summary[part[0]]} total={summary.total} />
          ))}
        </ul>
      ) : (
        <ul className={styles.parts} aria-label="Time by part" aria-busy>
          {parts.map((part) => (
            <Part key={part[0]} part={part} />
          ))}
        </ul>
      )}
      {summary.state === 'ready' && summary.ongoing ? (
        <Typography.BodySmall role="status">{ongoingNote}</Typography.BodySmall>
      ) : (
        notes.ongoing && <HeldNote text={ongoingNote} />
      )}
      {noEnd ? (
        <Typography.BodySmall>{noEnd}</Typography.BodySmall>
      ) : (
        notes.noEnd && <HeldNote text={notes.noEnd} />
      )}
      {summary.state === 'loading' && !announced && (
        <Typography.BodySmall role="status" className={visuallyHidden}>
          Loading records…
        </Typography.BodySmall>
      )}
    </div>
  )
}

/** Whether the root is a Task with a tool call of its own, such as a search. */
export function useTaskTools(store: ActivityTraceStore) {
  return useMemo(
    () =>
      store.rootRow?.kind === 'task' &&
      traceChildren(store, store.root).some((row) => row.kind === 'tool'),
    [store],
  )
}

/**
 * The time summary of one root. It reads every record the trace holds, not the rows on
 * screen, and states a value only when the trace holds them all.
 */
export function useTimeSummary(
  store: ActivityTraceStore,
  /** Ends the time of work in progress. */
  now: number,
): TimeSummary {
  // Only work in progress moves with the clock: the time of ended work is worked out once.
  const ended = useMemo(() => timeSummary(store, 0), [store])
  const moving = ended.state === 'ready' && ended.ongoing
  return useMemo(() => (moving ? timeSummary(store, now) : ended), [moving, ended, store, now])
}

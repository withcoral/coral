import type { ReactNode } from 'react'
import * as DescriptionList from '@/wax/components/description-list'
import { ContextImpact } from './context-impact'
import { DetailHeader } from './detail-header'
import { RecordedDuration } from './duration'
import { Instant } from './instant'
import { isOngoing } from './trace'
import type { ActivityRow } from './types'
import * as styles from './task-header.css'

type TaskRow = Extract<ActivityRow, { kind: 'task' }>

/**
 * A Task above its waterfall, with what its own details said: its state, the context its
 * results used, how long it took and when, and the feedback its agent gave. Its waterfall
 * below holds the records under it, so no rail shows the Task itself.
 */
export function TaskHeader({
  task,
  feedback,
  actions,
}: {
  task: Omit<TaskRow, 'kind'>
  /** What the agent said of the Task when it ended, when Coral has it. */
  feedback?: string
  /** Controls for the Task, such as following it live. */
  actions?: ReactNode
}) {
  return (
    <section aria-label="Task" className={styles.header}>
      <DetailHeader
        title={task.intent}
        status={task.outcome}
        heading="h2"
        sticky={false}
        actions={actions}
      />
      <DescriptionList.List density="inline">
        {/* The Task's stored total, as its row in the list shows it: — when unavailable. */}
        <DescriptionList.Row label="Context">
          <ContextImpact bytes={task.contextImpactBytes} partial={task.contextImpactPartial} />
        </DescriptionList.Row>
        <DescriptionList.Row label="Duration">
          <RecordedDuration
            startedAt={task.startedAt}
            endedAt={task.endedAt}
            ongoing={isOngoing({ kind: 'task', ...task })}
          />
        </DescriptionList.Row>
        <DescriptionList.Row label="Started">
          <Instant value={task.startedAt} />
        </DescriptionList.Row>
        {task.endedAt && (
          <DescriptionList.Row label="Ended">
            <Instant value={task.endedAt} />
          </DescriptionList.Row>
        )}
        {feedback !== undefined && (
          <DescriptionList.Row label="Feedback">
            {/* Keeps the agent's line breaks; the row owns the typography. */}
            <span className={styles.feedback}>{feedback}</span>
          </DescriptionList.Row>
        )}
      </DescriptionList.List>
    </section>
  )
}

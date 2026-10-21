import type { ReactNode } from 'react'
import { Card, Typography } from '@/wax/components'
import { Icon, type IconName } from '@/wax/components/icon'
import { NumberPopIn } from '@/wax/components/number-pop-in'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import {
  SUMMARY_DAYS,
  type ActivitySummaryData,
  type ApprovalDecisions,
  type CallOutcome,
  type CallOutcomes,
  type ContextEntry,
  type OperationCalls,
  type StopReason,
  type StoppedOperation,
  type SummaryDays,
} from '@/lib/activity-summary'
import { ContextImpact, describeContextImpact } from '../context-impact'
import * as styles from './summary.css'

const outcomes: Record<CallOutcome, { label: string; icon: IconName; spoken: string }> = {
  answered: { label: 'Answered', icon: 'CircleCheck', spoken: 'answered' },
  blocked: { label: 'Blocked by a rule', icon: 'ShieldBan', spoken: 'blocked' },
  notAllowed: { label: 'Not allowed', icon: 'UserRoundX', spoken: 'not allowed' },
  needsSetup: { label: 'Needs setup', icon: 'Wrench', spoken: 'need setup' },
  failed: { label: 'Failed', icon: 'CircleX', spoken: 'failed' },
  canceled: { label: 'Canceled', icon: 'CircleSlash', spoken: 'canceled' },
}
const order: readonly CallOutcome[] = [
  'answered',
  'blocked',
  'notAllowed',
  'needsSetup',
  'failed',
  'canceled',
]
const stopReasons: readonly StopReason[] = ['blocked', 'notAllowed', 'needsSetup']
const stoppedTotal = (counts: Record<StopReason, number>) =>
  stopReasons.reduce((sum, reason) => sum + counts[reason], 0)

const callTotal = (calls: CallOutcomes) => order.reduce((sum, outcome) => sum + calls[outcome], 0)
const number = (value: number) => value.toLocaleString('en-US')
const plural = (value: number, noun: string) => `${number(value)} ${noun}${value === 1 ? '' : 's'}`
const percent = (part: number, whole: number) =>
  whole === 0 ? '0%' : `${Math.round((part / whole) * 100)}%`
const windowLabel = (days: number) => (days === 1 ? 'Last 24 hours' : `Last ${days} days`)
const dayOptions = SUMMARY_DAYS.map((days) => ({
  value: String(days),
  label: days === 1 ? '24 hours' : `${days} days`,
}))

/**
 * Rows past the limit add up to one "others" row. Hiding a single row saves
 * no space, so a list one over the limit shows whole.
 */
function withOthers<T>(entries: readonly T[], limit: number) {
  const shown = entries.length > limit + 1 ? entries.slice(0, limit) : entries
  return { shown, others: entries.slice(shown.length) }
}

export interface SummaryTileProps {
  label: string
  icon: IconName
  value: string
  /** Spoken in place of the value when the value alone is ambiguous. */
  valueLabel?: string
  detail?: ReactNode
  valueColor?: 'success' | 'error' | 'tertiary'
}

/** One number with its label. */
export function SummaryTile({
  label,
  icon,
  value,
  valueLabel,
  detail,
  valueColor,
}: SummaryTileProps) {
  return (
    <Card.Container className={styles.tile}>
      <span className={styles.tileLabel}>
        <Icon name={icon} size="16" color="tertiary" />
        <Typography.BodySmall as="span" variant="tertiary">
          {label}
        </Typography.BodySmall>
      </span>
      <Typography.HeadingMedium
        as="span"
        className={styles.tileValue}
        variant={valueColor}
        aria-label={valueLabel}
      >
        <NumberPopIn value={value} />
      </Typography.HeadingMedium>
      {detail && (
        <Typography.BodySmall as="span" variant="tertiary">
          {detail}
        </Typography.BodySmall>
      )}
    </Card.Container>
  )
}

/** Every ended call in the window, with the share the provider answered. */
export function CallsTile({ calls }: { calls: CallOutcomes }) {
  const total = callTotal(calls)
  return (
    <SummaryTile
      label="Calls"
      icon="Activity"
      value={number(total)}
      detail={total > 0 ? `${percent(calls.answered, total)} answered` : 'No calls'}
    />
  )
}

/** Calls that a rule or a person stopped, or that need setup. */
export function StoppedTile({ calls }: { calls: CallOutcomes }) {
  const total = stoppedTotal(calls)
  return (
    <SummaryTile
      label="Stopped"
      icon="ShieldBan"
      value={number(total)}
      detail={
        total > 0
          ? stopReasons
              .filter((reason) => calls[reason] > 0)
              .map((reason) => `${number(calls[reason])} ${outcomes[reason].spoken}`)
              .join(' · ')
          : 'None'
      }
    />
  )
}

/** Net context bytes. A partial total leaves out running and unmeasured runs. */
export function ContextSavedTile({
  bytes,
  partial = false,
}: {
  bytes?: string
  partial?: boolean
}) {
  const impact = bytes === undefined ? undefined : describeContextImpact(BigInt(bytes), false)
  return (
    <SummaryTile
      label="Context"
      icon="Layers"
      value={impact?.label ?? '—'}
      valueLabel={
        impact === undefined
          ? 'Context impact unavailable'
          : `${impact.meaning}${partial ? ', incomplete' : ''}`
      }
      valueColor={impact?.variant ?? 'tertiary'}
      detail={
        impact === undefined ? 'Not measured' : `${impact.change}${partial ? ' · Incomplete' : ''}`
      }
    />
  )
}

/** One bar per call outcome, each against the window's total calls. */
export function CallOutcomeChart({ calls }: { calls: CallOutcomes }) {
  const total = callTotal(calls)
  return (
    <ul className={styles.rows} aria-label="Call outcomes">
      {order.map((outcome) => {
        const { label, icon } = outcomes[outcome]
        const value = calls[outcome]
        return (
          <li key={outcome} className={styles.row}>
            <span className={styles.splitPart}>
              <Icon name={icon} size="14" color="tertiary" />
              <Typography.BodySmall as="span" className={styles.rowName}>
                {label}
              </Typography.BodySmall>
            </span>
            <Typography.BodySmall as="span" variant="tertiary" className={styles.count}>
              {number(value)} · {percent(value, total)}
            </Typography.BodySmall>
            <span className={`${styles.bar} ${styles.rowBar}`} aria-hidden>
              {value > 0 && (
                <span className={styles.fill} style={{ width: `${(value / total) * 100}%` }} />
              )}
            </span>
          </li>
        )
      })}
    </ul>
  )
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Card.Container as="section" aria-label={title}>
      <Typography.HeadingXSmall as="h3">{title}</Typography.HeadingXSmall>
      {children}
    </Card.Container>
  )
}

function Empty({ children }: { children: string }) {
  return (
    <Typography.BodySmall as="p" variant="tertiary">
      {children}
    </Typography.BodySmall>
  )
}

/** The operations with the most stopped calls, most first, each split by who must act. */
export function StoppedOperations({
  operations,
  limit = 5,
}: {
  operations: readonly StoppedOperation[]
  limit?: number
}) {
  const { shown, others } = withOthers(operations, limit)
  const sum = (reason: StopReason) => others.reduce((total, op) => total + op[reason], 0)
  return (
    <Panel title="Stopped operations">
      {shown.length === 0 ? (
        <Empty>No stopped calls.</Empty>
      ) : (
        <ul className={styles.rows}>
          {shown.map((op) => (
            <StoppedRow key={op.callPath} name={<CallPath path={op.callPath} />} counts={op} />
          ))}
          {others.length > 0 && (
            <StoppedRow
              name={<OthersName count={others.length} />}
              counts={{
                blocked: sum('blocked'),
                notAllowed: sum('notAllowed'),
                needsSetup: sum('needsSetup'),
              }}
            />
          )}
        </ul>
      )}
    </Panel>
  )
}

function StoppedRow({ name, counts }: { name: ReactNode; counts: Record<StopReason, number> }) {
  return (
    <li className={styles.row}>
      {name}
      <span className={styles.split}>
        {stopReasons
          .filter((reason) => counts[reason] > 0)
          .map((reason) => (
            <SplitPart key={reason} outcome={reason} value={counts[reason]} />
          ))}
      </span>
    </li>
  )
}

function SplitPart({ outcome, value }: { outcome: CallOutcome; value: number }) {
  return (
    <span className={styles.splitPart}>
      <Icon name={outcomes[outcome].icon} size="14" color="tertiary" />
      <Typography.BodySmall as="span">
        <span aria-hidden>{number(value)}</span>
        <span className={visuallyHidden}>{`${number(value)} ${outcomes[outcome].spoken}`}</span>
      </Typography.BodySmall>
    </span>
  )
}

function CallPath({ path }: { path: string }) {
  return <Typography.CodeSmallInline className={styles.rowName}>{path}</Typography.CodeSmallInline>
}

function OthersName({ count }: { count: number }) {
  return (
    <Typography.BodySmall as="span" variant="tertiary" className={styles.rowName}>
      {count} others
    </Typography.BodySmall>
  )
}

/** The operations with the most ended calls, busiest first, as bars against the busiest one. */
export function TopOperations({
  operations,
  limit = 5,
}: {
  operations: readonly OperationCalls[]
  limit?: number
}) {
  const { shown, others } = withOthers(operations, limit)
  const max = shown[0]?.calls ?? 0
  return (
    <Panel title="Most called operations">
      {shown.length === 0 ? (
        <Empty>No calls.</Empty>
      ) : (
        <ul className={styles.rows}>
          {shown.map((op) => (
            <li key={op.callPath} className={styles.row}>
              <CallPath path={op.callPath} />
              <Typography.BodySmall as="span" className={styles.count}>
                {plural(op.calls, 'call')}
              </Typography.BodySmall>
              <span className={`${styles.bar} ${styles.rowBar}`} aria-hidden>
                <span className={styles.fill} style={{ width: `${(op.calls / max) * 100}%` }} />
              </span>
            </li>
          ))}
          {/* The others share no one bar, so their row shows only the sum. */}
          {others.length > 0 && (
            <li className={styles.row}>
              <OthersName count={others.length} />
              <Typography.BodySmall as="span" className={styles.count}>
                {plural(
                  others.reduce((sum, op) => sum + op.calls, 0),
                  'call',
                )}
              </Typography.BodySmall>
            </li>
          )}
        </ul>
      )}
    </Panel>
  )
}

/** Net context bytes per caller, most saved first. */
export function ContextBreakdown({
  title,
  entries,
  limit = 3,
}: {
  title: string
  entries: readonly ContextEntry[]
  limit?: number
}) {
  const { shown, others } = withOthers(entries, limit)
  return (
    <Panel title={title}>
      {shown.length === 0 ? (
        <Empty>No measured activity.</Empty>
      ) : (
        <ul className={styles.rows} aria-label={title}>
          {shown.map((entry) => (
            <li key={entry.key} className={styles.row}>
              <Typography.BodySmall as="span" className={styles.rowName}>
                {entry.label}
              </Typography.BodySmall>
              <ContextImpact bytes={entry.bytes} />
            </li>
          ))}
          {others.length > 0 && (
            <li className={styles.row}>
              <OthersName count={others.length} />
              <ContextImpact
                bytes={others.reduce((sum, entry) => sum + BigInt(entry.bytes), 0n).toString()}
              />
            </li>
          )}
        </ul>
      )}
    </Panel>
  )
}

/** How people decided approval requests, and how long calls waited for them. */
export function ApprovalSummary({ approvals }: { approvals: ApprovalDecisions }) {
  const decided = approvals.approved + approvals.declined
  return (
    <Panel title="Approvals">
      {decided === 0 ? (
        <Empty>No approval decisions.</Empty>
      ) : (
        <dl className={styles.stats}>
          <Stat icon="CircleCheck" label="Approved" value={number(approvals.approved)} />
          <Stat
            icon={outcomes.notAllowed.icon}
            label="Declined"
            value={number(approvals.declined)}
          />
          <Stat
            icon="Clock"
            label="Median wait"
            value={
              approvals.medianWaitSeconds === null ? '—' : duration(approvals.medianWaitSeconds)
            }
          />
        </dl>
      )}
    </Panel>
  )
}

function Stat({ icon, label, value }: { icon: IconName; label: string; value: string }) {
  return (
    <div className={styles.stat}>
      <dt className={styles.tileLabel}>
        <Icon name={icon} size="14" color="tertiary" />
        <Typography.BodySmall as="span" variant="tertiary">
          {label}
        </Typography.BodySmall>
      </dt>
      <Typography.HeadingSmall as="dd" className={styles.statValue}>
        {value}
      </Typography.HeadingSmall>
    </div>
  )
}

function duration(seconds: number) {
  if (seconds < 60) return `${Math.round(seconds)} s`
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`
  if (seconds < 86_400) return `${Number((seconds / 3600).toPrecision(2))} h`
  return `${Number((seconds / 86_400).toPrecision(2))} d`
}

export interface ActivitySummaryProps extends ActivitySummaryData {
  days: SummaryDays
  /** Shows the period picker. Without it, the period is fixed. */
  onDaysChange?: (days: SummaryDays) => void
  /** The numbers still cover an earlier period, while `days`' numbers load. */
  pending?: boolean
}

/** The window's numbers in one strip, with the call outcomes and breakdowns under it. */
export function ActivitySummary({
  days,
  onDaysChange,
  pending = false,
  calls,
  stopped,
  operations,
  approvals,
  contextImpactBytes,
  contextImpactPartial,
  contextActors = [],
}: ActivitySummaryProps) {
  return (
    <section
      className={pending ? `${styles.root} ${styles.pending}` : styles.root}
      aria-label="Activity summary"
      aria-busy={pending}
    >
      <div className={styles.heading}>
        <Typography.BodySmallStrong as="h2" variant="primary">
          Your activity in this workspace
        </Typography.BodySmallStrong>
        {onDaysChange ? (
          <SegmentedControl
            label="Period"
            size="small"
            options={dayOptions}
            value={String(days)}
            onChange={(value) => onDaysChange(Number(value) as SummaryDays)}
          />
        ) : (
          <Typography.BodySmall variant="tertiary">{windowLabel(days)}</Typography.BodySmall>
        )}
      </div>
      <div className={styles.strip}>
        <CallsTile calls={calls} />
        <StoppedTile calls={calls} />
        <ContextSavedTile bytes={contextImpactBytes} partial={contextImpactPartial} />
      </div>
      <Card.Container>
        <CallOutcomeChart calls={calls} />
      </Card.Container>
      <div className={styles.panels}>
        <TopOperations operations={operations} />
        <StoppedOperations operations={stopped} />
        <ApprovalSummary approvals={approvals} />
        {contextActors.length > 0 && (
          <ContextBreakdown title="Context by caller" entries={contextActors} />
        )}
      </div>
    </section>
  )
}

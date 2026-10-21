import type { ReactNode } from 'react'
import { Card, Typography } from '@/wax/components'
import { Meter } from '@/wax/components/charts'
import { Icon, type IconName } from '@/wax/components/icon'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { Stat, type StatProps } from '@/wax/components/stat'
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

/** `spoken` follows a count; `spokenOne` replaces it after a count of one. */
const outcomes: Record<
  CallOutcome,
  { label: string; icon: IconName; spoken: string; spokenOne?: string }
> = {
  answered: { label: 'Answered', icon: 'CircleCheck', spoken: 'answered' },
  blocked: { label: 'Blocked by a rule', icon: 'ShieldBan', spoken: 'blocked' },
  notAllowed: { label: 'Not allowed', icon: 'UserRoundX', spoken: 'not allowed' },
  needsSetup: {
    label: 'Needs setup',
    icon: 'Wrench',
    spoken: 'need setup',
    spokenOne: 'needs setup',
  },
  failed: { label: 'Failed', icon: 'CircleX', spoken: 'failed' },
  canceled: { label: 'Canceled', icon: 'CircleSlash', spoken: 'canceled' },
}
const outcomeOrder: readonly CallOutcome[] = [
  'answered',
  'blocked',
  'notAllowed',
  'needsSetup',
  'failed',
  'canceled',
]
const stopReasons: readonly StopReason[] = ['blocked', 'notAllowed', 'needsSetup']
/** Rows each panel lists before the rest add up to one "others" row. */
const OPERATION_ROWS = 5
const CONTEXT_ROWS = 3

const sum = <T,>(entries: readonly T[], value: (entry: T) => number) =>
  entries.reduce((total, entry) => total + value(entry), 0)
const callTotal = (calls: CallOutcomes) => sum(outcomeOrder, (outcome) => calls[outcome])
const stoppedTotal = (counts: Record<StopReason, number>) =>
  sum(stopReasons, (reason) => counts[reason])
const number = (value: number) => value.toLocaleString('en-US')
const plural = (value: number, noun: string) => `${number(value)} ${noun}${value === 1 ? '' : 's'}`
const percent = (part: number, whole: number) =>
  whole === 0 ? '0%' : `${Math.round((part / whole) * 100)}%`
const counted = (outcome: CallOutcome, value: number) => {
  const { spoken, spokenOne = spoken } = outcomes[outcome]
  return `${number(value)} ${value === 1 ? spokenOne : spoken}`
}
const periodName = (days: SummaryDays) => (days === 1 ? '24 hours' : `${days} days`)
const dayOptions = SUMMARY_DAYS.map((days) => ({ value: String(days), label: periodName(days) }))

/**
 * Rows past the limit add up to one "others" row. Hiding a single row saves
 * no space, so a list one over the limit shows whole.
 */
function withOthers<T>(entries: readonly T[], limit: number) {
  const shown = entries.length > limit + 1 ? entries.slice(0, limit) : entries
  return { shown, others: entries.slice(shown.length) }
}

function Tile(props: StatProps) {
  return (
    <Card.Container>
      <Stat {...props} />
    </Card.Container>
  )
}

/** Every ended call in the window, with the share the provider answered. */
export function CallsTile({ calls }: { calls: CallOutcomes }) {
  const total = callTotal(calls)
  return (
    <Tile
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
    <Tile
      label="Stopped"
      icon="ShieldBan"
      value={number(total)}
      detail={
        total > 0
          ? stopReasons
              .filter((reason) => calls[reason] > 0)
              .map((reason) => counted(reason, calls[reason]))
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
  const impact = bytes === undefined ? undefined : describeContextImpact(BigInt(bytes), partial)
  if (!impact)
    return (
      <Tile
        label="Context"
        icon="Layers"
        value="—"
        valueLabel={partial ? 'Context impact incomplete' : 'No context impact'}
        variant="tertiary"
        detail={partial ? 'Incomplete' : 'No runs'}
      />
    )
  return (
    <Tile
      label="Context"
      icon="Layers"
      value={impact.label}
      valueLabel={impact.meaning}
      variant={impact.variant}
      detail={partial ? `${impact.change} · Incomplete` : impact.change}
    />
  )
}

/** A name, its count, and an optional meter under both. */
function Row({ name, count, meter }: { name: ReactNode; count: ReactNode; meter?: ReactNode }) {
  return (
    <li className={styles.row}>
      {name}
      <Typography.BodySmall as="span" variant="tertiary" className={styles.count}>
        {count}
      </Typography.BodySmall>
      {meter}
    </li>
  )
}

function RowMeter({ value, max }: { value: number; max: number }) {
  return <Meter value={value} max={max} size="small" className={styles.rowMeter} />
}

/** One bar per call outcome, each against the window's total calls. */
export function CallOutcomeChart({ calls }: { calls: CallOutcomes }) {
  const total = callTotal(calls)
  return (
    <ul className={styles.rows} aria-label="Call outcomes">
      {outcomeOrder.map((outcome) => (
        <Row
          key={outcome}
          name={<Labeled icon={outcomes[outcome].icon}>{outcomes[outcome].label}</Labeled>}
          count={`${number(calls[outcome])} · ${percent(calls[outcome], total)}`}
          meter={<RowMeter value={calls[outcome]} max={total} />}
        />
      ))}
    </ul>
  )
}

function Labeled({ icon, children }: { icon: IconName; children: string }) {
  return (
    <span className={styles.labeled}>
      <Icon name={icon} size="14" color="tertiary" />
      <Typography.BodySmall as="span" className={styles.rowName}>
        {children}
      </Typography.BodySmall>
    </span>
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

function Note({ children }: { children: string }) {
  return (
    <Typography.BodySmall as="p" variant="tertiary">
      {children}
    </Typography.BodySmall>
  )
}

/** The operations with the most stopped calls, most first, each split by who must act. */
export function StoppedOperations({ operations }: { operations: readonly StoppedOperation[] }) {
  const { shown, others } = withOthers(operations, OPERATION_ROWS)
  const othersCounts = (reason: StopReason) => sum(others, (op) => op[reason])
  return (
    <Panel title="Stopped operations">
      {shown.length === 0 ? (
        <Note>No stopped calls.</Note>
      ) : (
        <ul className={styles.rows}>
          {shown.map((op) => (
            <Row
              key={op.callPath}
              name={<CallPath path={op.callPath} />}
              count={<StopSplit counts={op} />}
            />
          ))}
          {others.length > 0 && (
            <Row
              name={<OthersName count={others.length} />}
              count={
                <StopSplit
                  counts={{
                    blocked: othersCounts('blocked'),
                    notAllowed: othersCounts('notAllowed'),
                    needsSetup: othersCounts('needsSetup'),
                  }}
                />
              }
            />
          )}
        </ul>
      )}
    </Panel>
  )
}

function StopSplit({ counts }: { counts: Record<StopReason, number> }) {
  return (
    <span className={styles.split}>
      {stopReasons
        .filter((reason) => counts[reason] > 0)
        .map((reason) => (
          <span key={reason} className={styles.labeled}>
            <Icon name={outcomes[reason].icon} size="14" color="tertiary" />
            <span aria-hidden>{number(counts[reason])}</span>
            <span className={visuallyHidden}>{counted(reason, counts[reason])}</span>
          </span>
        ))}
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
export function TopOperations({ operations }: { operations: readonly OperationCalls[] }) {
  const { shown, others } = withOthers(operations, OPERATION_ROWS)
  const max = shown[0]?.calls ?? 0
  return (
    <Panel title="Most called operations">
      {shown.length === 0 ? (
        <Note>No calls.</Note>
      ) : (
        <ul className={styles.rows}>
          {shown.map((op) => (
            <Row
              key={op.callPath}
              name={<CallPath path={op.callPath} />}
              count={plural(op.calls, 'call')}
              meter={<RowMeter value={op.calls} max={max} />}
            />
          ))}
          {/* The others share no one bar, so their row shows only the sum. */}
          {others.length > 0 && (
            <Row
              name={<OthersName count={others.length} />}
              count={plural(
                sum(others, (op) => op.calls),
                'call',
              )}
            />
          )}
        </ul>
      )}
    </Panel>
  )
}

/** Net context bytes per caller, most saved first. A partial total leaves out running and
 * unmeasured runs, so the panel says so under its rows.
 */
export function ContextBreakdown({
  entries,
  partial = false,
}: {
  entries: readonly ContextEntry[]
  partial?: boolean
}) {
  const { shown, others } = withOthers(entries, CONTEXT_ROWS)
  return (
    <Panel title="Context by caller">
      {shown.length === 0 ? (
        <Note>No measured runs.</Note>
      ) : (
        <ul className={styles.rows}>
          {shown.map((entry) => (
            <Row
              key={entry.key}
              name={
                <Typography.BodySmall as="span" className={styles.rowName}>
                  {entry.label}
                </Typography.BodySmall>
              }
              count={<ContextImpact bytes={entry.bytes} />}
            />
          ))}
          {others.length > 0 && (
            <Row
              name={<OthersName count={others.length} />}
              count={
                <ContextImpact
                  bytes={others
                    .reduce((total, entry) => total + BigInt(entry.bytes), 0n)
                    .toString()}
                />
              }
            />
          )}
        </ul>
      )}
      {partial && <Note>Incomplete. Running and unmeasured runs are not counted.</Note>}
    </Panel>
  )
}

/** How people decided approval requests, and how long calls waited for them. */
export function ApprovalSummary({ approvals }: { approvals: ApprovalDecisions }) {
  const decided = approvals.approved + approvals.declined
  return (
    <Panel title="Approvals">
      {decided === 0 ? (
        <Note>No approval decisions.</Note>
      ) : (
        <div className={styles.stats}>
          <Stat
            size="small"
            icon="CircleCheck"
            label="Approved"
            value={number(approvals.approved)}
          />
          <Stat
            size="small"
            icon="UserRoundX"
            label="Declined"
            value={number(approvals.declined)}
          />
          <Stat
            size="small"
            icon="Clock"
            label="Median wait"
            value={
              approvals.medianWaitSeconds === null ? '—' : duration(approvals.medianWaitSeconds)
            }
          />
        </div>
      )}
    </Panel>
  )
}

/** Each unit is chosen from the rounded value, so 59.6 s reads "1 min", not "60 s". */
function duration(seconds: number) {
  const s = Math.round(seconds)
  if (s < 60) return `${s} s`
  const min = Math.round(seconds / 60)
  if (min < 60) return `${min} min`
  const h = Number((seconds / 3600).toPrecision(2))
  if (h < 24) return `${h} h`
  return `${Number((seconds / 86_400).toPrecision(2))} d`
}

export interface ActivitySummaryProps {
  /** Null when the summary is not available. */
  summary: ActivitySummaryData | null
  days: SummaryDays
  /** The totals include your agents' runs, as they do in a workspace you own. */
  owner?: boolean
  /** Shows the period picker. Without it, the period is fixed. */
  onDaysChange?: (days: SummaryDays) => void
  /** The numbers still cover an earlier period, while `days`' numbers load. */
  pending?: boolean
}

/** The window's numbers in one strip, with the call outcomes and breakdowns under it. */
export function ActivitySummary({
  summary,
  days,
  owner = false,
  onDaysChange,
  pending = false,
}: ActivitySummaryProps) {
  return (
    <section
      className={pending ? `${styles.root} ${styles.pending}` : styles.root}
      aria-label="Activity summary"
      aria-busy={pending}
    >
      <div className={styles.heading}>
        <Typography.BodySmallStrong as="h2" variant="primary">
          {owner ? 'Activity by you and your agents' : 'Your activity'}
        </Typography.BodySmallStrong>
        {pending && (
          <Typography.BodySmall as="span" variant="tertiary" role="status">
            Loading…
          </Typography.BodySmall>
        )}
        {onDaysChange ? (
          <SegmentedControl
            label="Period"
            size="small"
            options={dayOptions}
            value={String(days)}
            onChange={(value) => onDaysChange(Number(value) as SummaryDays)}
          />
        ) : (
          <Typography.BodySmall variant="tertiary">{`Last ${periodName(days)}`}</Typography.BodySmall>
        )}
      </div>
      {summary ? (
        <>
          <div className={styles.strip}>
            <CallsTile calls={summary.calls} />
            <StoppedTile calls={summary.calls} />
            <ContextSavedTile
              bytes={summary.contextImpactBytes}
              partial={summary.contextImpactPartial}
            />
          </div>
          <Card.Container>
            <CallOutcomeChart calls={summary.calls} />
          </Card.Container>
          <div className={styles.panels}>
            <TopOperations operations={summary.operations} />
            <StoppedOperations operations={summary.stopped} />
            <ApprovalSummary approvals={summary.approvals} />
            <ContextBreakdown
              entries={summary.contextActors ?? []}
              partial={summary.contextImpactPartial}
            />
          </div>
        </>
      ) : (
        <Card.Container>
          <Note>The summary is not available. Try again later.</Note>
        </Card.Container>
      )}
    </section>
  )
}

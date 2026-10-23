import { useNavigate, useSearchParams } from 'react-router'
import { Card, Tabs, Typography } from '@/wax/components'
import { SegmentedControl } from '@/wax/components/segmented-control'
import {
  SUMMARY_DAYS,
  type ActivitySummaryData,
  type CallOutcomes,
  type SummaryDays,
} from '@/lib/activity-summary'
import { describeContextImpact } from '../context-impact'
import { CallsLens } from './calls-lens'
import { ContextLens } from './context-lens'
import { duration, joined, number, share } from './format'
import { NeedsYouLens } from './needs-you-lens'
import { STOP_REASONS, type SummaryLinks } from './operations'
import { Note } from './parts'
import * as styles from './summary.css'

export type { SummaryLinks } from './operations'

const periodName = (days: SummaryDays) => (days === 1 ? '24 hours' : `${days} days`)
const dayOptions = SUMMARY_DAYS.map((days) => ({ value: String(days), label: periodName(days) }))

const LENSES = ['calls', 'needs', 'context'] as const
type Lens = (typeof LENSES)[number]
/** Calls opens with no parameter, so the Overview's own URL stays plain. */
const DEFAULT_LENS: Lens = 'calls'

/** The lens the URL names, so a reload or a shared link opens the same one. */
function useLens() {
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const lens = LENSES.find((option) => option === params.get('lens')) ?? DEFAULT_LENS
  const setLens = (next: Lens) => {
    const query = new URLSearchParams(params)
    if (next === DEFAULT_LENS) query.delete('lens')
    else query.set('lens', next)
    const text = query.toString()
    void navigate({ search: text ? `?${text}` : '' }, { replace: true, preventScrollReset: true })
  }
  return [lens, setLens] as const
}

const stoppedTotal = (calls: CallOutcomes) =>
  STOP_REASONS.reduce((total, reason) => total + calls[reason], 0)

/** The three numbers that head the summary, one per lens. */
function metrics(summary: ActivitySummaryData) {
  const { calls, approvals } = summary
  const total = Object.values(calls).reduce((sum, value) => sum + value, 0)
  const stopped = stoppedTotal(calls)
  const decided = approvals.approved + approvals.declined
  const impact =
    summary.contextImpactBytes === undefined
      ? undefined
      : describeContextImpact(BigInt(summary.contextImpactBytes), !!summary.contextImpactPartial)
  return [
    {
      value: 'calls',
      icon: 'Activity',
      label: 'Calls',
      metric: number(total),
      details: [total ? `${share(calls.answered, total)} answered` : 'No calls'],
    },
    {
      value: 'needs',
      icon: 'Hand',
      label: 'Needs you',
      metric: stopped ? `${number(stopped)} stopped` : 'Nothing',
      details: [
        joined([
          calls.blocked > 0 && `${number(calls.blocked)} blocked`,
          calls.needsSetup > 0 &&
            `${number(calls.needsSetup)} ${calls.needsSetup === 1 ? 'needs' : 'need'} setup`,
          calls.notAllowed > 0 && `${number(calls.notAllowed)} not allowed`,
        ]) || 'No stopped calls',
        decided
          ? joined([
              `${number(approvals.approved)} approved`,
              `${number(approvals.declined)} declined`,
              approvals.medianWaitSeconds !== null &&
                `${duration(approvals.medianWaitSeconds)} median wait`,
            ])
          : 'No approval decisions',
      ],
    },
    {
      value: 'context',
      icon: 'Layers',
      label: 'Context',
      metric: impact?.label ?? '—',
      details: [impact ? `${impact.change} · by caller` : 'No runs'],
      variant: impact?.variant,
    },
  ] satisfies (Tabs.MetricTabProps & { value: Lens })[]
}

export interface ActivitySummaryProps {
  /** Null when the summary is not available. */
  summary: ActivitySummaryData | null
  days: SummaryDays
  /** The pages its rows open. Without them, rows show but open nothing. */
  links?: SummaryLinks
  /** The totals include your agents' runs, as they do in a workspace you own. */
  owner?: boolean
  /** Shows the period picker. Without it, the period is fixed. */
  onDaysChange?: (days: SummaryDays) => void
  /** The numbers still cover an earlier period, while `days`' numbers load. */
  pending?: boolean
}

/**
 * The period's activity as three metrics: calls, what needs you, and context.
 * Each metric is a tab that opens the breakdown behind it.
 */
export function ActivitySummary({
  summary,
  days,
  links,
  owner = false,
  onDaysChange,
  pending = false,
}: ActivitySummaryProps) {
  const [lens, setLens] = useLens()
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
        <Card.Container className={styles.card}>
          <Tabs.Root value={lens} onValueChange={(value) => setLens(value as Lens)}>
            <Tabs.MetricList aria-label="Summary">
              {metrics(summary).map((metric) => (
                <Tabs.MetricTab key={metric.value} {...metric} />
              ))}
            </Tabs.MetricList>
            <Tabs.Panel value="calls" className={styles.panel}>
              <CallsLens summary={summary} links={links} onStopped={() => setLens('needs')} />
            </Tabs.Panel>
            <Tabs.Panel value="needs" className={styles.panel}>
              <NeedsYouLens summary={summary} links={links} />
            </Tabs.Panel>
            <Tabs.Panel value="context" className={styles.panel}>
              <ContextLens entries={summary.contextActors ?? []} />
            </Tabs.Panel>
          </Tabs.Root>
        </Card.Container>
      ) : (
        <Card.Container>
          <Note>The summary is not available. Try again later.</Note>
        </Card.Container>
      )}
    </section>
  )
}

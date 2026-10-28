import { useNavigate, useSearchParams } from 'react-router'
import { Card, Tabs, Typography } from '@/wax/components'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { SUMMARY_DAYS, type ActivitySummaryData, type SummaryDays } from '@/lib/activity-summary'
import { describeContextImpact } from '../context-impact'
import { CallsLens } from './calls-lens'
import { ContextLens, callers } from './context-lens'
import { isYou, joined, number, share } from './format'
import { NeedsYouLens } from './needs-you-lens'
import type { SummaryProvider } from './tools'
import { Note } from './parts'
import * as styles from './summary.css'

export type { SummaryProvider } from './tools'

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

/** Calls your agents made. Only an owner's calls include agents'. */
function agentCalls(summary: ActivitySummaryData, owner: boolean) {
  if (!owner) return 0
  return summary.callers
    .filter((caller) => !isYou(caller))
    .reduce((sum, caller) => sum + caller.calls, 0)
}

/** The three numbers that head the summary, one per lens. */
function metrics(summary: ActivitySummaryData, byAgents: number) {
  const { calls } = summary
  const total = Object.values(calls).reduce((sum, value) => sum + value, 0)
  // Without agents' calls, your part would repeat the total.
  const parts =
    byAgents > 0
      ? [
          { metric: number(total - byAgents), details: ['You'] },
          { metric: number(byAgents), details: ['Your agents'] },
        ]
      : undefined
  // A call a person declined was decided, so only the stops you fix count.
  const toFix = calls.blocked + calls.needsSetup
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
      parts,
    },
    {
      value: 'needs',
      icon: 'Hand',
      label: 'Needs you',
      metric: toFix ? `${number(toFix)} to fix` : 'Nothing',
      details: [
        joined([
          calls.blocked > 0 && `${number(calls.blocked)} blocked`,
          calls.needsSetup > 0 &&
            `${number(calls.needsSetup)} ${calls.needsSetup === 1 ? 'needs' : 'need'} setup`,
        ]) || 'No calls to fix',
      ],
    },
    {
      value: 'context',
      icon: 'Layers',
      label: 'Context',
      metric: impact?.label ?? '—',
      details: [impact ? joined([impact.change, callers(summary.contextActors ?? [])]) : 'No runs'],
      variant: impact?.variant,
    },
  ] satisfies (Tabs.MetricTabProps & { value: Lens })[]
}

export interface ActivitySummaryProps {
  /** Null when the summary is not available. */
  summary: ActivitySummaryData | null
  days: SummaryDays
  /** The workspace's providers, which name the rows and open their pages. Without them, rows open nothing. */
  providers?: readonly SummaryProvider[]
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
  providers,
  owner = false,
  onDaysChange,
  pending = false,
}: ActivitySummaryProps) {
  const [lens, setLens] = useLens()
  const byAgents = summary ? agentCalls(summary, owner) : 0
  return (
    <section
      className={pending ? `${styles.root} ${styles.pending}` : styles.root}
      aria-label="Activity summary"
      aria-busy={pending}
    >
      <div className={styles.heading}>
        <Typography.BodySmallStrong as="h2" variant="primary">
          {owner ? 'Workspace activity' : 'Your activity'}
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
        <Card.Container flush>
          <Tabs.Root value={lens} onValueChange={(value) => setLens(value as Lens)}>
            <Tabs.MetricList aria-label="Summary">
              {metrics(summary, byAgents).map((metric) => (
                <Tabs.MetricTab key={metric.value} {...metric} />
              ))}
            </Tabs.MetricList>
            {/* Every lens stays mounted in one grid cell, so the card keeps the tallest lens's
                height and switching lenses never shortens the page under you. */}
            <div className={styles.panels}>
              <Tabs.Panel value="calls" keepMounted className={styles.panel}>
                <CallsLens summary={summary} providers={providers} showCallers={byAgents > 0} />
              </Tabs.Panel>
              <Tabs.Panel value="needs" keepMounted className={styles.panel}>
                <NeedsYouLens summary={summary} providers={providers} />
              </Tabs.Panel>
              <Tabs.Panel value="context" keepMounted className={styles.panel}>
                <ContextLens entries={summary.contextActors ?? []} />
              </Tabs.Panel>
            </div>
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

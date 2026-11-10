import { useNavigate, useSearchParams } from 'react-router'
import { Card, Tabs, Typography } from '@/wax/components'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { Skeleton, SkeletonText } from '@/wax/components/skeleton'
import { Meter } from '@/wax/components/charts'
import * as Table from '@/wax/components/table'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { SUMMARY_DAYS, type ActivitySummaryData, type SummaryDays } from '@/lib/activity-summary'
import { describeContextImpact } from '../context-impact'
import { CallsLens } from './calls-lens'
import { ContextLens, callers } from './context-lens'
import { isYou, joined, number, share } from './format'
import { NeedsYouLens } from './needs-you-lens'
import type { SummaryProvider } from './tools'
import { Note } from './parts'
import { OUTCOME_PARTS, TOOL_COLUMNS } from './calls-lens'
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
  /** Null when the summary is not available. Undefined while the first summary loads. */
  summary: ActivitySummaryData | null | undefined
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
  const loading = summary === undefined
  return (
    <section
      className={pending && !loading ? `${styles.root} ${styles.pending}` : styles.root}
      aria-label="Activity summary"
      aria-busy={pending || loading}
    >
      <div className={styles.heading}>
        <Typography.BodySmallStrong as="h2" variant="primary">
          {owner ? 'Workspace activity' : 'Your activity'}
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
          <Typography.BodySmall variant="tertiary">{`Last ${periodName(days)}`}</Typography.BodySmall>
        )}
      </div>
      {loading ? (
        <LoadingSummary />
      ) : summary ? (
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

/** Rows the tools table shows while the summary loads. */
const LOADING_ROWS = 3

/**
 * A placeholder that approximates the Calls lens: the three metrics, the outcome cards, the
 * tools filter and a tools table of `LOADING_ROWS` rows. The summary that arrives can still
 * change the card's height, with its own number of tools and a taller lens. Assistive
 * technology hears only the status.
 */
function LoadingSummary() {
  return (
    <Card.Container flush>
      <span role="status" className={visuallyHidden}>
        Loading activity…
      </span>
      <div aria-hidden>
        <Tabs.MetricListSkeleton count={LENSES.length} />
        <div className={`${styles.panel} ${styles.lens}`}>
          <div className={styles.statCards}>
            {OUTCOME_PARTS.map((part) => (
              <Card.Container key={part.key} compact>
                <div className={styles.loadingStat}>
                  <SkeletonText variant="bodySmall" width="72px" />
                  <SkeletonText variant="headingSmall" width="56px" />
                </div>
              </Card.Container>
            ))}
          </div>
          <div className={styles.tools}>
            <SkeletonText variant="bodySmall" width="40px" />
            <div className={`${styles.filter} ${styles.loadingFilter}`}>
              <Skeleton height="100%" width="100%" borderRadius="8px" />
            </div>
          </div>
          <Table.Container variant="card" layout="fixed" columns={TOOL_COLUMNS}>
            <Table.Head>
              <Table.Row>
                {TOOL_COLUMNS.map((column, index) => (
                  <Table.Heading key={index} align={column.align}>
                    {column.label}
                  </Table.Heading>
                ))}
              </Table.Row>
            </Table.Head>
            <Table.Body>
              {Array.from({ length: LOADING_ROWS }, (_, index) => (
                <Table.Row key={index}>
                  <Table.Cell>
                    <span className={styles.tool}>
                      <Skeleton
                        height="20px"
                        width="20px"
                        borderRadius="4px"
                        style={{ flexShrink: 0 }}
                      />
                      <SkeletonText variant="bodySmall" width="160px" />
                    </span>
                  </Table.Cell>
                  <Table.Cell>
                    <span className={styles.barCell}>
                      <SkeletonText variant="bodySmall" width="24px" />
                      <Meter value={0} max={1} size="small" />
                    </span>
                  </Table.Cell>
                  <Table.Cell>
                    <SkeletonText variant="bodySmall" width="32px" />
                  </Table.Cell>
                  <Table.Cell>
                    <SkeletonText variant="bodySmall" width="12px" />
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        </div>
      </div>
    </Card.Container>
  )
}

import { useDeferredValue, useState } from 'react'
import { Typography } from '@/wax/components'
import * as Table from '@/wax/components/table'
import { FilterField } from '@/components/catalog'
import type { ActivitySummaryData } from '@/lib/activity-summary'
import { filterQuery } from '@/utils/filter-query'
import { number, outcomes, share } from './format'
import { STOP_REASONS, toolRows, type SummaryProvider } from './tools'
import { BarCell, None, ToolName, StatCards, StopSplit } from './parts'
import * as styles from './summary.css'

/** Rows the table shows before the rest scroll. */
const MAX_ROWS = 8
const FILTER_ID = 'summary-tools-filter'

// The three kinds of stop read as one part here.
const parts = [
  { key: 'answered', ...outcomes.answered },
  { key: 'stopped', label: 'Stopped', icon: 'Hand' },
  { key: 'failed', ...outcomes.failed },
  { key: 'canceled', ...outcomes.canceled },
] as const

/** Every ended call by outcome, each with its share. */
function Split({ summary }: { summary: ActivitySummaryData }) {
  const { calls } = summary
  const counts = {
    answered: calls.answered,
    stopped: STOP_REASONS.reduce((total, reason) => total + calls[reason], 0),
    failed: calls.failed,
    canceled: calls.canceled,
  }
  const total = counts.answered + counts.stopped + counts.failed + counts.canceled
  return (
    <StatCards
      stats={parts.map((part) => ({
        icon: part.icon,
        label: part.label,
        value: number(counts[part.key]),
        detail: share(counts[part.key], total),
      }))}
    />
  )
}

/** Is it working? The split of every call, then each tool, busiest first, to filter. */
export function CallsLens({
  summary,
  providers,
}: {
  summary: ActivitySummaryData
  providers?: readonly SummaryProvider[]
}) {
  const [input, setInput] = useState('')
  const query = filterQuery(useDeferredValue(input))
  const table = Table.useRowCursor({ filterId: FILTER_ID })
  const all = toolRows(summary, providers).toSorted((a, b) => b.calls - a.calls)
  const total = all.reduce((sum, row) => sum + row.calls, 0)
  const max = Math.max(0, ...all.map((row) => row.calls))
  const rows = all.filter((row) => query.matches(row.namespace, row.tool, row.provider))
  return (
    <div className={styles.lens}>
      <Split summary={summary} />
      <div className={styles.tools}>
        <Typography.BodySmall as="span" variant="tertiary" className={styles.shown}>
          {rows.length === all.length
            ? `${number(all.length)} ${all.length === 1 ? 'tool' : 'tools'}`
            : `${number(rows.length)} of ${number(all.length)}`}
        </Typography.BodySmall>
        <FilterField
          id={FILTER_ID}
          label="Filter tools"
          value={input}
          onChange={setInput}
          className={styles.filter}
        />
      </div>
      <Table.Container
        ref={table}
        ariaLabel="Tools"
        variant="card"
        layout="fixed"
        maxRows={MAX_ROWS}
        columns={[
          { label: 'Tool', width: { fill: 3 } },
          { label: 'Calls', width: { fill: 1.6 } },
          { label: 'Share', width: 64, align: 'right' },
          { label: 'Stopped', width: { fill: 1 }, align: 'right' },
        ]}
      >
        <Table.Head>
          <Table.Row>
            <Table.Heading>Tool</Table.Heading>
            <Table.Heading>Calls</Table.Heading>
            <Table.Heading align="right">Share</Table.Heading>
            <Table.Heading align="right">Stopped</Table.Heading>
          </Table.Row>
        </Table.Head>
        {rows.length === 0 ? (
          <Table.Status>
            {query.text ? `No tools match “${query.text}”.` : 'No calls.'}
          </Table.Status>
        ) : (
          <Table.Body>
            {rows.map((row) => {
              const to = row.pages?.tool
              return (
                <Table.Row key={row.callPath} to={to}>
                  <Table.Cell title={row.callPath}>
                    <ToolName row={row} to={to} query={query} />
                  </Table.Cell>
                  <Table.Cell>
                    <BarCell value={row.calls} max={max} />
                  </Table.Cell>
                  <Table.Cell>
                    <Typography.BodySmall as="span" variant="tertiary" className={styles.count}>
                      {share(row.calls, total)}
                    </Typography.BodySmall>
                  </Table.Cell>
                  <Table.Cell>
                    {row.stopped ? <StopSplit counts={row.stops} /> : <None />}
                  </Table.Cell>
                </Table.Row>
              )
            })}
          </Table.Body>
        )}
      </Table.Container>
    </div>
  )
}

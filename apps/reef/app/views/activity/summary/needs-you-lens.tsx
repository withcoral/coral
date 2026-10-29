import { Typography } from '@/wax/components'
import * as Table from '@/wax/components/table'
import type { ActivitySummaryData } from '@/lib/activity-summary'
import { duration, number } from './format'
import { fixFor, toolRows, type SummaryProvider } from './tools'
import { Note, ToolName, Reason, Section, StatCards } from './parts'
import * as styles from './summary.css'

/** What needs you: the stops you fix, what people decided, and the calls they did not allow. */
export function NeedsYouLens({
  summary,
  providers,
}: {
  summary: ActivitySummaryData
  providers?: readonly SummaryProvider[]
}) {
  const { approvals } = summary
  // One cursor runs through both tables, so ↑ and ↓ read them as one list.
  const cursor = Table.useRowCursor()
  const rows = toolRows(summary, providers)
  const toFix = rows
    .flatMap((row) =>
      (['blocked', 'needsSetup'] as const)
        .filter((reason) => row.stops[reason] > 0)
        .map((reason) => ({ row, reason, count: row.stops[reason] })),
    )
    .toSorted((a, b) => b.count - a.count)
  const notAllowed = rows
    .filter((row) => row.stops.notAllowed > 0)
    .toSorted((a, b) => b.stops.notAllowed - a.stops.notAllowed)
  return (
    <div ref={cursor} className={styles.lens}>
      <Section title="To fix" count={toFix.reduce((total, item) => total + item.count, 0)}>
        {toFix.length === 0 ? (
          <Note>Nothing to fix.</Note>
        ) : (
          <Table.Container
            ariaLabel="To fix"
            variant="card"
            layout="fixed"
            columns={[
              { label: 'Tool', width: { fill: 3 } },
              { label: 'Reason', width: { fill: 1.6 } },
              { label: 'Calls', width: 72, align: 'right' },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {toFix.map(({ row, reason, count }) => {
                const fix = fixFor(reason, row.pages)
                return (
                  <Table.Row key={`${row.callPath}/${reason}`} to={fix}>
                    <Table.Cell title={row.callPath}>
                      <ToolName row={row} to={fix} />
                    </Table.Cell>
                    <Table.Cell>
                      <Reason reason={reason} />
                    </Table.Cell>
                    <Table.Cell>
                      <Typography.BodySmall as="span" className={styles.count}>
                        {number(count)}
                      </Typography.BodySmall>
                    </Table.Cell>
                  </Table.Row>
                )
              })}
            </Table.Body>
          </Table.Container>
        )}
      </Section>
      <Section title="Approval decisions" count={approvals.approved + approvals.declined}>
        <StatCards
          stats={[
            { icon: 'CircleCheck', label: 'Approved', value: number(approvals.approved) },
            { icon: 'UserRoundX', label: 'Declined', value: number(approvals.declined) },
            {
              icon: 'Clock',
              label: 'Median wait',
              value:
                approvals.medianWaitSeconds === null ? '—' : duration(approvals.medianWaitSeconds),
            },
          ]}
        />
      </Section>
      {/* Calls, not decisions: an expired approval or a declined connection also lands here. */}
      {notAllowed.length > 0 && (
        <Section title="Calls not allowed" count={summary.calls.notAllowed}>
          <Table.Container
            ariaLabel="Calls not allowed"
            variant="card"
            layout="fixed"
            columns={[
              { label: 'Tool', width: { fill: 3 } },
              { label: 'Not allowed', width: { fill: 1.6 } },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {notAllowed.map((row) => {
                const to = row.pages?.tool
                return (
                  <Table.Row key={row.callPath} to={to}>
                    <Table.Cell title={row.callPath}>
                      <ToolName row={row} to={to} />
                    </Table.Cell>
                    <Table.Cell>
                      <Typography.BodySmall as="span" className={styles.count}>
                        {number(row.stops.notAllowed)}
                      </Typography.BodySmall>
                    </Table.Cell>
                  </Table.Row>
                )
              })}
            </Table.Body>
          </Table.Container>
        </Section>
      )}
    </div>
  )
}

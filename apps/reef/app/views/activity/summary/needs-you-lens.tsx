import { Typography } from '@/wax/components'
import { Stat } from '@/wax/components/stat'
import * as Table from '@/wax/components/table'
import type { ActivitySummaryData } from '@/lib/activity-summary'
import { duration, number, outcomes } from './format'
import { fixFor, operationRows, type SummaryLinks } from './operations'
import { ActionLink, Note, OperationName, Reason, Section } from './parts'
import * as styles from './summary.css'

/** What needs you: the stops you fix, then what people decided. */
export function NeedsYouLens({
  summary,
  links,
}: {
  summary: ActivitySummaryData
  links?: SummaryLinks
}) {
  const { approvals } = summary
  const rows = operationRows(summary, links)
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
    <div className={styles.lens}>
      <Section
        title="To fix"
        count={toFix.reduce((total, item) => total + item.count, 0)}
        note="A rule stopped these calls, or the provider or account needs setup."
      >
        {toFix.length === 0 ? (
          <Note>Nothing to fix.</Note>
        ) : (
          <Table.Container
            ariaLabel="To fix"
            variant="card"
            layout="fixed"
            columns={[
              { label: 'Operation', width: { fill: 3 } },
              { label: 'Reason', width: { fill: 1.6 } },
              { label: 'Calls', width: 72, align: 'right' },
              { ariaLabel: 'Fix', width: 120, align: 'right' },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {toFix.map(({ row, reason, count }) => {
                const fix = fixFor(reason, links?.operation(row.callPath))
                return (
                  <Table.Row key={`${row.callPath}/${reason}`} to={fix?.to}>
                    <Table.Cell title={row.callPath}>
                      <OperationName row={row} />
                    </Table.Cell>
                    <Table.Cell>
                      <Reason reason={reason} />
                    </Table.Cell>
                    <Table.Cell>
                      <Typography.BodySmall as="span" className={styles.count}>
                        {number(count)}
                      </Typography.BodySmall>
                    </Table.Cell>
                    <Table.Cell>
                      {fix && <ActionLink to={fix.to}>{fix.label}</ActionLink>}
                    </Table.Cell>
                  </Table.Row>
                )
              })}
            </Table.Body>
          </Table.Container>
        )}
      </Section>
      <Section
        title="Decided by people"
        count={approvals.approved + approvals.declined}
        note="Approval decisions in this period. A declined or expired request stops its call as not allowed."
      >
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
          <Stat
            size="small"
            icon={outcomes.notAllowed.icon}
            label="Calls not allowed"
            value={number(summary.calls.notAllowed)}
          />
        </div>
        {notAllowed.length > 0 && (
          <Table.Container
            ariaLabel="Calls not allowed"
            variant="card"
            layout="fixed"
            columns={[
              { label: 'Operation', width: { fill: 3 } },
              { label: 'Not allowed', width: { fill: 1.6 } },
              { ariaLabel: 'Open', width: 120, align: 'right' },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {notAllowed.map((row) => {
                const to = links?.operation(row.callPath)?.operation
                return (
                  <Table.Row key={row.callPath} to={to}>
                    <Table.Cell title={row.callPath}>
                      <OperationName row={row} />
                    </Table.Cell>
                    <Table.Cell>
                      <Typography.BodySmall as="span" className={styles.count}>
                        {number(row.stops.notAllowed)}
                      </Typography.BodySmall>
                    </Table.Cell>
                    <Table.Cell>{to && <ActionLink to={to}>Open operation</ActionLink>}</Table.Cell>
                  </Table.Row>
                )
              })}
            </Table.Body>
          </Table.Container>
        )}
      </Section>
    </div>
  )
}

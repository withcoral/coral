import { Typography } from '@/wax/components'
import * as Table from '@/wax/components/table'
import type { ActivitySummaryData, StopReason } from '@/lib/activity-summary'
import { duration, number, outcomes } from './format'
import { fixFor, TOOL_ORDER, toolRows, type SummaryProvider, type ToolRow } from './tools'
import { Note, ToolName, Reason, Section, StatCards } from './parts'
import * as styles from './summary.css'

/** A tool's stops for one reason that you fix. */
interface ToFix {
  row: ToolRow
  reason: Extract<StopReason, 'blocked' | 'needsSetup'>
  count: number
}

const TO_FIX_COLUMNS = {
  tool: { ...TOOL_ORDER, compare: (a: ToFix, b: ToFix) => TOOL_ORDER.compare(a.row, b.row) },
  reason: Table.byText((item: ToFix) => outcomes[item.reason].label),
  calls: Table.byCount((item: ToFix) => item.count),
}
// A tool's two reasons stay in reason order.
const toFixOrder = (a: ToFix, b: ToFix) =>
  TO_FIX_COLUMNS.tool.compare(a, b) || TO_FIX_COLUMNS.reason.compare(a, b)

const NOT_ALLOWED_COLUMNS = {
  tool: TOOL_ORDER,
  calls: Table.byCount((row: ToolRow) => row.stops.notAllowed),
}

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
  // Each table starts with the most calls. Equal counts stay in tool order.
  const toFixSort = Table.useSort({
    columns: TO_FIX_COLUMNS,
    initial: 'calls',
    tiebreak: toFixOrder,
  })
  const notAllowedSort = Table.useSort({
    columns: NOT_ALLOWED_COLUMNS,
    initial: 'calls',
    tiebreak: TOOL_ORDER.compare,
  })
  const rows = toolRows(summary, providers)
  const toFix = toFixSort.rows(
    rows.flatMap((row) =>
      (['blocked', 'needsSetup'] as const)
        .filter((reason) => row.stops[reason] > 0)
        .map((reason) => ({ row, reason, count: row.stops[reason] })),
    ),
  )
  const notAllowed = notAllowedSort.rows(rows.filter((row) => row.stops.notAllowed > 0))
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
              { width: { fill: 3 } },
              { width: { fill: 1.6 } },
              { width: 72, align: 'right' },
            ]}
          >
            <Table.Head>
              <Table.Row>
                <Table.SortHeading sort={toFixSort} column="tool">
                  Tool
                </Table.SortHeading>
                <Table.SortHeading sort={toFixSort} column="reason">
                  Reason
                </Table.SortHeading>
                <Table.SortHeading sort={toFixSort} column="calls">
                  Calls
                </Table.SortHeading>
              </Table.Row>
            </Table.Head>
            <Table.Body>
              {toFix.map(({ row, reason, count }) => {
                const fix = fixFor(reason, row.pages)
                return (
                  <Table.Row key={`${row.tool}/${reason}`} to={fix}>
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
            columns={[{ width: { fill: 3 } }, { width: { fill: 1.6 } }]}
          >
            <Table.Head>
              <Table.Row>
                <Table.SortHeading sort={notAllowedSort} column="tool">
                  Tool
                </Table.SortHeading>
                <Table.SortHeading sort={notAllowedSort} column="calls">
                  Not allowed
                </Table.SortHeading>
              </Table.Row>
            </Table.Head>
            <Table.Body>
              {notAllowed.map((row) => {
                const to = row.pages?.tool
                return (
                  <Table.Row key={row.tool} to={to}>
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

import { useDeferredValue, useState } from 'react'
import classNames from 'classnames'
import { Button, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { Logo } from '@/wax/components/logo'
import { SegmentedControl } from '@/wax/components/segmented-control'
import * as Table from '@/wax/components/table'
import { FilterField } from '@/components/catalog'
import type { ActivitySummaryData } from '@/lib/activity-summary'
import { filterQuery } from '@/utils/filter-query'
import { number, share } from './format'
import {
  STOP_REASONS,
  groupByProvider,
  operationRows,
  type OperationRow,
  type SummaryLinks,
} from './operations'
import { ActionLink, BarCell, None, OperationName, StopSplit } from './parts'
import * as styles from './summary.css'

/** Rows the table shows before the rest scroll. */
const MAX_ROWS = 8
const FILTER_ID = 'summary-operations-filter'

const parts = [
  { key: 'answered', label: 'Answered', icon: 'CircleCheck', fill: styles.fillAnswered },
  { key: 'stopped', label: 'Stopped', icon: 'Hand', fill: styles.fillStopped },
  { key: 'failed', label: 'Failed', icon: 'CircleX', fill: styles.fillFailed },
  { key: 'canceled', label: 'Canceled', icon: 'CircleSlash', fill: styles.fillCanceled },
] as const

/** Every ended call in one bar, then what each part is and where it leads. */
function Split({
  summary,
  links,
  onStopped,
}: {
  summary: ActivitySummaryData
  links?: SummaryLinks
  onStopped: () => void
}) {
  const { calls } = summary
  const counts = {
    answered: calls.answered,
    stopped: STOP_REASONS.reduce((total, reason) => total + calls[reason], 0),
    failed: calls.failed,
    canceled: calls.canceled,
  }
  const total = counts.answered + counts.stopped + counts.failed + counts.canceled
  return (
    <div className={styles.split}>
      <div className={styles.splitBar} aria-hidden>
        {parts
          .filter((part) => counts[part.key] > 0)
          .map((part) => (
            <span
              key={part.key}
              className={classNames(styles.splitSegment, part.fill)}
              style={{ flexGrow: counts[part.key] }}
            />
          ))}
      </div>
      <ul className={styles.legend} aria-label="Calls by outcome">
        {parts.map((part) => (
          <li key={part.key} className={styles.legendItem}>
            <span className={classNames(styles.swatch, part.fill)} />
            <Icon name={part.icon} size="14" color="tertiary" />
            <Typography.BodySmall as="span">{part.label}</Typography.BodySmall>
            <Typography.BodySmall as="span" variant="tertiary" className={styles.count}>
              {`${number(counts[part.key])} · ${share(counts[part.key], total)}`}
            </Typography.BodySmall>
            {part.key === 'stopped' && counts.stopped > 0 && (
              <Button.TextButton variant="link" size="22" onClick={onStopped}>
                See why
              </Button.TextButton>
            )}
            {/* The summary does not split these by operation, so Activity lists them. */}
            {(part.key === 'failed' || part.key === 'canceled') &&
              counts[part.key] > 0 &&
              links && <ActionLink to={links.activity}>Open activity</ActionLink>}
          </li>
        ))}
      </ul>
    </div>
  )
}

type Sort = 'name' | 'calls' | 'stopped'
type Ranked = { calls: number; stopped: number; name: string }

const orderBy = (sort: Sort) => (a: Ranked, b: Ranked) =>
  sort === 'name'
    ? a.name.localeCompare(b.name)
    : sort === 'stopped'
      ? b.stopped - a.stopped || b.calls - a.calls
      : b.calls - a.calls

/** Is it working? The split of every call, then each operation to filter, sort and group. */
export function CallsLens({
  summary,
  links,
  onStopped,
}: {
  summary: ActivitySummaryData
  links?: SummaryLinks
  onStopped: () => void
}) {
  const [input, setInput] = useState('')
  const query = filterQuery(useDeferredValue(input))
  const [sort, setSort] = useState<Sort>('calls')
  const [show, setShow] = useState<'all' | 'stopped'>('all')
  const [grouping, setGrouping] = useState<'operations' | 'providers'>('operations')
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set())
  const table = Table.useRowCursor({ filterId: FILTER_ID })
  const all = operationRows(summary, links)
  const total = all.reduce((sum, row) => sum + row.calls, 0)
  const max = Math.max(0, ...all.map((row) => row.calls))
  const order = orderBy(sort)
  const rows = all
    .filter((row) => show === 'all' || row.stopped > 0)
    .filter((row) => query.matches(row.operation, row.provider))
    .toSorted((a, b) =>
      order(
        { ...a, name: `${a.provider} ${a.operation}` },
        { ...b, name: `${b.provider} ${b.operation}` },
      ),
    )
  const groups = groupByProvider(rows).toSorted((a, b) =>
    order({ ...a, name: a.provider }, { ...b, name: b.provider }),
  )
  const toggle = (key: string) =>
    setCollapsed((current) => {
      const next = new Set(current)
      if (!next.delete(key)) next.add(key)
      return next
    })
  const sortBy = (key: Sort) => ({ active: sort === key, onSort: () => setSort(key) })
  const figures = (item: Pick<OperationRow, 'calls' | 'stops' | 'stopped'>) => (
    <>
      <Table.Cell>
        <BarCell value={item.calls} max={max} />
      </Table.Cell>
      <Table.Cell>
        <Typography.BodySmall as="span" variant="tertiary" className={styles.count}>
          {share(item.calls, total)}
        </Typography.BodySmall>
      </Table.Cell>
      <Table.Cell>{item.stopped ? <StopSplit counts={item.stops} /> : <None />}</Table.Cell>
    </>
  )
  const row = (r: OperationRow, grouped: boolean) => {
    const to = links?.operation(r.callPath)?.operation
    return (
      <Table.Row key={r.callPath} to={to}>
        <Table.Cell title={r.callPath}>
          <OperationName row={r} to={to} query={query} grouped={grouped} />
        </Table.Cell>
        {figures(r)}
      </Table.Row>
    )
  }
  return (
    <div className={styles.lens}>
      <Split summary={summary} links={links} onStopped={onStopped} />
      <div className={styles.tools}>
        <FilterField
          id={FILTER_ID}
          label="Filter operations"
          value={input}
          onChange={setInput}
          className={styles.filter}
        />
        <Typography.BodySmall as="span" variant="tertiary" className={styles.shown}>
          {rows.length === all.length
            ? `${number(all.length)} ${all.length === 1 ? 'operation' : 'operations'}`
            : `${number(rows.length)} of ${number(all.length)}`}
        </Typography.BodySmall>
        <SegmentedControl
          label="Show"
          size="small"
          value={show}
          onChange={setShow}
          options={[
            { value: 'all', label: 'All' },
            { value: 'stopped', label: 'With stops' },
          ]}
        />
        <SegmentedControl
          label="Group"
          size="small"
          value={grouping}
          onChange={setGrouping}
          options={[
            { value: 'operations', label: 'Operations' },
            { value: 'providers', label: 'Providers' },
          ]}
        />
      </div>
      <Table.Container
        ref={table}
        ariaLabel="Operations"
        variant="card"
        layout="fixed"
        maxRows={MAX_ROWS}
        groupHeadings="plain"
        columns={[
          { label: 'Operation', width: { fill: 3 } },
          { label: 'Calls', width: { fill: 1.6 } },
          { label: 'Share', width: 64, align: 'right' },
          { label: 'Stopped', width: { fill: 1 }, align: 'right' },
        ]}
      >
        <Table.Head>
          <Table.Row>
            <Table.Heading sort={sortBy('name')}>Operation</Table.Heading>
            <Table.Heading sort={sortBy('calls')}>Calls</Table.Heading>
            <Table.Heading align="right">Share</Table.Heading>
            <Table.Heading align="right" sort={sortBy('stopped')}>
              Stopped
            </Table.Heading>
          </Table.Row>
        </Table.Head>
        {rows.length === 0 ? (
          <Table.Status>
            {query.text
              ? `No operations match “${query.text}”.`
              : show === 'stopped'
                ? 'No stopped calls.'
                : 'No calls.'}
          </Table.Status>
        ) : (
          <Table.Body>
            {grouping === 'operations'
              ? rows.map((r) => row(r, false))
              : groups.map((group) => {
                  // A filter shows every match, so it opens the groups it matches in.
                  const isCollapsed = collapsed.has(group.key) && !query.text
                  return (
                    <Table.Group
                      key={group.key}
                      label={group.provider}
                      collapsed={isCollapsed}
                      heading={
                        <>
                          <Table.Cell>
                            <span className={styles.groupName}>
                              <Table.GroupToggle
                                collapsed={isCollapsed}
                                label={group.provider}
                                onToggle={() => toggle(group.key)}
                              />
                              <Logo name={group.provider} icons={group.icons} size="20" />
                              <Typography.BodySmallStrong as="span">
                                {group.provider}
                              </Typography.BodySmallStrong>
                              <Typography.BodySmall as="span" variant="tertiary">
                                {number(group.rows.length)}
                              </Typography.BodySmall>
                            </span>
                          </Table.Cell>
                          {figures(group)}
                        </>
                      }
                    >
                      {group.rows.map((r) => row(r, true))}
                    </Table.Group>
                  )
                })}
          </Table.Body>
        )}
      </Table.Container>
    </div>
  )
}

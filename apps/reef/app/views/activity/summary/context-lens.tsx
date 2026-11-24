import { Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import * as Table from '@/wax/components/table'
import type { ContextEntry } from '@/lib/activity-summary'
import { ContextImpact } from '../context-impact'
import { number, share } from './format'
import { BarCell, Note } from './parts'
import * as styles from './summary.css'

// The API names no caller kind yet; only your own row reads "You".
const isYou = (entry: ContextEntry) => entry.label === 'You'

/** Who moved the context: you, your agents, or both. */
export function callers(entries: readonly ContextEntry[]) {
  const count = entries.filter((entry) => !isYou(entry)).length
  const agents = count > 0 && `${number(count)} ${count === 1 ? 'agent' : 'agents'}`
  if (!entries.some(isYou)) return agents
  return agents ? `You and ${agents}` : 'You'
}

const size = (entry: ContextEntry) => Math.abs(Number(entry.bytes))

/** Orders exact byte counts, which can pass what a number holds. */
function compareBytes(bytes: (entry: ContextEntry) => bigint) {
  return (a: ContextEntry, b: ContextEntry) => {
    const [x, y] = [bytes(a), bytes(b)]
    return x < y ? -1 : x > y ? 1 : 0
  }
}
const net = (entry: ContextEntry) => BigInt(entry.bytes)
const moved = (entry: ContextEntry) => (net(entry) < 0n ? -net(entry) : net(entry))

const SORT_COLUMNS = {
  caller: Table.byText((entry: ContextEntry) => entry.label),
  share: { compare: compareBytes(moved), first: 'descending' },
  // The most saved comes first.
  context: { compare: compareBytes(net), first: 'ascending' },
} satisfies Record<string, Table.SortColumn<ContextEntry>>

/** Each caller's net context, with its share of all the context moved. */
export function ContextLens({ entries }: { entries: readonly ContextEntry[] }) {
  // Coral lists the callers that saved the most first. Equal values stay in caller order.
  const sort = Table.useSort({
    columns: SORT_COLUMNS,
    initial: 'context',
    tiebreak: SORT_COLUMNS.caller.compare,
  })
  if (entries.length === 0) return <Note>No measured runs.</Note>
  const sizes = entries.map(size)
  const whole = sizes.reduce((total, value) => total + value, 0)
  const max = Math.max(...sizes)
  return (
    <Table.Container
      ariaLabel="Context by caller"
      variant="card"
      layout="fixed"
      maxRows={8}
      columns={[{ width: { fill: 2 } }, { width: { fill: 2 } }, { width: 120, align: 'right' }]}
    >
      <Table.Head>
        <Table.Row>
          <Table.SortHeading sort={sort} column="caller">
            Caller
          </Table.SortHeading>
          <Table.SortHeading sort={sort} column="share">
            Share
          </Table.SortHeading>
          <Table.SortHeading sort={sort} column="context">
            Context
          </Table.SortHeading>
        </Table.Row>
      </Table.Head>
      <Table.Body>
        {sort.rows(entries).map((entry) => (
          <Table.Row key={entry.key}>
            <Table.Cell title={entry.label}>
              <span className={styles.labeled}>
                <Icon name={isYou(entry) ? 'UserRound' : 'Bot'} size="14" color="tertiary" />
                <Typography.BodySmall as="span" className={styles.truncate}>
                  {entry.label}
                </Typography.BodySmall>
              </span>
            </Table.Cell>
            <Table.Cell>
              <BarCell value={size(entry)} label={share(size(entry), whole)} max={max} />
            </Table.Cell>
            <Table.Cell>
              <ContextImpact bytes={entry.bytes} />
            </Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Container>
  )
}

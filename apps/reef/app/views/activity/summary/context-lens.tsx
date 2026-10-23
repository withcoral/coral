import { Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import * as Table from '@/wax/components/table'
import type { ContextEntry } from '@/lib/activity-summary'
import { ContextImpact } from '../context-impact'
import { share } from './format'
import { BarCell, Note } from './parts'
import * as styles from './summary.css'

/** Each caller's net context, with its share of all the context moved. */
export function ContextLens({ entries }: { entries: readonly ContextEntry[] }) {
  if (entries.length === 0) return <Note>No measured runs.</Note>
  const sizes = entries.map((entry) => Math.abs(Number(entry.bytes)))
  const whole = sizes.reduce((total, size) => total + size, 0)
  const max = Math.max(...sizes)
  return (
    <Table.Container
      ariaLabel="Context by caller"
      variant="card"
      layout="fixed"
      maxRows={8}
      columns={[
        { label: 'Caller', width: { fill: 2 } },
        { label: 'Share', width: { fill: 2 } },
        { label: 'Context', width: 120, align: 'right' },
      ]}
    >
      <Table.Head />
      <Table.Body>
        {entries.map((entry, i) => (
          <Table.Row key={entry.key}>
            <Table.Cell title={entry.label}>
              <span className={styles.labeled}>
                {/* The API names no caller kind yet; only your own row reads "You". */}
                <Icon
                  name={entry.label === 'You' ? 'UserRound' : 'Bot'}
                  size="14"
                  color="tertiary"
                />
                <Typography.BodySmall as="span" className={styles.truncate}>
                  {entry.label}
                </Typography.BodySmall>
              </span>
            </Table.Cell>
            <Table.Cell>
              <BarCell value={sizes[i]} label={share(sizes[i], whole)} max={max} />
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

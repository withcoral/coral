import { TruncatedList } from '@/components/truncated-list'
import { IconButton } from '@/wax/components/button'
import { Pill } from '@/wax/components/pill'
import { Table } from '@/wax/components/table'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './function-list.css'

export interface FunctionListItem {
  description: string
  name: string
  sources: string[]
}

export interface FunctionListProps {
  functions: FunctionListItem[]
  onDelete: (fn: FunctionListItem) => void
}

export function FunctionList({ functions, onDelete }: FunctionListProps) {
  return (
    <Table.Wrapper>
      <Table.Root className={styles.table}>
        <Table.Head>
          <Table.Row>
            <Table.HeaderCell className={styles.nameColumn}>Name</Table.HeaderCell>
            <Table.HeaderCell>Description</Table.HeaderCell>
            <Table.HeaderCell className={styles.sourcesColumn}>Sources</Table.HeaderCell>
            <Table.HeaderCell align="right" ariaLabel="Actions" className={styles.actionsColumn} />
          </Table.Row>
        </Table.Head>
        <Table.Body>
          {functions.map((fn) => (
            <Table.Row className={styles.row} key={fn.name}>
              <Table.Cell>
                <Tooltip content={fn.name} showOnlyWhenTruncated>
                  <span className={styles.cellContent}>
                    <Typography.Body variant="primary">{fn.name}</Typography.Body>
                  </span>
                </Tooltip>
              </Table.Cell>
              <Table.Cell>
                <Tooltip content={fn.description || '—'} showOnlyWhenTruncated>
                  <span className={styles.cellContent}>
                    <Typography.Body variant={fn.description ? 'secondary' : 'tertiary'}>
                      {fn.description || '—'}
                    </Typography.Body>
                  </span>
                </Tooltip>
              </Table.Cell>
              <Table.Cell>
                <FunctionSources sources={fn.sources} />
              </Table.Cell>
              <Table.Cell align="right">
                <div className={styles.action}>
                  <IconButton
                    ariaLabel={`Delete ${fn.name}`}
                    name="Trash2"
                    onClick={() => onDelete(fn)}
                    size="32"
                    variant="bare"
                  />
                </div>
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Table.Wrapper>
  )
}

function FunctionSources({ sources }: { sources: string[] }) {
  if (sources.length === 0) {
    return <Typography.Body variant="tertiary">—</Typography.Body>
  }

  return (
    <TruncatedList
      getKey={(source) => source}
      items={sources}
      renderItem={(source) => <Pill color="gray">{source}</Pill>}
      renderOverflowContent={(hiddenSources) =>
        hiddenSources.map((source) => (
          <Pill color="gray" key={source}>
            {source}
          </Pill>
        ))
      }
      renderOverflowTrigger={(hiddenCount) => (
        <Pill as="button" color="gray">
          +{hiddenCount}
        </Pill>
      )}
    />
  )
}

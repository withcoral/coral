import { useDeferredValue, useState } from 'react'
import { CatalogToolbar } from '@/components/catalog'
import { Table } from '@/wax/components'
import * as styles from './accounts.css'
import { filterQuery } from '@/utils/filter-query'

/** A short list reads at a glance; a filter only helps a long one. */
const FILTER_FROM = 8

/** The permissions Coral requests, once each and sorted. */
export function permissionRows(requested: string[]): string[] {
  return [...new Set(requested)].toSorted()
}

/**
 * Lists what Coral requests. The grant a service reports stays as it wrote it:
 * services name scopes in their own ways, so Coral does not compare the two.
 */
export function PermissionsTable({ rows }: { rows: string[] }) {
  const [input, setInput] = useState('')
  const query = filterQuery(useDeferredValue(input))
  const shown = rows.filter((name) => query.matches(name))
  return (
    <div className={styles.permissions}>
      {rows.length >= FILTER_FROM && (
        <CatalogToolbar
          filter={{ label: 'Filter permissions', value: input, onChange: setInput, fill: true }}
        />
      )}
      <Table.Container
        ariaLabel="Permissions"
        variant="card"
        density="compact"
        layout="fixed"
        maxRows={10}
        staticRows
        columns={[{ label: 'Permission', width: 'minmax(160px, 1fr)' }]}
      >
        <Table.Head />
        {shown.length === 0 ? (
          <Table.Status>{`No permissions match “${query.text}”.`}</Table.Status>
        ) : (
          <Table.Body>
            {shown.map((name) => (
              <Table.Row key={name}>
                <Table.Cell mono>{name}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        )}
      </Table.Container>
    </div>
  )
}

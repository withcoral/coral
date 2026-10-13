import { useDeferredValue, useState, type ReactNode } from 'react'
import { CatalogFilterMenu, CatalogToolbar, type CatalogFilterOption } from '@/components/catalog'
import { Table } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { Person, personLabel } from './person'
import type { PersonView } from './types'

interface MemberRow {
  key: string
  profile: PersonView
  role: string
  /** False when the person's organization membership is disabled. */
  enabled: boolean
  actions?: ReactNode
}

type RoleFilter = 'all' | 'Owner' | 'Member'

const ROLE_FILTERS: CatalogFilterOption<RoleFilter>[] = [
  { value: 'all', label: 'All roles', icon: 'ListFilter' },
  { value: 'Owner', label: 'Owner', icon: 'Crown' },
  { value: 'Member', label: 'Member', icon: 'User' },
]

const FILTER_ID = 'members-filter'

export function MembersTable({
  ariaLabel,
  currentUser,
  rows,
}: {
  ariaLabel: string
  currentUser: string
  rows: MemberRow[]
}) {
  const [input, setInput] = useState('')
  const [role, setRole] = useState<RoleFilter>('all')
  const query = useDeferredValue(input).trim()
  const shown = rows.filter(
    (row) =>
      (role === 'all' || row.role === role) &&
      [personLabel(row.profile), row.profile.email].some((text) =>
        text.toLowerCase().includes(query.toLowerCase()),
      ),
  )
  return (
    <>
      {rows.length > 0 && (
        <CatalogToolbar
          filter={{
            id: FILTER_ID,
            label: 'Filter members',
            shortcut: '$mod+f',
            value: input,
            onChange: setInput,
          }}
        >
          <CatalogFilterMenu
            ariaLabel="Filter members by role"
            options={ROLE_FILTERS}
            value={role}
            onChange={setRole}
          />
        </CatalogToolbar>
      )}
      <Table.Container
        ariaLabel={ariaLabel}
        variant="card"
        layout="fixed"
        staticRows
        columns={[
          { label: 'Name', width: { fill: 2 } },
          { label: 'Role', width: 'fill' },
          { label: 'State', width: 'fill' },
          { ariaLabel: 'Actions', width: 48 },
        ]}
      >
        <Table.Head />
        {rows.length === 0 && <Table.Status>No members on this page.</Table.Status>}
        {rows.length > 0 && shown.length === 0 && (
          <Table.Status>
            {query ? `No members match “${query}”.` : 'No members match.'}
          </Table.Status>
        )}
        <Table.Body>
          {shown.map((row) => (
            <Table.Row key={row.key}>
              <Table.Cell wrap>
                <Person person={row.profile} currentUser={currentUser} compact />
              </Table.Cell>
              <Table.Cell>
                <Pill color={row.role === 'Owner' ? 'blue' : 'gray'}>{row.role}</Pill>
              </Table.Cell>
              <Table.Cell>
                <Pill color={row.enabled ? 'green' : 'gray'}>
                  {row.enabled ? 'Enabled' : 'Disabled'}
                </Pill>
              </Table.Cell>
              <Table.Cell actions>{row.actions}</Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </>
  )
}

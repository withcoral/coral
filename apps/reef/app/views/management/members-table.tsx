import type { ReactNode } from 'react'
import { Table } from '@/wax/components'
import { Person } from './person'
import type { PersonView } from './types'

interface MemberRow {
  key: string
  profile: PersonView
  role: string
  /** False when the person's organization membership is disabled. */
  enabled: boolean
  actions?: ReactNode
}

export function MembersTable({
  ariaLabel,
  currentUser,
  rows,
}: {
  ariaLabel: string
  currentUser: string
  rows: MemberRow[]
}) {
  return (
    <Table.Container
      ariaLabel={ariaLabel}
      layout="fixed"
      columns={[
        { label: 'Name', width: { fill: 2 } },
        { label: 'Role', width: 'fill' },
        { label: 'State', width: 'fill' },
        { ariaLabel: 'Actions', width: 48 },
      ]}
    >
      <Table.Head />
      <Table.Body>
        {rows.length === 0 && <Table.Status>No members on this page.</Table.Status>}
        {rows.map((row) => (
          <Table.Row key={row.key}>
            <Table.Cell wrap>
              <Person person={row.profile} currentUser={currentUser} compact />
            </Table.Cell>
            <Table.Cell wrap>{row.role}</Table.Cell>
            <Table.Cell wrap>{row.enabled ? 'Enabled' : 'Disabled'}</Table.Cell>
            <Table.Cell actions>{row.actions}</Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Container>
  )
}

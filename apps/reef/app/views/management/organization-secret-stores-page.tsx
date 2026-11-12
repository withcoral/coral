import { Table } from '@/wax/components'
import { Status } from '@/wax/components/status'
import { ManagementLayout, trail } from './layout'

/** The secret stores the operator defined in deployment config. */
export function OrganizationSecretStoresPage({
  stores,
}: {
  stores: { name: string; kind: string; blockers: string[] }[]
}) {
  return (
    <ManagementLayout breadcrumbs={trail([], 'Secret stores')} compact>
      <Table.Container
        ariaLabel="Secret stores"
        variant="card"
        layout="fixed"
        staticRows
        columns={[
          { label: 'Name', width: 'fill' },
          { label: 'Kind', width: 'fill' },
          { label: 'Status', width: 'fill' },
        ]}
      >
        <Table.Head />
        {stores.length === 0 && (
          <Table.Status>No secret stores. To define one, edit config.toml.</Table.Status>
        )}
        <Table.Body>
          {stores.map((store) => (
            <Table.Row key={store.name}>
              <Table.Cell title={store.name}>{store.name}</Table.Cell>
              <Table.Cell>{store.kind}</Table.Cell>
              <Table.Cell>
                <Status blockers={store.blockers} ready="Available" />
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </ManagementLayout>
  )
}

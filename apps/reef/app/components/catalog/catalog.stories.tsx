import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { createRoutesStub } from 'react-router'

import { filterQuery } from '@/utils/filter-query'
import { Button, Table } from '@/wax/components'

import { CatalogLayout, CatalogRail, CatalogRailItem, CatalogToolbar } from './catalog'

const groups = [
  { name: 'finance', subtitle: 'finance_reports', members: ['monthlyTotals', 'overdueInvoices'] },
  { name: 'linear', members: ['completedSince', 'issueBrief', 'myOpenIssues'] },
]

function Catalog({ title }: { title?: string }) {
  const [rail, setRail] = useState('')
  const [table, setTable] = useState('')
  const [current, setCurrent] = useState('finance')
  const tableQuery = filterQuery(table)
  const listed = groups.filter((g) => filterQuery(rail).matches(g.name))
  return (
    <div style={{ height: 420 }}>
      <CatalogLayout
        rail={
          <CatalogRail
            label="groups"
            filter={{
              label: 'Filter groups',
              value: rail,
              onChange: setRail,
            }}
            empty={listed.length === 0}
            emptyMessage="No groups match."
          >
            {listed.map((g) => (
              <CatalogRailItem
                key={g.name}
                to={`#${g.name}`}
                current={g.name === current}
                onSelect={() => setCurrent(g.name)}
                name={g.name}
                subtitle={g.subtitle}
                count={g.members.length}
              />
            ))}
          </CatalogRail>
        }
      >
        <CatalogToolbar
          title={title}
          action={
            title && <Button.IconButton name="Plus" tooltipText="Add member" variant="bare" />
          }
          filter={{
            label: 'Filter members',
            value: table,
            onChange: setTable,
          }}
        />
        <Table.Container
          ariaLabel="Members"
          layout="scroll"
          columns={[{ label: 'Name', width: 'fill' }]}
        >
          <Table.Head />
          {listed.map((g) => (
            <Table.Group key={g.name} label={g.name}>
              {g.members
                .filter((m) => tableQuery.matches(m))
                .map((m) => (
                  <Table.Row key={m}>
                    <Table.Cell>{m}</Table.Cell>
                  </Table.Row>
                ))}
            </Table.Group>
          ))}
        </Table.Container>
      </CatalogLayout>
    </div>
  )
}

const meta = {
  component: CatalogLayout,
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return <Stub />
    },
  ],
  parameters: { layout: 'fullscreen' },
  title: 'Components/Catalog',
} satisfies Meta<typeof CatalogLayout>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: { rail: null, children: null },
  render: () => <Catalog />,
}

// The toolbar heads the page with its title and main action.
export const Titled: Story = {
  args: { rail: null, children: null },
  render: () => <Catalog title="Functions" />,
}

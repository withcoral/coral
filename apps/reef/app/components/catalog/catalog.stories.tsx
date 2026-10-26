import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { createRoutesStub } from 'react-router'
import { expect } from 'storybook/test'

import { filterQuery } from '@/utils/filter-query'
import { Button, Table } from '@/wax/components'

import {
  CatalogLayout,
  CatalogRail,
  CatalogRailItem,
  CatalogToolbar,
  toolbarActionLabel,
} from './catalog'

const groups = [
  { name: 'finance', members: ['monthlyTotals', 'overdueInvoices'] },
  { name: 'linear', members: ['completedSince', 'issueBrief', 'myOpenIssues'] },
]

function Catalog({ title, action }: { title?: string; action?: ReactNode }) {
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
            empty={listed.length === 0 ? { message: 'No groups match.' } : null}
          >
            {listed.map((g) => (
              <CatalogRailItem
                key={g.name}
                to={`#${g.name}`}
                current={g.name === current}
                onSelect={() => setCurrent(g.name)}
                name={g.name}
                count={g.members.length}
              />
            ))}
          </CatalogRail>
        }
      >
        <CatalogToolbar
          title={title}
          action={action}
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

// The toolbar heads the page with its title.
export const Titled: Story = {
  args: { rail: null, children: null },
  render: () => <Catalog title="Functions" />,
}

// The main action ends the toolbar. A toolbar with an action lays itself out
// by its own width, and that containment must not shrink it to its content.
export const WithAction: Story = {
  args: { rail: null, children: null },
  render: () => (
    <Catalog
      title="Functions"
      action={
        <Button.Container variant="primary">
          <Button.Icon name="Plus" />
          <Button.Text className={toolbarActionLabel}>New function</Button.Text>
        </Button.Container>
      }
    />
  ),
  play: async ({ canvas }) => {
    const action = canvas.getByRole('button', { name: 'New function' })
    await expect(action).toBeVisible()
    let toolbar = action.parentElement
    while (toolbar && getComputedStyle(toolbar).containerType !== 'inline-size')
      toolbar = toolbar.parentElement
    const parent = toolbar!.parentElement!
    await expect(toolbar!.getBoundingClientRect().width).toBe(parent.getBoundingClientRect().width)
  },
}

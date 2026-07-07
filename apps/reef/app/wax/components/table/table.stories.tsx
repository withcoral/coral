import type { Meta, StoryObj } from '@storybook/react-vite'

import { theme } from '@/wax/theme/theme.css'

import { Table } from './table'

const meta: Meta<typeof Table.Root> = {
  component: Table.Root,
  decorators: [
    (Story) => (
      <div style={{ backgroundColor: theme.surface.mainContent, padding: 24 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Wax/Table',
}

export default meta
type Story = StoryObj<typeof Table.Root>

function ExampleTable({ tableStyle }: { tableStyle: 'compact' | 'default' }) {
  return (
    <Table.Wrapper variant={tableStyle}>
      <Table.Root>
        <Table.Head>
          <Table.Row>
            <Table.HeaderCell>Name</Table.HeaderCell>
            <Table.HeaderCell>Role</Table.HeaderCell>
            <Table.HeaderCell align="right">Requests</Table.HeaderCell>
          </Table.Row>
        </Table.Head>
        <Table.Body>
          <Table.Row>
            <Table.Cell>Ada Lovelace</Table.Cell>
            <Table.Cell>Admin</Table.Cell>
            <Table.Cell align="right" mono>
              24
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>Grace Hopper</Table.Cell>
            <Table.Cell>User</Table.Cell>
            <Table.Cell align="right" mono>
              12
            </Table.Cell>
          </Table.Row>
        </Table.Body>
      </Table.Root>
    </Table.Wrapper>
  )
}

export const Compact: Story = {
  render: () => <ExampleTable tableStyle="compact" />,
}

export const Default: Story = {
  render: () => <ExampleTable tableStyle="default" />,
}

import type { Meta, StoryObj } from '@storybook/react-vite'

import { Table } from '@/wax/components'

import { QueryDetailResults } from './query-detail-results'

const meta = {
  component: QueryDetailResults,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  title: 'Components/QueryDetail/Results',
} satisfies Meta<typeof QueryDetailResults>

export default meta
type Story = StoryObj<typeof meta>

const CATALOG_COLUMNS: Table.Column[] = [
  { label: 'Schema name', width: 'fill' },
  { align: 'right', label: 'Table count', width: 'content' },
]

export const CatalogRows: Story = {
  render: (args) => (
    <QueryDetailResults {...args}>
      <Table.Container columns={CATALOG_COLUMNS} density="compact">
        <Table.Head />
        <Table.Body>
          <Table.Row>
            <Table.Cell mono>gitlab</Table.Cell>
            <Table.Cell align="right" mono>
              216
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell mono>clickup</Table.Cell>
            <Table.Cell align="right" mono>
              46
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell mono>notion</Table.Cell>
            <Table.Cell align="right" mono>
              12
            </Table.Cell>
          </Table.Row>
        </Table.Body>
      </Table.Container>
    </QueryDetailResults>
  ),
}

export const Empty: Story = {}

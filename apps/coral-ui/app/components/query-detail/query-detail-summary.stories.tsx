import type { Meta, StoryObj } from '@storybook/react-vite'

import { QueryDetailSummary } from './query-detail-summary'

const meta = {
  args: {
    sql: 'SELECT * FROM github.meta LIMIT 1',
    stats: [
      { label: 'Duration', value: '42ms' },
      { label: 'Rows', value: '1' },
      { label: 'Table scans', value: '1' },
      { label: 'API requests', value: '2' },
    ],
    statusLabel: 'done',
    statusTone: 'ok',
    title: 'Query details',
  },
  component: QueryDetailSummary,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/QueryDetail/Summary',
} satisfies Meta<typeof QueryDetailSummary>

export default meta
type Story = StoryObj<typeof meta>

export const Done: Story = {}

export const Error: Story = {
  args: {
    sql: 'SELECT * FROM github.repositories LIMIT 5',
    stats: [
      { label: 'Duration', value: '—' },
      { label: 'Rows', value: '—' },
      { label: 'Table scans', value: '1' },
      { label: 'API requests', value: '1' },
    ],
    statusLabel: 'error',
    statusTone: 'error',
  },
}

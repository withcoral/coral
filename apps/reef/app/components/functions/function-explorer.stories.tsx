import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { expect, fn } from 'storybook/test'

import { FunctionExplorer, type FunctionExplorerProps } from './function-explorer'

const functions: FunctionExplorerProps['functions'] = [
  {
    arguments: [
      { dataType: 'Utf8', name: 'owner' },
      { dataType: 'Utf8', name: 'repo' },
    ],
    body: `select
  pull.number,
  pull.title,
  pull.html_url,
  pull.author_login
from github.pulls(owner => $owner, repo => $repo) as pull
where pull.state = 'open'
  and pull.review_decision = 'REVIEW_REQUIRED'
order by pull.updated_at desc`,
    description: 'Pull requests waiting for review in a repository.',
    name: 'review_queue',
    namespace: 'engineering',
    resultColumns: [
      { dataType: 'Int64', name: 'number', nullable: false },
      { dataType: 'Utf8', name: 'title', nullable: false },
      { dataType: 'Utf8', name: 'html_url', nullable: false },
      { dataType: 'Utf8', name: 'author_login', nullable: true },
    ],
    sources: ['github'],
  },
  {
    arguments: [
      { dataType: 'Timestamp', name: 'since' },
      { dataType: 'Utf8', name: 'severity' },
    ],
    description: 'Incidents opened since a point in time, filtered by severity.',
    name: 'recent_incidents',
    namespace: 'operations',
    resultColumns: [
      { dataType: 'Utf8', name: 'id', nullable: false },
      { dataType: 'Utf8', name: 'title', nullable: false },
      { dataType: 'Utf8', name: 'status', nullable: false },
    ],
    sources: ['datadog', 'pagerduty'],
  },
  {
    arguments: [],
    description: 'Customer conversations that still need a response.',
    name: 'customer_follow_up',
    namespace: 'operations',
    resultColumns: [
      { dataType: 'Utf8', name: 'conversation_id', nullable: false },
      { dataType: 'Utf8', name: 'summary', nullable: true },
    ],
    sources: ['intercom', 'linear', 'slack'],
  },
]

function StatefulExplorer({
  onSelect,
  selectedName: initialSelectedName,
  ...args
}: FunctionExplorerProps) {
  const [selectedName, setSelectedName] = useState(initialSelectedName)
  return (
    <FunctionExplorer
      {...args}
      onSelect={(name) => {
        setSelectedName(name)
        onSelect(name)
      }}
      selectedName={selectedName}
    />
  )
}

const meta = {
  args: {
    functions,
    onDelete: fn(),
    onSelect: fn(),
    selectedName: functions[0].name,
  },
  component: FunctionExplorer,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => (
    <div style={{ height: '100dvh' }}>
      <StatefulExplorer {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/Functions/FunctionExplorer',
} satisfies Meta<typeof FunctionExplorer>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  play: async ({ canvas, userEvent }) => {
    await expect(canvas.getByRole('button', { name: /engineering/ })).toHaveAttribute(
      'aria-expanded',
      'false',
    )
    await userEvent.click(canvas.getByRole('button', { name: /engineering/ }))
    await expect(canvas.getByRole('button', { name: 'review_queue' })).toHaveAttribute(
      'aria-pressed',
      'true',
    )

    await userEvent.click(canvas.getByRole('button', { name: /operations/ }))
    await userEvent.click(canvas.getByRole('button', { name: 'recent_incidents' }))

    await expect(canvas.getByRole('button', { name: 'recent_incidents' })).toHaveAttribute(
      'aria-pressed',
      'true',
    )
    await expect(canvas.getByRole('heading', { name: 'recent_incidents' })).toBeVisible()
  },
}

export const Empty: Story = {
  args: {
    functions: [],
    selectedName: undefined,
  },
}

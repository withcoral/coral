import type { Meta, StoryObj } from '@storybook/react-vite'
import { fn } from 'storybook/test'

import { RuntimeFeatureRow, RuntimeFeaturesList } from './runtime-features-list'

const meta = {
  args: {
    features: [
      {
        description: 'Enables installing and querying database sources. Off by default.',
        enabled: false,
        key: 'database_sources',
        label: 'Database sources',
      },
      {
        description: 'Exposes the MCP feedback tool when enabled.',
        enabled: true,
        key: 'feedback',
        label: 'Feedback',
      },
      {
        description:
          'Enables collecting, indexing, retrieving, and maintaining values observed during earlier queries. Off by default.',
        enabled: false,
        key: 'observed_values_search',
        label: 'Observed values search',
      },
    ],
    renderRow: (feature) => <RuntimeFeatureRow feature={feature} onToggle={fn()} />,
  },
  component: RuntimeFeaturesList,
  parameters: { layout: 'padded' },
  render: (args) => (
    <div style={{ maxWidth: 960 }}>
      <RuntimeFeaturesList {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/RuntimeFeaturesList',
} satisfies Meta<typeof RuntimeFeaturesList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Pending: Story = {
  args: {
    renderRow: (feature) => (
      <RuntimeFeatureRow feature={feature} onToggle={fn()} pending={feature.key === 'feedback'} />
    ),
  },
}

export const WriteError: Story = {
  args: {
    renderRow: (feature) => (
      <RuntimeFeatureRow
        error={feature.key === 'feedback' ? "Unknown feature 'feedback'." : undefined}
        feature={feature}
        onToggle={fn()}
      />
    ),
  },
}

export const Empty: Story = {
  args: { features: [] },
}

export const Error: Story = {
  args: { error: 'Unable to read runtime features.', features: [] },
}

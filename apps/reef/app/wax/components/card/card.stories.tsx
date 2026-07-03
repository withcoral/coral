import type { Meta, StoryObj } from '@storybook/react-vite'

import { fn } from 'storybook/test'

import { Icon } from '@/wax/components/icon'

import { Card } from './card'

const meta = {
  component: Card,
  parameters: {
    layout: 'centered',
  },
  render: (args) => (
    <div style={{ width: 320 }}>
      <Card {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Wax/Card',
} satisfies Meta<typeof Card>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    description: 'Monitor logs, metrics, and traces across your services.',
    title: 'Datadog',
  },
}

export const WithIcon: Story = {
  args: {
    description: 'Sync issues, pull requests, and code from your repositories.',
    icon: <Icon name="GitBranch" size="20" />,
    title: 'GitHub',
  },
}

export const NoIcon: Story = {
  args: {
    description: 'A source rendered without an icon.',
    title: 'Plain source',
  },
}

export const Interactive: Story = {
  args: {
    description: 'Click this card — it renders as a button when onSelect is provided.',
    icon: <Icon name="Database" size="20" />,
    onSelect: fn(),
    title: 'Postgres',
  },
}

export const LongText: Story = {
  args: {
    description:
      'This description is intentionally very long to exercise wrapping behaviour. ' +
      'It keeps going to make sure text inside the card reflows gracefully instead of ' +
      'overflowing the card boundary, including a ReallyLongUnbrokenTokenWithoutAnySpaces.',
    icon: <Icon name="Activity" size="20" />,
    title: 'A source with an unusually long title that truncates with an ellipsis on one line',
  },
}

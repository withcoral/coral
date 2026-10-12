import type { Meta, StoryObj } from '@storybook/react-vite'

import { Link, MemoryRouter } from 'react-router'
import { expect, fn } from 'storybook/test'

import { Button } from '@/wax/components'
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

export const WithHeaderPill: Story = {
  args: {
    description: 'A source with a compact status pill in the card header.',
    headerPill: { label: 'Imported' },
    icon: <Icon name="Plug" size="20" />,
    title: 'Custom source',
  },
}

export const WithPillBesideTitle: Story = {
  args: {
    headerPill: { color: 'green', label: 'Added', placement: 'title' },
    icon: <Icon name="Plug" size="20" />,
    title: 'Custom source',
  },
}

export const Compact: Story = {
  args: {
    compact: true,
    icon: <Icon name="Plug" size="20" />,
    title: 'Custom source',
  },
}

export const WithAction: Story = {
  args: {
    description: 'Disabling the agent blocks new runs and calls that Coral has not sent.',
    headerPill: { label: 'Enabled', color: 'green' },
    title: 'Agent',
    action: <Button.TextButton variant="secondary">Disable agent</Button.TextButton>,
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
    description: 'Click this card — it renders as a button through the polymorphic as prop.',
    icon: <Icon name="Database" size="20" />,
    title: 'Postgres',
  },
  render: (args) => (
    <div style={{ width: 320 }}>
      <Card {...args} as="button" onClick={fn()} />
    </div>
  ),
}

export const AsReactRouterLink: Story = {
  args: {
    description: 'Navigate to the GitHub source details route using React Router Link.',
    headerPill: { label: 'Core' },
    icon: <Icon name="GitBranch" size="20" />,
    title: 'GitHub',
  },
  render: (args) => (
    <MemoryRouter>
      <div style={{ width: 320 }}>
        <Card {...args} as={Link} prefetch="intent" to="/workspaces/default/functions" />
      </div>
    </MemoryRouter>
  ),
}

// Catches: a form card's action rendered as content instead of submitting to it.
export const AsForm: Story = {
  args: {
    description: 'A card that is a form keeps its own action as the destination.',
    title: 'Rename workspace',
  },
  render: (args) => (
    <div style={{ width: 320 }}>
      <Card {...args} as="form" action="/settings" method="post" aria-label="Rename workspace" />
    </div>
  ),
  play: async ({ canvas }) => {
    const form = canvas.getByRole('form', { name: 'Rename workspace' })
    await expect(form).toHaveAttribute('action', '/settings')
    await expect(form).not.toHaveTextContent('/settings')
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

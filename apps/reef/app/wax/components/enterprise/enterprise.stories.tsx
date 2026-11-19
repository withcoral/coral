import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'

import { EnterpriseBadge, EnterpriseGate } from './enterprise'

const meta = {
  component: EnterpriseGate,
  title: 'Wax/Enterprise',
} satisfies Meta<typeof EnterpriseGate>

export default meta
type Story = StoryObj<typeof meta>

/** The badge names the Edition in text beside an icon, so colour alone carries nothing. */
export const Badge: Story = {
  args: { enabled: true, feature: 'Example feature', children: null },
  render: () => (
    <div style={{ display: 'flex', gap: 8 }}>
      <EnterpriseBadge size="small" />
      <EnterpriseBadge size="default" />
    </div>
  ),
  play: async ({ canvasElement }) => {
    expect(within(canvasElement).getAllByText('Enterprise')).toHaveLength(2)
  },
}

/** While the feature works, the gate shows only what it holds. */
export const Enabled: Story = {
  args: {
    enabled: true,
    feature: 'Example feature',
    children: <p>Secret store settings</p>,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    expect(canvas.getByText('Secret store settings')).toBeVisible()
    expect(canvas.queryByText('Enterprise')).toBeNull()
  },
}

/** In the Community Edition, the gate names the feature and hides what it holds. */
export const NeedsEnterprise: Story = {
  args: {
    enabled: false,
    feature: 'Example feature',
    children: <p>Secret store settings</p>,
  },
  render: (args) => (
    <div style={{ maxWidth: 720 }}>
      <EnterpriseGate {...args} />
    </div>
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    expect(canvas.getByRole('status')).toHaveTextContent('Example feature')
    expect(canvas.getByRole('status')).toHaveTextContent(
      'You need the Enterprise Edition for this feature.',
    )
    expect(canvas.getByText('Enterprise')).toBeVisible()
    expect(canvas.queryByText('Secret store settings')).toBeNull()
  },
}

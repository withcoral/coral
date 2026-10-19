import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect } from 'storybook/test'

import { Status } from './status'
import { StatusDot } from './status-dot'

const meta = {
  component: Status,
  title: 'Components/Status',
  args: { blockers: [] },
} satisfies Meta<typeof Status>
export default meta
type Story = StoryObj<typeof meta>

export const Ready: Story = {}
export const OneBlocker: Story = { args: { blockers: ['Access expired'] } }
export const SeveralBlockers: Story = {
  args: {
    blockers: ['Agent disabled', 'API access suspended', 'Workspace archived'],
    detail: true,
  },
}
export const CustomReadyLabel: Story = { args: { ready: 'Active' } }
// Catches: a narrow table cell clipping the pill's box instead of truncating its text.
export const NarrowColumn: Story = {
  args: { blockers: ['Access expired'] },
  render: (args) => (
    <div data-testid="column" style={{ width: 72, overflow: 'hidden', whiteSpace: 'nowrap' }}>
      <Status {...args} />
    </div>
  ),
  play: async ({ canvas }) => {
    const column = canvas.getByTestId('column').getBoundingClientRect()
    const pill = canvas.getByTitle('Access expired').getBoundingClientRect()
    await expect(pill.right).toBeLessThanOrEqual(column.right)
  },
}
export const Dots: Story = {
  render: () => (
    <span style={{ display: 'inline-flex', gap: 12 }}>
      <StatusDot blockers={[]} />
      <StatusDot blockers={[]} personalAccountAction="Connect your account" />
      <StatusDot blockers={['Sign-in needed']} />
      <StatusDot blockers={['Provider disabled']} personalAccountAction="Connect your account" />
    </span>
  ),
  // Catches: your account hiding why the resource itself is blocked.
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('img', { name: 'Ready' })).toBeVisible()
    await expect(canvas.getByRole('img', { name: 'Connect your account' })).toBeVisible()
    await expect(canvas.getByRole('img', { name: 'Sign-in needed' })).toBeVisible()
    await expect(canvas.getByRole('img', { name: 'Provider disabled' })).toBeVisible()
    await expect(canvas.getAllByRole('img', { name: 'Connect your account' })).toHaveLength(1)
  },
}

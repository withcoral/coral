import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within } from 'storybook/test'
import { useState } from 'react'

import { PolicyToggle, type PolicyState } from './policy-toggle'

function Demo({
  disabled,
  initial = 'approval_required',
}: {
  disabled?: boolean
  initial?: PolicyState
}) {
  const [value, setValue] = useState<PolicyState>(initial)
  return <PolicyToggle label="Policy" value={value} onChange={setValue} disabled={disabled} />
}

const meta = {
  component: Demo,
  title: 'Components/PolicyToggle',
} satisfies Meta<typeof Demo>
export default meta
type Story = StoryObj<typeof meta>

// Each restrictive policy says what it does, for sighted and screen reader users.
export const NeedsApproval: Story = {
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByRole('radio', { name: 'Needs approval' })).toHaveAccessibleDescription(
      'Coral asks before each use.',
    )
    await expect(canvas.getByRole('radio', { name: 'Disabled' })).toHaveAccessibleDescription(
      'Coral blocks every call. Search results do not show this tool.',
    )
  },
}
export const AlwaysAllow: Story = { args: { initial: 'enabled' } }
export const Disabled: Story = { args: { initial: 'disabled' } }
export const Locked: Story = { args: { disabled: true } }

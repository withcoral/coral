import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, userEvent } from 'storybook/test'

import { Button } from '@/wax/components'

import { SecretReveal } from './secret-reveal'

const meta = {
  component: SecretReveal,
  title: 'Components/SecretReveal',
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return <Stub initialEntries={['/']} />
    },
  ],
  args: {
    title: 'Save this API key now',
    warning:
      "Save this API key in your client's credential settings. Coral shows it only once. Replace the key if you lose it.",
    secret: 'coral_agent_storybook_only_not_a_valid_credential',
    copyLabel: 'Copy API key',
    dismissLabel: 'Dismiss API key',
    actions: <Button.InternalLink to="/">Manage agent</Button.InternalLink>,
  },
} satisfies Meta<typeof SecretReveal>
export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
export const Dismissed: Story = {
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Dismiss API key' }))
    await expect(canvas.queryByText(/coral_agent_/)).toBeNull()
  },
}
export const Narrow: Story = { parameters: { chromatic: { viewports: [390] } } }

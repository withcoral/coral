import type { Meta, StoryObj } from '@storybook/react-vite'

import { Button } from '@/wax/components'
import { Pill } from '@/wax/components/pill'

import * as SettingRow from './index'

const meta = {
  title: 'Components/SettingRow',
  render: () => (
    <SettingRow.List>
      <SettingRow.Row
        label="Agent"
        value={<Pill color="green">Enabled</Pill>}
        description="Disabling the agent blocks new runs and calls that Coral has not sent. Enabling it leaves other access settings unchanged."
        action={<Button.TextButton variant="secondary">Disable agent</Button.TextButton>}
      />
      <SettingRow.Row
        label="Access expires"
        value={<Pill color="gray">Never</Pill>}
        description="A past date stops access immediately."
        action={<Button.TextButton variant="secondary">Edit expiration</Button.TextButton>}
      />
      <SettingRow.Row
        label="API key"
        description="Replacing the key immediately invalidates the old one."
        action={<Button.TextButton variant="destructive">Replace API key</Button.TextButton>}
      />
      <SettingRow.Row label="Read-only setting" value={<Pill color="gray">Fixed</Pill>} />
    </SettingRow.List>
  ),
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
export const Narrow: Story = { parameters: { chromatic: { viewports: [390] } } }

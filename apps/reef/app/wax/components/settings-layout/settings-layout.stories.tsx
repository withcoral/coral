import type { Meta, StoryObj } from '@storybook/react-vite'

import { Button, Typography } from '@/wax/components'
import * as CardFrame from '@/wax/components/card'
import { Card } from '@/wax/components/card/card'

import { SettingsLayout, SettingsRemoval } from './index'

const meta = {
  title: 'Components/SettingsLayout',
  render: () => (
    <SettingsLayout
      header={<Typography.HeadingLarge as="h2">Nightly reconciler</Typography.HeadingLarge>}
      aside={
        <CardFrame.Container as="aside" aria-label="Source">
          <Typography.BodyLargeStrong as="h2">Source</Typography.BodyLargeStrong>
        </CardFrame.Container>
      }
      removal={
        <SettingsRemoval
          title="Delete agent"
          description="Delete this agent and its API key. Coral keeps its run history."
          action={<Button.TextButton variant="destructive">Delete agent</Button.TextButton>}
        />
      }
    >
      <Card
        title="Agent"
        description="Disabling the agent blocks new runs."
        headerPill={{ label: 'Enabled', color: 'green' }}
        action={<Button.TextButton variant="secondary">Disable agent</Button.TextButton>}
      />
      <Card title="API key" description="Replacing the key invalidates the old key immediately." />
    </SettingsLayout>
  ),
} satisfies Meta

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
export const WithoutAside: Story = {
  render: () => (
    <SettingsLayout header={<Typography.HeadingLarge as="h2">Settings</Typography.HeadingLarge>}>
      <Card title="Agent" description="Disabling the agent blocks new runs." />
    </SettingsLayout>
  ),
}

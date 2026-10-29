import type { Meta, StoryObj } from '@storybook/react-vite'

import { Button, Typography } from '@/wax/components'
import { Card } from '@/wax/components/card/card'

import {
  SettingsAside,
  SettingsFact,
  SettingsFacts,
  SettingsLayout,
  SettingsRemoval,
  SettingsWideColumns,
} from './index'

const meta = {
  title: 'Components/SettingsLayout',
  render: () => (
    <SettingsLayout
      header={<Typography.HeadingLarge as="h2">Nightly reconciler</Typography.HeadingLarge>}
      aside={
        <SettingsAside title="Source">
          <SettingsFacts>
            <SettingsFact label="Type">OpenAPI document</SettingsFact>
            <SettingsFact label="URL">api.example.com</SettingsFact>
          </SettingsFacts>
        </SettingsAside>
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
/** A long list takes the free width; the aside keeps its settings width. */
export const WideColumns: Story = {
  render: () => (
    <SettingsWideColumns
      aside={
        <SettingsAside title="Source">
          <SettingsFacts>
            <SettingsFact label="Type">OpenAPI document</SettingsFact>
          </SettingsFacts>
        </SettingsAside>
      }
    >
      <Card title="Tools" description="Every tool the source offers." />
    </SettingsWideColumns>
  ),
}

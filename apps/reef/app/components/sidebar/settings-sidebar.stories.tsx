import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { SettingsSidebar } from './settings-sidebar'

const meta = {
  component: SettingsSidebar,
  decorators: [
    (Story, context) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ padding: 12, width: 220 }}>
          <Stub initialEntries={[context.parameters.initialEntry ?? '/organization/members']} />
        </div>
      )
    },
  ],
  args: {
    reader: { organizationOwner: true, people: true, secretStoresEnabled: true },
    back: '/workspaces/demo/tools',
    isMinimized: false,
  },
  title: 'Components/Sidebar/Settings',
} satisfies Meta<typeof SettingsSidebar>
export default meta
type Story = StoryObj<typeof meta>

// An organization owner sees every section.
export const Owner: Story = {}
// A member has no settings. Coral refuses a page they open by its address,
// and the nav then shows only the way back.
export const Member: Story = {
  args: { reader: { organizationOwner: false, people: true, secretStoresEnabled: true } },
  parameters: { initialEntry: '/usage-data' },
}
// A local deployment has no other people, so it has no members to manage.
export const LocalDeployment: Story = {
  args: { reader: { organizationOwner: true, people: false, secretStoresEnabled: true } },
  parameters: { initialEntry: '/organization/workspaces' },
}
// Desktop turns secret stores off, so it has no secret stores section.
export const SecretStoresOff: Story = {
  args: { reader: { organizationOwner: true, people: false, secretStoresEnabled: false } },
  parameters: { initialEntry: '/organization/workspaces' },
}
export const Collapsed: Story = { args: { isMinimized: true } }

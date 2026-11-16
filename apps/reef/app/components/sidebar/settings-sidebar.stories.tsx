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
    reader: { organizationOwner: true, people: true },
    back: '/workspaces/demo/tools',
    isMinimized: false,
  },
  title: 'Components/Sidebar/Settings',
} satisfies Meta<typeof SettingsSidebar>
export default meta
type Story = StoryObj<typeof meta>

// An organization owner sees every section.
export const Owner: Story = {}
// A member reads the usage data and administers nothing in the organization.
export const Member: Story = {
  args: { reader: { organizationOwner: false, people: true } },
  parameters: { initialEntry: '/usage-data' },
}
// A local deployment has no other people, so it has no members to manage.
export const LocalDeployment: Story = {
  args: { reader: { organizationOwner: true, people: false } },
  parameters: { initialEntry: '/organization/workspaces' },
}
export const Collapsed: Story = { args: { isMinimized: true } }

import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { userEvent, within } from 'storybook/test'
import { ContentContainer } from '@/components/content-container'
import { NavigationRoot } from '@/wax/components/navigation-frame/navigation-frame'
import { PageLayout } from '@/wax/components/page-layout'
import { Typography } from '@/wax/components/typography'
import { Sidebar } from './sidebar'
import * as shell from './sidebar.stories.css'

const meta = {
  component: Sidebar,
  decorators: [
    (Story, context) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return <Stub initialEntries={[context.parameters.initialEntry ?? '/workspaces/demo']} />
    },
  ],
  parameters: { layout: 'fullscreen' },
  render: (args) => (
    <NavigationRoot>
      <div className={shell.layout}>
        <Sidebar {...args} />
        <ContentContainer>
          <PageLayout breadcrumbs={[{ id: 'overview', type: 'text', text: 'Overview' }]}>
            <Typography.Body>Workspace overview</Typography.Body>
          </PageLayout>
        </ContentContainer>
      </div>
    </NavigationRoot>
  ),
  args: {
    auth: { mode: 'required', csrfToken: 'storybook' },
    userName: 'Alex Morgan',
    initialIsMinimized: false,
    workspace: {
      path: '/workspaces/demo',
      displayName: 'Operations team',
      effectiveRole: 'owner',
      mcpEndpoint: 'https://coral.example.com/mcp',
      etag: 'etag',
      state: 'active',
    },
    workspaces: [
      {
        displayName: 'Operations team',
        path: '/workspaces/demo',
        effectiveRole: 'owner',
        mcpEndpoint: 'https://coral.example.com/mcp',
        etag: 'etag',
        state: 'active',
      },
      {
        displayName: 'Research',
        path: '/workspaces/research',
        effectiveRole: 'member',
        mcpEndpoint: 'https://coral.example.com/mcp',
        etag: 'etag',
        state: 'active',
      },
    ],
    form: { csrf: 'storybook', requestId: 'storybook' },
  },
  title: 'Components/Sidebar',
} satisfies Meta<typeof Sidebar>
export default meta
type Story = StoryObj<typeof meta>
export const Owner: Story = {}
export const Activity: Story = {
  parameters: { initialEntry: '/workspaces/demo/activity' },
}
// The current Workspace shows each count on its page; the switcher the total of the others.
export const ActionCount: Story = {
  ...Activity,
  args: {
    actionCounts: {
      '/workspaces/demo': { total: '5', approvals: '3', connections: '2' },
      '/workspaces/research': { total: '12', approvals: '12', connections: '0' },
    },
  },
}
export const LargeActionCount: Story = {
  ...Activity,
  args: {
    actionCounts: {
      '/workspaces/demo': { total: '1000', approvals: '1000', connections: '0' },
      '/workspaces/research': { total: '0', approvals: '0', connections: '0' },
    },
  },
}
export const CollapsedActionCount: Story = {
  ...ActionCount,
  args: { ...ActionCount.args, initialIsMinimized: true },
}
export const Collapsed: Story = { args: { initialIsMinimized: true } }
const openSwitcher: Story['play'] = async ({ canvas }) => {
  await userEvent.click(canvas.getByRole('button', { name: /^Workspace: / }))
}
export const Switcher: Story = { play: openSwitcher }
// Each other Workspace shows its own count in the menu.
export const SwitcherActionCount: Story = { ...ActionCount, play: openSwitcher }
export const CollapsedSwitcher: Story = { ...Collapsed, play: openSwitcher }
export const CollapsedManyWorkspaces: Story = {
  args: {
    initialIsMinimized: true,
    workspaces: [
      'Operations team',
      'Research',
      'Growth experiments',
      'Customer support',
      'Finance',
      'Data platform',
      'Security review',
    ].map((displayName, i) => ({
      displayName,
      path: i === 0 ? '/workspaces/demo' : `/workspaces/w${i}`,
      effectiveRole: 'owner',
      mcpEndpoint: 'https://coral.example.com/mcp',
      etag: 'etag',
      state: 'active',
    })),
  },
  parameters: { initialEntry: '/workspaces/demo/tools' },
}
// The menu scrolls; the sidebar keeps one Workspace's pages.
export const ManyWorkspaces: Story = {
  play: openSwitcher,
  args: {
    workspaces: Array.from({ length: 24 }, (_, i) => ({
      displayName: i === 0 ? 'Operations team' : `Workspace ${i + 1}`,
      path: i === 0 ? '/workspaces/demo' : `/workspaces/w${i}`,
      effectiveRole: 'owner',
      mcpEndpoint: 'https://coral.example.com/mcp',
      etag: 'etag',
      state: 'active',
    })),
  },
  parameters: { initialEntry: '/workspaces/demo/tools' },
}
export const LocalUser: Story = {
  args: { auth: { mode: 'disabled' }, userName: undefined },
}
export const MissingProfile: Story = { args: { userName: undefined } }
export const EmailFallback: Story = { args: { userName: 'alex.morgan@example.com' } }
const openAccountMenu: Story['play'] = async ({ canvas }) => {
  await userEvent.click(canvas.getByRole('button', { name: /^Open account menu/ }))
}
export const LongUserName: Story = {
  args: { userName: 'Alexandra Morgan with a name long enough to truncate' },
}
export const AccountMenu: Story = { play: openAccountMenu }
export const CollapsedAccountMenu: Story = { ...Collapsed, play: openAccountMenu }
export const LocalAccountMenu: Story = { ...LocalUser, play: openAccountMenu }
export const HelpMenu: Story = {
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Help and feedback' }))
  },
}
export const Member: Story = {
  args: {
    workspace: {
      path: '/workspaces/demo',
      displayName: 'Operations team',
      effectiveRole: 'member',
      mcpEndpoint: 'https://coral.example.com/mcp',
      etag: 'etag',
      state: 'active',
    },
  },
}
export const LongWorkspaceName: Story = {
  args: {
    workspace: {
      path: '/workspaces/demo',
      displayName: 'A Workspace with a display name long enough to truncate',
      effectiveRole: 'owner',
      mcpEndpoint: 'https://coral.example.com/mcp',
      etag: 'etag',
      state: 'active',
    },
  },
}
export const NoWorkspace: Story = {
  args: { workspace: undefined },
  parameters: { initialEntry: '/' },
}
export const Agents: Story = { parameters: { initialEntry: '/workspaces/demo/agents' } }
// A provider's own pages keep its resource path, and still mark Tools.
export const ToolsOnProviderPage: Story = {
  parameters: { initialEntry: '/workspaces/demo/providers/github/settings' },
}
const archivedWorkspace = {
  path: '/workspaces/archived',
  displayName: 'Archived workspace',
  effectiveRole: 'owner',
  mcpEndpoint: '',
  etag: 'etag',
  state: 'archived',
}
// An open archived workspace shows the archive's icon and only its Activity.
// The switcher does not list it: it lists the workspaces you can work in.
export const ArchivedWorkspace: Story = {
  args: { workspace: archivedWorkspace },
  parameters: { initialEntry: '/workspaces/archived/activity' },
}
export const ArchivedWorkspaceSwitcher: Story = { ...ArchivedWorkspace, play: openSwitcher }
export const CollapsedArchivedWorkspace: Story = {
  ...ArchivedWorkspace,
  args: { ...ArchivedWorkspace.args, initialIsMinimized: true },
}
// In the organization's settings their nav takes the place of the switcher and the pages.
export const OrganizationSettings: Story = {
  args: { area: 'settings', organizationOwner: true, workspace: undefined },
  parameters: { initialEntry: '/organization/members' },
}
export const CollapsedOrganizationSettings: Story = {
  ...OrganizationSettings,
  args: { ...OrganizationSettings.args, initialIsMinimized: true },
}
export const CreateWorkspace: Story = {
  play: async (context) => {
    await openSwitcher(context)
    const body = within(context.canvasElement.ownerDocument.body)
    await userEvent.click(await body.findByRole('menuitem', { name: 'Create workspace' }))
    await body.findByRole('dialog', { name: 'Create workspace' })
  },
}
const newerRelease = {
  version: '1.3.0',
  notesUrl: 'https://github.com/withcoral/coral/releases/tag/v1.3.0',
}
export const NewerRelease: Story = { args: { newerRelease } }
export const CollapsedNewerRelease: Story = { args: { newerRelease, initialIsMinimized: true } }
export const Dark: Story = { globals: { backgrounds: { value: 'dark' } } }
export const Light: Story = { globals: { backgrounds: { value: 'light' } } }
export const Narrow: Story = {
  // Chromatic reads `viewports`; Storybook and its Vitest run read the `viewport` global.
  globals: { viewport: { value: 'mobile2' } },
  parameters: { chromatic: { viewports: [390] } },
}
export const Drawer: Story = {
  ...Narrow,
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Open navigation' }))
  },
}

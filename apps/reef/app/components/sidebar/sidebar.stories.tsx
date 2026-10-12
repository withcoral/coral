import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { userEvent } from 'storybook/test'
import { ContentContainer } from '@/components/content-container'
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
    <div className={shell.layout}>
      <Sidebar {...args} />
      <ContentContainer>
        <Typography.HeadingMedium>Overview</Typography.HeadingMedium>
      </ContentContainer>
    </div>
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
    },
    workspaces: [
      {
        displayName: 'Operations team',
        path: '/workspaces/demo',
        effectiveRole: 'owner',
        mcpEndpoint: 'https://coral.example.com/mcp',
      },
      {
        displayName: 'Research',
        path: '/workspaces/research',
        effectiveRole: 'member',
        mcpEndpoint: 'https://coral.example.com/mcp',
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
// The open Workspace shows its count on Activity; the closed one on its name.
export const ActionCount: Story = {
  ...Activity,
  args: {
    actionCounts: { '/workspaces/demo': '3', '/workspaces/research': '12' },
  },
}
export const LargeActionCount: Story = {
  ...Activity,
  args: {
    actionCounts: { '/workspaces/demo': '1000', '/workspaces/research': '0' },
  },
}
export const CollapsedActionCount: Story = {
  ...ActionCount,
  args: { ...ActionCount.args, initialIsMinimized: true },
}
export const UnavailableActionCount: Story = {
  ...Activity,
  args: { actionCounts: null },
}
export const Collapsed: Story = { args: { initialIsMinimized: true } }
// Collapsed, Slack and GitHub fold into one help menu above the account.
export const CollapsedHelpMenu: Story = {
  ...Collapsed,
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Get help' }))
  },
}
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
    })),
  },
  parameters: { initialEntry: '/workspaces/demo/providers' },
}
// The list scrolls under the fixed Workspaces label; the footer stays put.
export const ManyWorkspaces: Story = {
  args: {
    workspaces: Array.from({ length: 24 }, (_, i) => ({
      displayName: i === 0 ? 'Operations team' : `Workspace ${i + 1}`,
      path: i === 0 ? '/workspaces/demo' : `/workspaces/w${i}`,
      effectiveRole: 'owner',
      mcpEndpoint: 'https://coral.example.com/mcp',
    })),
  },
  parameters: { initialEntry: '/workspaces/demo/providers' },
}
export const LocalUser: Story = {
  args: { auth: { mode: 'disabled' }, userName: undefined },
}
export const MissingProfile: Story = { args: { userName: undefined } }
export const EmailFallback: Story = { args: { userName: 'alex.morgan@example.com' } }
export const LongUserName: Story = {
  args: { userName: 'Alexandra Morgan with a name long enough to truncate' },
}
export const AccountMenu: Story = {
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Open account menu for Alex Morgan' }))
  },
}
export const Member: Story = {
  args: {
    workspace: {
      path: '/workspaces/demo',
      displayName: 'Operations team',
      effectiveRole: 'member',
      mcpEndpoint: 'https://coral.example.com/mcp',
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
    },
  },
}
export const NoWorkspace: Story = {
  args: { workspace: undefined },
  parameters: { initialEntry: '/' },
}
export const Agents: Story = { parameters: { initialEntry: '/workspaces/demo/agents' } }
export const Functions: Story = {
  parameters: { initialEntry: '/workspaces/demo/functions/finance.monthlyTotals' },
}
export const Settings: Story = { parameters: { initialEntry: '/workspaces/demo/settings' } }
export const OpenAnotherWorkspace: Story = {
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Research' }))
  },
}
// The list holds active workspaces; an archived one shows while it is open.
export const ArchivedWorkspace: Story = {
  args: {
    workspace: {
      path: '/workspaces/archived',
      displayName: 'Archived workspace',
      effectiveRole: 'owner',
      mcpEndpoint: 'https://coral.example.com/mcp',
    },
  },
  parameters: { initialEntry: '/workspaces/archived' },
}
export const CreateWorkspace: Story = {
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('button', { name: 'Create workspace' }))
  },
}
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

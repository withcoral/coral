import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, within } from 'storybook/test'
import { workspaceView } from '@/views/providers/fixtures'
import { UserAccountPage } from './user-account-page'
import type { AccountView, UserAccountView } from './types'

const slackScopes = [
  'canvases:read',
  'canvases:write',
  'channels:history',
  'channels:read',
  'channels:write',
  'chat:write',
  'emoji:read',
  'files:read',
  'files:write',
  'groups:history',
  'groups:read',
  'im:history',
  'im:read',
  'reactions:read',
  'reactions:write',
  'search:read.public',
  'users:read',
  'users:read.email',
]
const notConnected: UserAccountView = {
  workspace: { ...workspaceView, effectiveRole: 'member' },
  providerName: 'Linear',
  path: '/connect',
  returnTo: '/workspaces/demo/runs/run',
  accountsPath: '/workspaces/demo/accounts',
  state: 'not_connected',
  scopes: ['read', 'write'],
  canConnect: true,
  canDisconnect: false,
  oauth: true,
  credentialFields: [{ name: 'token', label: 'Token', description: '', sensitive: true }],
  form: { csrf: 'storybook', requestId: 'storybook' },
}
const account: AccountView = {
  id: 'b0031242-ea22-42bf-9cdd-d55539fb9f4c',
  canManage: true,
  state: 'ready',
  grantedScope: 'read write',
  connectedAt: '2026-10-12T10:25:33.240Z',
  configuredByUser: 'u_demo',
}
const connected: UserAccountView = {
  ...notConnected,
  returnTo: null,
  state: 'connected',
  canDisconnect: true,
  account,
}
const slack: UserAccountView = {
  ...connected,
  providerName: 'Slack',
  scopes: slackScopes,
  account: {
    ...account,
    automaticRefresh: false,
    // Slack separates its grant with commas, and the page shows it as Slack wrote it.
    grantedScope: [...slackScopes.filter((s) => !s.endsWith(':write')), 'team:read'].join(),
  },
}

const meta = {
  title: 'Reef/Your account',
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: () => <Story />,
          action: () => ({ error: 'This story does not save changes.', credential: null }),
        },
      ])
      return <Stub initialEntries={['/accounts']} />
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

/** A long request, a grant that differs from it, and no automatic refresh. */
export const PartialGrant: Story = { render: () => <UserAccountPage view={slack} /> }
export const FullGrant: Story = { render: () => <UserAccountPage view={connected} /> }
/** A GitHub App ignores OAuth scopes, so Coral asks for none and says who decides access. */
export const NoPermissionsRequested: Story = {
  render: () => (
    <UserAccountPage
      view={{
        ...connected,
        providerName: 'GitHub',
        scopes: [],
        account: { ...account, grantedScope: '' },
      }}
    />
  ),
}
/** A secret has no grant, and this provider asks for no permissions. */
export const ConnectedWithSecret: Story = {
  render: () => (
    <UserAccountPage
      view={{ ...connected, oauth: false, scopes: [], account: { ...account, grantedScope: '' } }}
    />
  ),
}
export const ReconnectNeeded: Story = {
  render: () => (
    <UserAccountPage
      view={{
        ...slack,
        state: 'needs_reconnect',
        returnTo: slack.accountsPath,
        account: { ...slack.account!, state: 'reconnect' },
      }}
    />
  ),
}
export const ConnectFromRun: Story = { render: () => <UserAccountPage view={notConnected} /> }
export const ConnectWithSecret: Story = {
  render: () => <UserAccountPage view={{ ...notConnected, oauth: false, returnTo: null }} />,
}
export const CannotConnect: Story = {
  render: () => <UserAccountPage view={{ ...notConnected, canConnect: false }} />,
}
export const FilterPermissions: Story = {
  render: () => <UserAccountPage view={slack} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const filter = canvas.getByRole('searchbox', { name: 'Filter permissions' })
    const table = canvas.getByRole('table', { name: 'Permissions' })
    await userEvent.type(filter, 'canvases')
    await expect(await within(table).findAllByRole('row')).toHaveLength(3)
    await expect(within(table).getByText('canvases:write')).toBeVisible()
    await userEvent.clear(filter)
    await userEvent.type(filter, 'admin')
    await expect(await within(table).findByText('No permissions match “admin”.')).toBeVisible()
  },
}

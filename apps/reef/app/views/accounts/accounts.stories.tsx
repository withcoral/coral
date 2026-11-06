import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { expect, userEvent, waitFor, within } from 'storybook/test'
import githubIcon from '../../../recipes/github/github.svg'
import githubDarkIcon from '../../../recipes/github/github-dark.svg'
import linearIcon from '../../../recipes/linear/logo.svg'
import linearDarkIcon from '../../../recipes/linear/logo-dark.svg'
import { freezeStoryClock, minutesAfterStoryNow } from '../activity/story-clock'
import { AccountsPage } from './accounts-page'
import { AuthorizationPage } from './authorization-page'
import { UserAccountsPage } from './user-accounts-page'
import type { AccountsView, AuthorizationView, UserAccountsView } from './types'

const form = { csrf: 'storybook', requestId: 'storybook' }
const account = {
  id: 'a129cdf0-f51a-4edb-a4a5-ad0806f6bbf7',
  canManage: true,
  state: 'ready' as const,
  grantedScope: 'read write extra',
  connectedAt: '2026-09-12T12:00:00.000Z',
  configuredByUser: 'u_demo',
}
const parents = [
  { label: 'Providers', to: '/providers' },
  { label: 'Ledger', to: '/providers/ledger' },
]
const view: AccountsView = {
  parents,
  title: 'Account',
  path: '/accounts',
  scope: 'shared',
  account,
  scopes: ['read', 'write'],
  needsCredential: true,
  usesScope: true,
  canConnect: false,
  canReconnect: true,
  active: true,
  route: 'caller',
  form,
}
const authorization: AuthorizationView = {
  providerName: 'Ledger',
  returnTo: '/activity',
  scope: 'user',
  path: '/authorization',
  parents,
  state: 'pending',
  oauth: false,
  grantedScope: '',
  credentialFields: [{ name: 'token', label: 'Token', description: '', sensitive: true }],
  secretStores: [],
  form,
}
const workspace = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: '',
  effectiveRole: 'member',
  currentUser: 'u_demo',
  profile: { user: 'u_demo', name: 'Demo user', email: 'demo@example.test', local: false },
  mcpEndpoint: '',
  connectEndpoint: '',
  path: '/workspaces/demo',
}
const personal: UserAccountsView = {
  workspace,
  path: '/workspaces/demo/accounts',
  accounts: [
    {
      provider: 'workspaces/demo/providers/linear',
      name: 'Linear',
      icons: { light: linearIcon, dark: linearDarkIcon },
      oauth: true,
      state: 'not_connected',
      canConnect: true,
      canDisconnect: false,
      connectionRequest: {
        name: 'workspaces/demo/accountConnectionRequests/linear',
        expiresAt: minutesAfterStoryNow(15),
      },
      connectPath: '/connect',
      detailPath: '/details',
    },
    {
      provider: 'workspaces/demo/providers/github',
      name: 'GitHub',
      icons: { light: githubIcon, dark: githubDarkIcon },
      oauth: true,
      state: 'connected',
      accountId: account.id,
      canConnect: false,
      canDisconnect: true,
      connectPath: '/connect',
      detailPath: '/details',
    },
    {
      provider: 'workspaces/demo/providers/ledger',
      name: 'Ledger',
      oauth: false,
      state: 'needs_reconnect',
      accountId: account.id,
      canConnect: true,
      canDisconnect: true,
      connectPath: '/connect',
      detailPath: '/details',
    },
  ],
  form,
}
const meta = {
  title: 'Reef/Accounts',
  beforeEach: freezeStoryClock,
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: () => <Story />,
          action: () => ({
            error: 'This story does not save changes.',
            credential: null,
          }),
        },
      ])
      return <Stub initialEntries={['/accounts']} />
    },
  ],
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>
export const Caller: Story = { render: () => <AccountsPage view={view} /> }
export const ReconnectRequired: Story = {
  render: () => (
    <AccountsPage
      view={{
        ...view,
        account: { ...account, state: 'reconnect' },
      }}
    />
  ),
}
export const Disconnected: Story = {
  render: () => (
    <AccountsPage
      view={{
        ...view,
        canConnect: true,
        canReconnect: false,
        account: { ...account, state: 'disconnected' },
      }}
    />
  ),
}
export const NeverConnected: Story = {
  render: () => (
    <AccountsPage view={{ ...view, canConnect: true, canReconnect: false, account: undefined }} />
  ),
}
export const UnconnectedRecord: Story = {
  render: () => (
    <AccountsPage
      view={{
        ...view,
        canConnect: true,
        canReconnect: false,
        account: {
          ...account,
          state: 'disconnected',
          connectedAt: '',
          configuredByUser: '',
          grantedScope: '',
        },
      }}
    />
  ),
}
export const SharedOwner: Story = {
  render: () => (
    <AccountsPage
      view={{
        ...view,
        scope: 'shared',
        route: 'fixed',
        account,
      }}
    />
  ),
}
export const Archived: Story = {
  render: () => (
    <AccountsPage
      view={{
        ...view,
        active: false,
        canConnect: false,
        canReconnect: false,
        account: { ...account, canManage: false },
      }}
    />
  ),
}
export const Agent: Story = {
  render: () => (
    <AccountsPage
      view={{
        ...view,
        parents: [
          { label: 'Agents', to: '/agents' },
          { label: 'Report bot', to: '/agents/report-bot' },
        ],
        title: 'Ledger account',
        canConnect: true,
        canReconnect: false,
        scope: 'agent',
        account: undefined,
      }}
    />
  ),
}
export const SecretEntry: Story = { render: () => <AuthorizationPage view={authorization} /> }

const sharedSecretEntry: AuthorizationView = {
  ...authorization,
  scope: 'shared',
  secretStores: ['platform', 'research'],
}
// Catches: the typed input submitted beside a reference, or a store never chosen.
export const SecretEntryWithSecretStore: Story = {
  render: () => <AuthorizationPage view={sharedSecretEntry} />,
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole('radio', { name: 'Use a secret store' }))
    await expect(canvas.queryByRole('textbox', { name: /token/i })).toBeNull()
    await expect(canvas.getByRole('textbox', { name: 'Path' })).toBeRequired()
    await expect(canvas.getByRole('textbox', { name: 'Key' })).toBeRequired()
    // A store is always chosen, so the form cannot post without one.
    await expect(canvas.getByRole('button', { name: 'Secret store' })).toHaveTextContent('platform')
  },
}
export const SecretEntryWithSecretStoreProblem: Story = {
  render: () => (
    <AuthorizationPage
      view={{
        ...sharedSecretEntry,
        secretStoreProblem: 'The secret store has no entry at this path. Check the path.',
      }}
    />
  ),
  play: async ({ canvas }) => {
    await expect(canvas.getByText(/no entry at this path/)).toBeVisible()
  },
}
export const OAuthPending: Story = {
  render: () => <AuthorizationPage view={{ ...authorization, oauth: true }} />,
}
export const DeviceSignIn: Story = {
  render: () => (
    <AuthorizationPage view={{ ...authorization, oauth: true, userCode: 'ABCD-EFGH' }} />
  ),
}
export const Exchanging: Story = {
  render: () => <AuthorizationPage view={{ ...authorization, state: 'exchanging' }} />,
}
export const RejectedSignInCode: Story = {
  render: () => (
    <AuthorizationPage
      view={{ ...authorization, state: 'failed', failure: 'oauth_code_rejected' }}
    />
  ),
}
export const ReportedGrant: Story = {
  render: () => (
    <AuthorizationPage
      view={{ ...authorization, state: 'succeeded', grantedScope: account.grantedScope }}
    />
  ),
}
export const Expired: Story = {
  render: () => <AuthorizationPage view={{ ...authorization, state: 'expired' }} />,
}
export const YourAccounts: Story = { render: () => <UserAccountsPage view={personal} /> }
export const FilterYourAccounts: Story = {
  render: () => <UserAccountsPage view={personal} />,
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const page = within(canvasElement.ownerDocument.body)
    // The filter applies through a deferred value, so the rows can lag a render.
    const providers = (expected: string[]) =>
      waitFor(() =>
        expect(
          canvas
            .getAllByRole('link', { name: / account details$/ })
            .map((link) => link.textContent),
        ).toEqual(expected),
      )

    await userEvent.click(
      canvas.getByRole('button', { name: 'Filter accounts by status: All statuses' }),
    )
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'Needs reconnect' }))
    await providers(['Ledger'])

    await userEvent.type(canvas.getByRole('searchbox', { name: 'Filter accounts' }), 'git')
    await expect(await canvas.findByText('No accounts match “git”.')).toBeVisible()

    // The button's name says which status the table shows.
    await userEvent.click(
      canvas.getByRole('button', { name: 'Filter accounts by status: Needs reconnect' }),
    )
    await userEvent.click(await page.findByRole('menuitemradio', { name: 'All statuses' }))
    await providers(['GitHub'])
  },
}
/** The table keeps its shape with no accounts, and points to the providers. */
export const NoSeparateAccounts: Story = {
  render: () => <UserAccountsPage view={{ ...personal, accounts: [] }} />,
  play: async ({ canvas }) => {
    const table = within(canvas.getByRole('table', { name: 'Accounts' }))
    await expect(table.getByText('No accounts yet')).toBeVisible()
    await expect(table.getByRole('link', { name: 'View providers' })).toHaveAttribute(
      'href',
      `${personal.workspace.path}/providers`,
    )
  },
}
export const WithoutAutomaticRefresh: Story = {
  render: () => (
    <AccountsPage view={{ ...view, account: { ...account, automaticRefresh: false } }} />
  ),
}
export const ApplicationNeedsRepair: Story = {
  render: () => (
    <AuthorizationPage
      view={{ ...authorization, oauth: true, state: 'failed', applicationFailure: true }}
    />
  ),
}

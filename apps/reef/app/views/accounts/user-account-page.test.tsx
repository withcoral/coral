/** @vitest-environment happy-dom */
import { expect, it } from 'vitest'
import { markupRoute } from '@/test-support/route'
import { workspaceView } from '@/views/providers/fixtures'
import { UserAccountPage } from './user-account-page'
import type { UserAccountView } from './types'

const view: UserAccountView = {
  workspace: { ...workspaceView, effectiveRole: 'member' },
  providerName: 'Linear',
  path: '/workspaces/w/accounts/p?returnTo=%2Fworkspaces%2Fw%2Fruns%2Fr',
  returnTo: '/workspaces/w/runs/r',
  accountsPath: '/workspaces/w/accounts',
  state: 'not_connected',
  scopes: ['read'],
  canConnect: true,
  canDisconnect: false,
  oauth: true,
  credentialFields: [{ name: 'token', label: 'Token', description: '', sensitive: true }],
  form: { csrf: 'test', requestId: 'test-request' },
}
const connected: Partial<UserAccountView> = {
  state: 'connected',
  canDisconnect: true,
  account: {
    id: 'account',
    canManage: true,
    state: 'ready',
    grantedScope: 'read write',
    connectedAt: '2026-09-24T12:00:00.000Z',
    configuredByUser: 'u',
  },
}
function render(changes: Partial<UserAccountView>) {
  return markupRoute(<UserAccountPage view={{ ...view, ...changes }} />)
}

it('signs in to the provider with one button and keeps the run to return to', () => {
  const html = render({})
  // The breadcrumb titles the page; the content adds no heading of its own.
  expect(html.match(/<h1[^>]*>[^<]*<\/h1>/g)).toEqual([expect.stringContaining('>Linear</h1>')])
  expect(html).toContain('>Connect Linear<')
  expect(html).toContain('name="intent" value="connect"')
  expect(html).not.toContain('name="credential.token"')
  expect(render({ oauth: false })).toContain('name="intent" value="secret"')
  expect(html).toContain('href="/workspaces/w/runs/r"')
  expect(html).toContain('Requested permissions: read')
})

it('manages a connected account here: granted permissions, reconnect, and disconnect', () => {
  const html = render({ ...connected, returnTo: null })
  expect(html).toContain('Granted permissions: read write')
  expect(html).not.toContain('Configured by Coral user')
  expect(html).toContain('>Reconnect<')
  expect(html).toContain('>Disconnect<')
  expect(html).not.toContain('Account connected.')
})

it('does not offer the Accounts page twice as a run to return to', () => {
  const html = render({ state: 'needs_reconnect', returnTo: view.accountsPath })
  expect(html).toContain('>Reconnect Linear<')
  expect(html).not.toContain('Return to run')
})

it('removes credentials that need reconnecting and replays a repeated connect', () => {
  const html = render({
    ...connected,
    state: 'needs_reconnect',
    account: { ...connected.account!, state: 'reconnect' },
  })
  expect(html).toContain('>Reconnect Linear<')
  expect(html).toContain('>Disconnect<')
  expect(html).toContain('name="requestId" value="test-request"')
})

/** @vitest-environment happy-dom */
import { expect, it } from 'vitest'
import { screen } from '@testing-library/react'
import { markupRoute, renderRoute } from '@/test-support/route'
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
  expect(html).toContain('>Linear account</h2>')
  expect(html).toContain('Sign in to Linear to connect it.')
  expect(html).toContain('>Connect<')
  expect(html).toContain('name="intent" value="connect"')
  expect(html).not.toContain('name="credential.token"')
  expect(render({ oauth: false })).toContain('name="intent" value="secret"')
  expect(html).toContain('href="/workspaces/w/runs/r"')
  expect(html).toContain('Coral asks for 1 permission.')
  expect(html).toContain('>read<')
})

it('manages a connected account here: the reported grant, reconnect, and disconnect', () => {
  // The grant leaves out "admin", but services name scopes in their own ways.
  const html = render({ ...connected, scopes: ['read', 'admin'], returnTo: null })
  // The grant reads as the service reported it, apart from the request: no comparison.
  expect(html).toContain('Coral asks for 2 permissions.')
  expect(html).toMatch(/>Granted permissions<\/dt><dd[^>]*><span[^>]*>read write<\/span>/)
  expect(html).not.toContain('Not granted')
  expect(html).not.toContain('Configured by Coral user')
  expect(html).toContain('>Reconnect<')
  expect(html).toContain('>Disconnect<')
  expect(html).not.toContain('Account connected.')
})

it('does not offer the Accounts page twice as a run to return to', () => {
  const html = render({ state: 'needs_reconnect', returnTo: view.accountsPath })
  expect(html).toContain('>Reconnect<')
  expect(html).not.toContain('Return to run')
})

it('removes credentials that need reconnecting and replays a repeated connect', () => {
  const html = render({
    ...connected,
    state: 'needs_reconnect',
    account: { ...connected.account!, state: 'reconnect' },
  })
  expect(html).toContain('Coral cannot use this account. Sign in to Linear to reconnect it.')
  expect(html).toContain('>Reconnect<')
  expect(html).toContain('>Disconnect<')
  expect(html).toContain('name="requestId" value="test-request"')
})

it.each([
  ['OAuth explains an empty request', true, true],
  ['a secret keeps quiet', false, false],
])('with no requested permissions, %s', (_, oauth, shown) => {
  const html = render({
    ...connected,
    oauth,
    scopes: [],
    account: { ...connected.account!, grantedScope: '' },
  })
  // The section alone: no table follows an empty request.
  expect(html.split('aria-label="Permissions"').length - 1).toBe(shown ? 1 : 0)
  expect(
    html.includes('Coral asks for no permissions. Linear decides what this account can access.'),
  ).toBe(shown)
})

// Catches: a provider's settings sending you here with a button that says it
// returns to a run.
it.each([
  ['a run', '/workspaces/w/runs/r', 'Return to run'],
  ['provider settings', '/workspaces/w/providers/p/settings', 'Return to settings'],
])('names the way back to %s', (_, returnTo, label) => {
  const html = render({ returnTo })
  expect(html).toContain(`>${label}<`)
  expect(html).toContain(`href="${returnTo}"`)
})

// Catches: a secret reconnect leaving this page for a separate connection page.
it.each([
  ['a secret in a dialog', false, 'dialog'],
  ['OAuth in one click', true, null],
])('reconnects %s', (_, oauth, popup) => {
  renderRoute(<UserAccountPage view={{ ...view, ...connected, oauth }} />)
  const reconnect = screen.getByRole('button', { name: 'Reconnect' })
  expect(reconnect.getAttribute('aria-haspopup')).toBe(popup)
  expect(reconnect.closest('form') !== null).toBe(oauth)
})

/** @vitest-environment happy-dom */

import { screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { markupRoute, renderRoute } from '@/test-support/route'
import { AuthorizationPage } from './authorization-page'
import type { AuthorizationView } from './types'

const view: AuthorizationView = {
  providerName: 'GitHub',
  scope: 'user',
  path: '/workspaces/w/account-authorizations/original?returnTo=%2Fworkspaces%2Fw%2Factivity%3Frequest%3Doriginal',
  returnTo: '/workspaces/w/activity?request=original',
  parents: [],
  state: 'pending',
  oauth: false,
  grantedScope: '',
  credentialFields: [{ name: 'token', label: 'Token', description: '', sensitive: true }],
  form: { csrf: 'test', requestId: 'test-request' },
}
function render(changes: Partial<AuthorizationView> = {}) {
  return markupRoute(<AuthorizationPage view={{ ...view, ...changes }} />)
}

it('names the provider while you enter credentials', () => {
  const html = render()
  // The breadcrumb titles the page; the content adds no heading of its own.
  expect(html.match(/<h1[^>]*>[^<]*<\/h1>/g)).toEqual([
    expect.stringContaining('>Connect account</h1>'),
  ])
  expect(html).toContain('Enter the credentials from GitHub.')
})

it.each([false, true])(
  'keeps the validated journey target on every secret/OAuth form (OAuth: %s)',
  (oauth) => {
    const html = render({ oauth })
    const actions = [...html.matchAll(/<form[^>]*action="([^"]+)"/g)].map((match) => match[1])
    expect(actions).toEqual([view.path, view.path])
  },
)

it.each([false, true])(
  'offers only the main action and Cancel while the connection is open (OAuth: %s)',
  (oauth) => {
    // Catches: a way back that leaves the connection open behind you.
    renderRoute(<AuthorizationPage view={{ ...view, oauth }} />)
    screen.getByRole('button', { name: oauth ? 'Continue to sign in' : 'Connect account' })
    const cancel = screen.getByRole<HTMLButtonElement>('button', { name: 'Cancel' })
    expect(new FormData(cancel.form!).get('intent')).toBe('cancel')
    expect(screen.queryByRole('link', { name: 'Check connection' })).toBeNull()
    expect(screen.queryByRole('link', { name: 'Return to run' })).toBeNull()
  },
)

it.each(['succeeded', 'failed', 'expired', 'canceled'] as const)(
  'offers the original Run after authorization is %s, not a new request',
  (state) => {
    const html = render({ state })
    expect(html).toContain('href="/workspaces/w/activity?request=original"')
    expect(html).toContain('Return to run')
    expect(html).not.toContain('start another connection')
    expect(html).not.toContain('type="submit"')
    if (state === 'succeeded') {
      expect(html).toContain('Account connected.')
      expect(html).toContain('Your run may still need approval.')
    }
  },
)

it('keeps direct account settings entry usable without inventing a Run', () => {
  const html = render({ state: 'succeeded', returnTo: '/workspaces/w/activity' })
  expect(html).toContain('href="/workspaces/w/activity"')
  expect(html).toContain('Back to activity')
  expect(html).not.toContain('Return to run')
  expect(html).not.toContain('Your run may still need approval.')
})

// Catches: a connection started outside a run offering "Return to run".
it.each([
  ['settings', '/workspaces/w/providers/p/settings', 'Return to settings'],
  ['Accounts', '/workspaces/w/accounts', 'Return to accounts'],
  ['your account page', '/workspaces/w/accounts/p', 'Return to account'],
])('names %s, not a run, as the way back', (_, returnTo, label) => {
  renderRoute(<AuthorizationPage view={{ ...view, state: 'expired', returnTo }} />)
  expect(screen.getByRole('link', { name: label }).getAttribute('href')).toBe(returnTo)
  expect(screen.queryByText(/your run/)).toBeNull()
})

it.each(['failed', 'expired', 'canceled'] as const)(
  'offers another attempt from your account page after a %s sign-in',
  (state) => {
    const html = render({ state, retryPath: '/workspaces/w/accounts/p' })
    expect(html).toMatch(/<a[^>]*href="\/workspaces\/w\/accounts\/p"[^>]*>.*Try again/s)
    expect(render({ state })).not.toContain('href="/workspaces/w/accounts/p"')
  },
)

it.each([
  ['a device sign-in', 'ABCD-EFGH', 'Open sign-in page', '_blank'],
  ['a redirect sign-in', undefined, 'Continue to sign in', null],
] as const)('keeps this page open during %s', (_name, userCode, action, target) => {
  // Only a device sign-in never returns here, so only it opens in a new tab.
  renderRoute(<AuthorizationPage view={{ ...view, oauth: true, userCode }} />)
  const button = screen.getByRole<HTMLButtonElement>('button', { name: action })
  expect(button.form!.getAttribute('target')).toBe(target)
})

it.each([
  ['an issued code', 'ABCD-EFGH', true],
  ['a code still on its way', '', false],
] as const)('offers the device sign-in only for %s', (_name, userCode, ready) => {
  // Without a code there is nothing to copy and nothing to enter on the sign-in page.
  renderRoute(<AuthorizationPage view={{ ...view, oauth: true, userCode }} />)
  expect(
    screen.getByRole<HTMLButtonElement>('button', { name: 'Copy sign-in code' }).disabled,
  ).toBe(!ready)
  expect(
    screen.getByRole<HTMLButtonElement>('button', { name: 'Open sign-in page' }).disabled,
  ).toBe(!ready)
})

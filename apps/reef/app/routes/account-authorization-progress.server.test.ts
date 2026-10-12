import { Code, ConnectError } from '@connectrpc/connect'
import { redirect } from 'react-router'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import type { AuthorizationView } from '@/views/accounts/types'
import { loader } from './account-authorization-progress'

const loadAuthorization = vi.hoisted(() => vi.fn())
vi.mock('@/lib/accounts.server', async (original) => ({
  ...(await original<typeof import('@/lib/accounts.server')>()),
  loadAuthorization,
}))

const workspace = '00000000-0000-4000-8000-000000000001'
const authorization = '00000000-0000-4000-8000-000000000002'
const load = () =>
  loader(
    authRouteTestArgs(
      new Request(
        `http://reef/workspaces/${workspace}/account-authorizations/${authorization}/progress`,
      ),
      { workspace, authorization },
      null,
    ),
  )

beforeEach(() => {
  loadAuthorization.mockReset()
})

it('returns the authorization view while it is in progress', async () => {
  const view = { state: 'pending' } as AuthorizationView
  loadAuthorization.mockResolvedValue(view)
  expect((await load()).data).toEqual({ view })
})

// Catches: a finished sign-in redirecting the page under the dialog, or a failed
// check reaching the page's error boundary.
it.each([
  ['a succeeded authorization', redirect('/workspaces/w/agents/a'), { succeeded: true }],
  [
    'a Coral outage',
    new ConnectError('down', Code.Unavailable),
    { error: 'Coral could not check the connection. Coral is not available.' },
  ],
  ['a lapsed sign-in', new ConnectError('signed out', Code.Unauthenticated), { signedOut: true }],
  [
    'a malformed authorization',
    new Response('Invalid authorization', { status: 503 }),
    { error: 'Coral could not check the connection.' },
  ],
])('answers %s as data', async (_case, thrown, answer) => {
  loadAuthorization.mockRejectedValue(thrown)
  expect((await load()).data).toEqual(answer)
})

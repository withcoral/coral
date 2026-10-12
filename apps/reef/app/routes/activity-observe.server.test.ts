import { expect, it } from 'vitest'

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { requestAuthContext } from '@/auth/server-context'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import { action as countAction } from './activity-count'
import { action } from './activity-observe'

it.each([undefined, 'https://other.test'])(
  'rejects missing or foreign browser origins before allocating an observation',
  async (origin) => {
    const request = new Request('http://reef.test/activity/observe', {
      method: 'POST',
      headers: origin ? { origin } : undefined,
      body: '{}',
    })
    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({
      status: 403,
    })
  },
)
it('rejects non-POST observations even from the same origin', async () => {
  await expect(
    action(
      authRouteTestArgs(
        new Request('http://reef.test/activity/observe', {
          method: 'PUT',
          headers: { origin: 'http://reef.test' },
        }),
        {},
      ),
    ),
  ).rejects.toMatchObject({ status: 405 })
})
it('accepts the configured HTTPS browser origin behind an HTTP reverse proxy', async () => {
  // TLS ends at the proxy, so Reef sees plain HTTP from the browser's HTTPS origin.
  const { publicUrl } = requiredAuthConfig
  const request = new Request(`http://${new URL(publicUrl).host}/activity/observe`, {
    method: 'POST',
    headers: { origin: publicUrl },
    body: JSON.stringify({
      path: '/workspaces/00000000-0000-4000-8000-000000000001/activity',
      search: '',
      window: { roots: [], followRoot: false, branches: {} },
    }),
  })
  const args = authRouteTestArgs(request, {}, 'expired-dummy-token', hostedRuntimeConfig)
  args.context.set(requestAuthContext, {
    mode: 'required',
    accessToken: 'expired-dummy-token',
    csrfToken: '',
    session: {
      accessToken: 'expired-dummy-token',
      expiresAt: 0,
      tokenType: 'Bearer',
    },
  })
  const response = await action(args)
  expect(response.status).toBe(200)
  expect(await response.text()).toContain('sessionExpired')
})
// The stream owns expiry (activity-stream.server.test.ts); each route only hands it the deadline.
it.each([
  ['count', countAction, '/activity/count'],
  ['observe', action, '/activity/observe'],
] as const)('passes the session deadline to the %s stream', async (_, route, path) => {
  const request = new Request(`http://reef.test${path}`, {
    method: 'POST',
    headers: { origin: 'http://reef.test' },
    body: JSON.stringify({
      path: '/workspaces/00000000-0000-4000-8000-000000000001/activity',
      search: '',
      window: { roots: [], followRoot: false, branches: {} },
    }),
  })
  const args = authRouteTestArgs(request, {})
  args.context.set(requestAuthContext, {
    mode: 'required',
    accessToken: 'expired-dummy-token',
    csrfToken: '',
    session: {
      accessToken: 'expired-dummy-token',
      expiresAt: 0,
      tokenType: 'Bearer',
    },
  })
  const response = await route(args)
  expect(await response.text()).toContain('"kind":"sessionExpired"')
})

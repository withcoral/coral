import { expect, it } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { action } from './activity-count'

it.each([undefined, 'https://other.test'])(
  'rejects missing or foreign origins before allocating count observation',
  (origin) => {
    const request = new Request('http://reef.test/activity/count', {
      method: 'POST',
      headers: origin ? { origin } : undefined,
    })
    expect(() => action(authRouteTestArgs(request, {}))).toThrow(
      expect.objectContaining({ status: 403 }),
    )
  },
)
it('rejects non-POST requests', () => {
  expect(() =>
    action(authRouteTestArgs(new Request('http://reef.test/activity/count'), {})),
  ).toThrow(expect.objectContaining({ status: 405 }))
})

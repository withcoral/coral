import { describe, expect, it } from 'vitest'

import { AUTH_STREAM_REQUEST_HEADER, AUTH_STREAM_RETURN_TO_HEADER } from './response'
import {
  authPrivateHeaders,
  expiredSessionRedirect,
  markAuthResponsePrivate,
} from './response.server'

// The two are one policy with two shapes: routes that build their own response
// declare the headers, routes handed one mark it. They were separate literals,
// which is the arrangement where a header added to one quietly misses the other
// and nothing fails. This pins them to each other rather than to a copy of the
// expected values, so it keeps holding when the policy changes.
describe('authPrivateHeaders', () => {
  it('declares exactly what marking a response applies', () => {
    const declared = [...authPrivateHeaders().entries()]
    const marked = markAuthResponsePrivate(new Response('body')).headers

    // Every header the declaring form promises is one the marking form really
    // sets, with the same value. Compared this way rather than as whole header
    // sets, because a response with a body also carries a `content-type` that
    // has nothing to do with this policy.
    expect(declared.length).toBeGreaterThan(0)
    for (const [name, value] of declared) {
      expect(marked.get(name), `${name} should match the marked response`).toBe(value)
    }
  })

  it('still says private and varies on the session cookie', () => {
    const headers = authPrivateHeaders()

    expect(headers.get('Cache-Control')).toBe('private, no-store')
    expect(headers.get('Vary')).toBe('Cookie')
  })
})

describe('markAuthResponsePrivate', () => {
  it('marks a bare response private and varying on the session cookie', () => {
    const response = markAuthResponsePrivate(new Response('body'))

    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toBe('Cookie')
  })

  it('preserves an existing Vary list while adding the session cookie', () => {
    const response = markAuthResponsePrivate(
      new Response('body', { headers: { Vary: 'Accept-Encoding' } }),
    )

    expect(response.headers.get('Vary')).toBe('Accept-Encoding, Cookie')
  })

  it.each(['Cookie', 'cookie', 'Accept-Encoding, Cookie'])(
    'does not repeat a cookie that is already declared: %s',
    (vary) => {
      const response = markAuthResponsePrivate(new Response('body', { headers: { Vary: vary } }))

      expect(response.headers.get('Vary')).toBe(vary)
    },
  )

  // `*` is not a field name the list can be extended with: it already says the
  // response is unique to its request, which subsumes `Cookie`. Appending would
  // emit `*, Cookie`, which is not a valid `Vary` value.
  it.each(['*', ' * '])('leaves a wildcard Vary alone: %s', (vary) => {
    const response = markAuthResponsePrivate(new Response('body', { headers: { Vary: vary } }))

    expect(response.headers.get('Vary')).toBe(vary.trim())
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })
})

describe('expiredSessionRedirect', () => {
  it('uses a validated visible-page return location for auth stream fetches', () => {
    const response = expiredSessionRedirect(
      new Request('https://reef.example.test/workspaces/analytics/sources/oauth-import', {
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: '/workspaces/analytics/sources/new?step=oauth#method',
        },
      }),
    )

    expect(response.headers.get('location')).toBe(
      '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew%3Fstep%3Doauth%23method',
    )
  })

  it.each([
    'https://attacker.example/return',
    '//attacker.example/return',
    // Every case above is off-origin as written. These become off-origin only
    // once `URL` normalizes them, which is the class this guard used to miss.
    '/..//attacker.example/return',
    '/./..//attacker.example',
    '/workspaces/../..//attacker.example',
    '',
    `/${'a'.repeat(2048)}`,
  ])('rejects an unsafe auth stream return location: %s', (returnTo) => {
    const response = expiredSessionRedirect(
      new Request('https://reef.example.test/workspaces/analytics/sources/oauth-import', {
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: returnTo,
        },
      }),
    )

    expect(response.headers.get('location')).toBe('/login?returnTo=%2F')
  })
})

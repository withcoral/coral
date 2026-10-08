import { describe, expect, it } from 'vitest'

import { AUTH_STREAM_REQUEST_HEADER, AUTH_STREAM_RETURN_TO_HEADER } from './response'
import {
  authPrivateHeaders,
  expiredSessionRedirect,
  markAuthResponsePrivate,
} from './response.server'

describe('authPrivateHeaders', () => {
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

  it.each([
    ['Accept-Encoding', ['accept-encoding', 'cookie']],
    ['Cookie', ['cookie']],
    ['cookie', ['cookie']],
    ['Accept-Encoding, Cookie', ['accept-encoding', 'cookie']],
  ])('adds the session cookie to Vary exactly once: %s', (vary, expectedFields) => {
    const response = markAuthResponsePrivate(new Response('body', { headers: { Vary: vary } }))

    expect(varyFields(response)).toEqual(expectedFields)
  })

  // `*` is not a field name the list can be extended with: it already says the
  // response is unique to its request, which subsumes `Cookie`. Appending would
  // emit `*, Cookie`, which is not a valid `Vary` value.
  it.each(['*', ' * '])('leaves a wildcard Vary alone: %s', (vary) => {
    const response = markAuthResponsePrivate(new Response('body', { headers: { Vary: vary } }))

    expect(response.headers.get('Vary')).toBe(vary.trim())
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })
})

function varyFields(response: Response): string[] {
  return (
    response.headers
      .get('Vary')
      ?.split(',')
      .map((field) => field.trim().toLowerCase()) ?? []
  )
}

describe('expiredSessionRedirect', () => {
  it('uses a validated visible-page return location for auth stream fetches', () => {
    const response = expiredSessionRedirect(
      new Request('https://coral-ui.example.test/workspaces/analytics/sources/oauth-import', {
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
      new Request('https://coral-ui.example.test/workspaces/analytics/sources/oauth-import', {
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: returnTo,
        },
      }),
    )

    expect(response.headers.get('location')).toBe('/login?returnTo=%2F')
  })
})

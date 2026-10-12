import { describe, expect, it } from 'vitest'

import { authPrivateHeaders, markAuthResponsePrivate } from './response.server'

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

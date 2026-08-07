import { describe, expect, it } from 'vitest'

import { markAuthResponsePrivate } from './response.server'

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

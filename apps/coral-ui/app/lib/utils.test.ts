import { describe, expect, it } from 'vitest'

import { errorMessage } from './utils'

describe('errorMessage', () => {
  it('preserves route responses instead of turning redirects into error text', () => {
    const redirect = new Response(null, { headers: { location: '/login' }, status: 302 })

    expect(() => errorMessage(redirect)).toThrow(redirect)
  })

  it('formats ordinary errors', () => {
    expect(errorMessage(new Error('Coral unavailable'))).toBe('Coral unavailable')
  })
})

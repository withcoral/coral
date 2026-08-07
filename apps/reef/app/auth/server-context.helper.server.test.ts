import { describe, expect, it } from 'vitest'

import { requestAuthContext } from './server-context'
import { authTestContext } from './server-context.test-helper'

// A test helper that quietly answers a different question than it was asked is
// worse than no helper: every suite built on it keeps passing while proving
// something else. `null` means "this deployment has auth disabled"; anything
// else is a token, including one that is empty because a caller lost it.
describe('authTestContext', () => {
  it('builds a disabled-auth context only for a null token', () => {
    const context = authTestContext(null)

    expect(context.get(requestAuthContext)).toEqual({ accessToken: null, mode: 'disabled' })
  })

  it.each([
    ['an empty token', ''],
    ['a whitespace token', ' '],
  ])('keeps %s in a required-auth context', (_label, accessToken) => {
    const auth = authTestContext(accessToken).get(requestAuthContext)

    expect(auth.mode).toBe('required')
    expect(auth.accessToken).toBe(accessToken)
  })

  it('defaults to a required-auth context with a token', () => {
    const auth = authTestContext().get(requestAuthContext)

    expect(auth.mode).toBe('required')
    expect(auth.accessToken).toBe('test-coral-token')
  })
})

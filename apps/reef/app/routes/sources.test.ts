import type { ShouldRevalidateFunctionArgs } from 'react-router'
import { describe, expect, it } from 'vitest'

import { shouldRevalidate } from './sources'

function revalidationArgs(
  overrides: Partial<ShouldRevalidateFunctionArgs>,
): ShouldRevalidateFunctionArgs {
  return {
    currentParams: {},
    currentUrl: new URL('http://localhost/sources'),
    defaultShouldRevalidate: true,
    nextParams: {},
    nextUrl: new URL('http://localhost/sources/github'),
    ...overrides,
  }
}

describe('sources shouldRevalidate', () => {
  it('skips parent catalog reloads for normal source detail navigation', () => {
    expect(
      shouldRevalidate(
        revalidationArgs({
          currentUrl: new URL('http://localhost/sources'),
          defaultShouldRevalidate: true,
          nextUrl: new URL('http://localhost/sources/github'),
        }),
      ),
    ).toBe(false)
  })

  it('revalidates the catalog after source mutations even when the action redirects to /sources', () => {
    expect(
      shouldRevalidate(
        revalidationArgs({
          currentUrl: new URL('http://localhost/sources/github'),
          defaultShouldRevalidate: false,
          formMethod: 'POST',
          nextUrl: new URL('http://localhost/sources'),
        }),
      ),
    ).toBe(true)
  })
})

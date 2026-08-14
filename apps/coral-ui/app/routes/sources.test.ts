import type { ShouldRevalidateFunctionArgs } from 'react-router'
import { describe, expect, it } from 'vitest'

import { shouldRevalidate } from './sources'

function revalidationArgs(
  overrides: Partial<ShouldRevalidateFunctionArgs>,
): ShouldRevalidateFunctionArgs {
  return {
    currentParams: {},
    currentUrl: new URL('http://localhost/workspaces/default/sources'),
    defaultShouldRevalidate: true,
    nextParams: {},
    nextUrl: new URL('http://localhost/workspaces/default/sources/github'),
    ...overrides,
  }
}

describe('sources shouldRevalidate', () => {
  it('skips parent catalog reloads for normal source detail navigation', () => {
    expect(
      shouldRevalidate(
        revalidationArgs({
          currentUrl: new URL('http://localhost/workspaces/default/sources'),
          defaultShouldRevalidate: true,
          nextUrl: new URL('http://localhost/workspaces/default/sources/github'),
        }),
      ),
    ).toBe(false)
  })

  it('revalidates the catalog after source mutations', () => {
    expect(
      shouldRevalidate(
        revalidationArgs({
          currentUrl: new URL('http://localhost/workspaces/default/sources/github'),
          defaultShouldRevalidate: false,
          formMethod: 'POST',
          nextUrl: new URL('http://localhost/workspaces/default/sources'),
        }),
      ),
    ).toBe(true)
  })

  it('revalidates when navigating between workspaces', () => {
    expect(
      shouldRevalidate(
        revalidationArgs({
          currentUrl: new URL('http://localhost/workspaces/default/sources/github'),
          defaultShouldRevalidate: true,
          nextUrl: new URL('http://localhost/workspaces/analytics/sources/github'),
        }),
      ),
    ).toBe(true)
  })
})

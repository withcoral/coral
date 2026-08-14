import type { ShouldRevalidateFunctionArgs } from 'react-router'
import { describe, expect, it } from 'vitest'

import { shouldRevalidate } from './traces'
import { traceLocation } from '@/views/traces/trace-location'
import { routePath } from '@/routing/routemap'

const WORKSPACE_ID = 'analytics'
const TRACES_PATH = routePath('workspaceTraces', { workspaceId: WORKSPACE_ID })

function revalidationArgs(
  currentPath: string,
  nextPath: string,
  currentTraceId?: string,
  nextTraceId?: string,
  overrides: Partial<ShouldRevalidateFunctionArgs> = {},
): ShouldRevalidateFunctionArgs {
  return {
    currentParams: {
      workspaceId: WORKSPACE_ID,
      ...(currentTraceId === undefined ? {} : { traceId: currentTraceId }),
    },
    currentUrl: new URL(`http://localhost${currentPath}`),
    defaultShouldRevalidate: true,
    nextParams: {
      workspaceId: WORKSPACE_ID,
      ...(nextTraceId === undefined ? {} : { traceId: nextTraceId }),
    },
    nextUrl: new URL(`http://localhost${nextPath}`),
    ...overrides,
  }
}

describe('traces shouldRevalidate', () => {
  it.each([
    [TRACES_PATH, `${TRACES_PATH}/trace-a`, undefined, 'trace-a'],
    [`${TRACES_PATH}/trace-a`, `${TRACES_PATH}/trace-b`, 'trace-a', 'trace-b'],
  ])(
    'skips the parent loader for %s -> %s',
    (currentPath, nextPath, currentTraceId, nextTraceId) => {
      expect(
        shouldRevalidate(revalidationArgs(currentPath, nextPath, currentTraceId, nextTraceId)),
      ).toBe(false)
    },
  )

  it('revalidates the parent loader when returning from detail to the list', () => {
    expect(
      shouldRevalidate(
        revalidationArgs(`${TRACES_PATH}/trace-b`, TRACES_PATH, 'trace-b', undefined, {
          defaultShouldRevalidate: false,
        }),
      ),
    ).toBe(true)
  })

  it('delegates same-URL and unrelated navigation to React Router', () => {
    expect(
      shouldRevalidate(
        revalidationArgs(TRACES_PATH, TRACES_PATH, undefined, undefined, {
          defaultShouldRevalidate: false,
        }),
      ),
    ).toBe(false)
    expect(
      shouldRevalidate(
        revalidationArgs(TRACES_PATH, '/workspaces/analytics/sources', undefined, undefined, {
          defaultShouldRevalidate: true,
        }),
      ),
    ).toBe(true)
    expect(
      shouldRevalidate(
        revalidationArgs(
          `${TRACES_PATH}/trace-a`,
          '/workspaces/other/traces/trace-a',
          'trace-a',
          'trace-a',
          {
            currentParams: { traceId: 'trace-a', workspaceId: WORKSPACE_ID },
            defaultShouldRevalidate: false,
            nextParams: { traceId: 'trace-a', workspaceId: 'other' },
          },
        ),
      ),
    ).toBe(true)
    expect(
      shouldRevalidate(
        revalidationArgs(
          `${TRACES_PATH}/trace-a?search=one`,
          `${TRACES_PATH}/trace-a?search=two`,
          'trace-a',
          'trace-a',
          {
            defaultShouldRevalidate: false,
          },
        ),
      ),
    ).toBe(false)
  })

  it('never suppresses non-GET revalidation', () => {
    expect(
      shouldRevalidate(
        revalidationArgs(`${TRACES_PATH}/trace-a`, TRACES_PATH, 'trace-a', undefined, {
          defaultShouldRevalidate: false,
          formMethod: 'POST',
        }),
      ),
    ).toBe(true)
  })
})

describe('trace locations', () => {
  it('preserves search and pro state in encoded adjacent trace locations', () => {
    expect(traceLocation(WORKSPACE_ID, 'trace/with?reserved', '?search=playwright&pro')).toEqual({
      pathname: '/workspaces/analytics/traces/trace%2Fwith%3Freserved',
      search: '?search=playwright&pro',
    })
  })
})

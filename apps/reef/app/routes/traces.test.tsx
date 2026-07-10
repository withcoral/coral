import {
  createMemoryRouter,
  Link,
  Outlet,
  RouterProvider,
  type ShouldRevalidateFunctionArgs,
  useLoaderData,
  useNavigate,
} from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import { shouldRevalidate } from './traces'
import { traceLocation } from '@/views/traces/trace-location'

function revalidationArgs(
  currentPath: string,
  nextPath: string,
  currentTraceId?: string,
  nextTraceId?: string,
  overrides: Partial<ShouldRevalidateFunctionArgs> = {},
): ShouldRevalidateFunctionArgs {
  return {
    currentParams: currentTraceId === undefined ? {} : { traceId: currentTraceId },
    currentUrl: new URL(`http://localhost${currentPath}`),
    defaultShouldRevalidate: true,
    nextParams: nextTraceId === undefined ? {} : { traceId: nextTraceId },
    nextUrl: new URL(`http://localhost${nextPath}`),
    ...overrides,
  }
}

describe('traces shouldRevalidate', () => {
  it.each([
    ['/traces', '/traces/trace-a', undefined, 'trace-a'],
    ['/traces/trace-a', '/traces/trace-b', 'trace-a', 'trace-b'],
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
        revalidationArgs('/traces/trace-b', '/traces', 'trace-b', undefined, {
          defaultShouldRevalidate: false,
        }),
      ),
    ).toBe(true)
  })

  it('delegates same-URL and unrelated navigation to React Router', () => {
    expect(
      shouldRevalidate(
        revalidationArgs('/traces', '/traces', undefined, undefined, {
          defaultShouldRevalidate: false,
        }),
      ),
    ).toBe(false)
    expect(
      shouldRevalidate(
        revalidationArgs('/traces', '/sources', undefined, undefined, {
          defaultShouldRevalidate: true,
        }),
      ),
    ).toBe(true)
    expect(
      shouldRevalidate(
        revalidationArgs(
          '/traces/trace-a?search=one',
          '/traces/trace-a?search=two',
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
        revalidationArgs('/traces/trace-a', '/traces', 'trace-a', undefined, {
          defaultShouldRevalidate: false,
          formMethod: 'POST',
        }),
      ),
    ).toBe(true)
  })
})

function ParentRoute() {
  const data = useLoaderData() as { traces: string[] }
  return (
    <div>
      {data.traces.map((traceId) => (
        <Link key={traceId} to={`/traces/${traceId}`}>
          {traceId}
        </Link>
      ))}
      <Outlet />
    </div>
  )
}

function DetailRoute() {
  const traceId = useLoaderData() as string
  const navigate = useNavigate()
  return (
    <div>
      <span>detail {traceId}</span>
      <button onClick={() => navigate('/traces')} type="button">
        Close
      </button>
    </div>
  )
}

describe('nested traces data routing', () => {
  it('preserves search and pro state in encoded adjacent trace locations', () => {
    expect(traceLocation('trace/with?reserved', '?search=playwright&pro')).toEqual({
      pathname: '/traces/trace%2Fwith%3Freserved',
      search: '?search=playwright&pro',
    })
  })
  it('preserves the list while detail is open and refreshes it immediately on close', async () => {
    const parentLoader = vi.fn().mockResolvedValue({ traces: ['trace-a', 'trace-b'] })
    const childLoader = vi.fn(({ params }) => params.traceId)
    const router = createMemoryRouter(
      [
        {
          children: [
            {
              element: <DetailRoute />,
              loader: childLoader,
              path: ':traceId',
            },
          ],
          element: <ParentRoute />,
          loader: parentLoader,
          path: '/traces',
          shouldRevalidate,
        },
      ],
      { initialEntries: ['/traces'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('link', { name: 'trace-a' }).click()
    await expect.element(screen.getByText('detail trace-a')).toBeVisible()
    await screen.getByRole('link', { name: 'trace-b' }).click()
    await expect.element(screen.getByText('detail trace-b')).toBeVisible()
    expect(parentLoader).toHaveBeenCalledOnce()
    await screen.getByRole('button', { name: 'Close' }).click()

    expect(router.state.location.pathname).toBe('/traces')
    expect(parentLoader).toHaveBeenCalledTimes(2)
    expect(childLoader).toHaveBeenCalledTimes(2)
  })

  it('supports a direct detail link and browser back to the mounted list', async () => {
    const router = createMemoryRouter(
      [
        {
          children: [
            {
              element: <DetailRoute />,
              loader: ({ params }) => params.traceId,
              path: ':traceId',
            },
          ],
          element: <ParentRoute />,
          loader: () => ({ traces: ['trace-a'] }),
          path: '/traces',
          shouldRevalidate,
        },
      ],
      { initialEntries: ['/traces', '/traces/trace-a'], initialIndex: 1 },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByText('detail trace-a')).toBeVisible()
    await router.navigate(-1)
    expect(router.state.location.pathname).toBe('/traces')
    await expect.element(screen.getByRole('link', { name: 'trace-a' })).toBeVisible()
  })
})

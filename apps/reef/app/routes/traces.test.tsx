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
import { listSearch, rootSpanIdFromSearch, traceLocation } from '@/views/traces/trace-location'
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
        revalidationArgs(
          `${TRACES_PATH}/trace-a?rootSpanId=root-a`,
          `${TRACES_PATH}/trace-a?rootSpanId=root-a`,
          'trace-a',
          'trace-a',
          { defaultShouldRevalidate: true },
        ),
      ),
    ).toBe(true)
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

  it('does not reload the parent list when only a same-trace root selector changes', () => {
    expect(
      shouldRevalidate(
        revalidationArgs(
          `${TRACES_PATH}/trace-a?search=kept&rootSpanId=root-a`,
          `${TRACES_PATH}/trace-a?search=kept&rootSpanId=root-b`,
          'trace-a',
          'trace-a',
          { defaultShouldRevalidate: true },
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

function ParentRoute() {
  const data = useLoaderData() as { traces: string[] }
  return (
    <div>
      {data.traces.map((traceId) => (
        <Link
          key={traceId}
          to={routePath('workspaceTrace', { traceId, workspaceId: WORKSPACE_ID })}
        >
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
      <button onClick={() => navigate(TRACES_PATH)} type="button">
        Close
      </button>
    </div>
  )
}

describe('nested traces data routing', () => {
  it('preserves search and pro state in encoded adjacent trace locations', () => {
    expect(traceLocation(WORKSPACE_ID, 'trace/with?reserved', '?search=playwright&pro')).toEqual({
      pathname: '/workspaces/analytics/traces/trace%2Fwith%3Freserved',
      search: '?search=playwright&pro',
    })
  })

  it('replaces and removes only the operation root span selector', () => {
    const location = traceLocation(
      WORKSPACE_ID,
      'shared-trace',
      '?search=playwright&rootSpanId=stale&pro',
      'root/next',
    )

    expect(location).toEqual({
      pathname: `${TRACES_PATH}/shared-trace`,
      search: '?search=playwright&pro&rootSpanId=root%2Fnext',
    })
    expect(rootSpanIdFromSearch(location.search)).toBe('root/next')
    expect(listSearch(location.search)).toBe('?search=playwright&pro')
  })

  it('preserves malformed encoding while removing only root span selectors', () => {
    expect(
      listSearch('?filter=%E0%A4%A&bad%ZZ=value&rootSpanId=stale%ZZ&pro&root%53panId=encoded'),
    ).toBe('?filter=%E0%A4%A&bad%ZZ=value&pro')
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
          path: TRACES_PATH,
          shouldRevalidate,
        },
      ],
      { initialEntries: [TRACES_PATH] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('link', { name: 'trace-a' }).click()
    await expect.element(screen.getByText('detail trace-a')).toBeVisible()
    await screen.getByRole('link', { name: 'trace-b' }).click()
    await expect.element(screen.getByText('detail trace-b')).toBeVisible()
    expect(parentLoader).toHaveBeenCalledOnce()
    await screen.getByRole('button', { name: 'Close' }).click()

    expect(router.state.location.pathname).toBe(TRACES_PATH)
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
          path: TRACES_PATH,
          shouldRevalidate,
        },
      ],
      { initialEntries: [TRACES_PATH, `${TRACES_PATH}/trace-a`], initialIndex: 1 },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByText('detail trace-a')).toBeVisible()
    await router.navigate(-1)
    expect(router.state.location.pathname).toBe(TRACES_PATH)
    await expect.element(screen.getByRole('link', { name: 'trace-a' })).toBeVisible()
  })

  it('reloads only child detail for same-trace root navigation and honors explicit refresh', async () => {
    const parentLoader = vi.fn().mockResolvedValue({ traces: ['trace-a'] })
    const childLoader = vi.fn(({ request }: { request: Request }) => {
      const rootSpanId = new URL(request.url).searchParams.get('rootSpanId')
      return rootSpanId
    })
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
          path: TRACES_PATH,
          shouldRevalidate,
        },
      ],
      { initialEntries: [`${TRACES_PATH}/trace-a?rootSpanId=root-a`] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByText('detail root-a')).toBeVisible()
    expect(parentLoader).toHaveBeenCalledOnce()
    expect(childLoader).toHaveBeenCalledOnce()

    await router.navigate(`${TRACES_PATH}/trace-a?rootSpanId=root-b`)
    await expect.element(screen.getByText('detail root-b')).toBeVisible()
    expect(parentLoader).toHaveBeenCalledOnce()
    expect(childLoader).toHaveBeenCalledTimes(2)

    await router.revalidate()
    expect(parentLoader).toHaveBeenCalledTimes(2)
    expect(childLoader).toHaveBeenCalledTimes(3)
  })
})

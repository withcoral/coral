/** @vitest-environment happy-dom */

import { act, render, screen } from '@testing-library/react'
import type { ReactNode } from 'react'
import {
  createMemoryRouter,
  Outlet,
  RouterProvider,
  type LoaderFunction,
  type RouteObject,
} from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { NavigationProgressBar } from './navigation-progress-bar'

// The tests drive the router and read its navigation state, so they build it
// themselves instead of using `renderRoute`.
const routers: ReturnType<typeof createMemoryRouter>[] = []

function AppShell() {
  return (
    <>
      <NavigationProgressBar />
      <Outlet />
    </>
  )
}

function Page({ children = 'Ready' }: { children?: ReactNode }) {
  return <main>{children}</main>
}

function SlowDestination() {
  vi.advanceTimersByTime(151)
  return <Page>Committed</Page>
}

function renderRouter(children: RouteObject[]) {
  const router = createMemoryRouter(
    [
      {
        Component: AppShell,
        children: [{ Component: Page, path: '/' }, ...children],
        path: '/',
      },
    ],
    { initialEntries: ['/'] },
  )
  routers.push(router)
  render(<RouterProvider router={router} />)

  return router
}

function progressBar() {
  return screen.queryByRole('progressbar')
}

function pendingLoader(): ReturnType<LoaderFunction> {
  return new Promise(() => undefined)
}

function deferredLoader() {
  let resolve!: () => void
  const promise = new Promise<null>((done) => {
    resolve = () => done(null)
  })
  return { loader: () => promise, resolve }
}

function deferredLoaders() {
  const completions: Array<() => void> = []
  return {
    loader: () =>
      new Promise<null>((done) => {
        completions.push(() => done(null))
      }),
    resolveNext: () => completions.shift()?.(),
  }
}

async function navigateWithoutWaiting(
  router: ReturnType<typeof createMemoryRouter>,
  path: string,
): Promise<void> {
  await act(async () => {
    void router.navigate(path)
    await Promise.resolve()
  })
}

async function navigateDeltaWithoutWaiting(
  router: ReturnType<typeof createMemoryRouter>,
  delta: number,
): Promise<void> {
  await act(async () => {
    void router.navigate(delta)
    await Promise.resolve()
  })
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe('NavigationProgressBar', () => {
  afterEach(async () => {
    vi.useRealTimers()
    for (const router of routers.splice(0)) router.dispose()
  })

  it('waits before showing the progress bar for a loading navigation', async () => {
    vi.useFakeTimers()
    const router = renderRouter([{ Component: Page, loader: pendingLoader, path: 'pending' }])

    await navigateWithoutWaiting(router, '/pending')
    expect(progressBar()).toBeNull()

    await advance(149)
    expect(progressBar()).toBeNull()

    await advance(1)
    expect(progressBar()).not.toBeNull()
  })

  it('keeps completed short navigations from flashing the progress bar', async () => {
    vi.useFakeTimers()
    const completion = deferredLoader()
    const router = renderRouter([{ Component: Page, loader: completion.loader, path: 'quick' }])

    let navigation!: Promise<void>
    await act(async () => {
      navigation = router.navigate('/quick')
      await Promise.resolve()
    })
    await advance(75)
    await act(async () => {
      completion.resolve()
      await navigation
    })
    await advance(75)

    expect(router.state.navigation.state).toBe('idle')
    expect(progressBar()).toBeNull()
  })

  it('hides after a delayed navigation completes', async () => {
    vi.useFakeTimers()
    const completion = deferredLoader()
    const router = renderRouter([{ Component: Page, loader: completion.loader, path: 'complete' }])

    let navigation!: Promise<void>
    await act(async () => {
      navigation = router.navigate('/complete')
      await Promise.resolve()
    })
    await advance(150)
    expect(progressBar()).not.toBeNull()

    await act(async () => {
      completion.resolve()
      await navigation
    })

    expect(progressBar()).toBeNull()
  })

  it('restarts the anti-flash delay when a loading navigation is interrupted', async () => {
    vi.useFakeTimers()
    const router = renderRouter([
      { Component: Page, loader: pendingLoader, path: 'first' },
      { Component: Page, loader: pendingLoader, path: 'second' },
    ])

    await navigateWithoutWaiting(router, '/first')
    await advance(75)
    await navigateWithoutWaiting(router, '/second')
    await advance(75)
    expect(progressBar()).toBeNull()

    await advance(75)
    expect(progressBar()).not.toBeNull()
  })

  it('does not show a replayed timer after navigation is already idle', async () => {
    vi.useFakeTimers()
    const router = renderRouter([
      {
        Component: SlowDestination,
        loader: () => new Promise((resolve) => setTimeout(() => resolve(null), 100)),
        path: 'slow',
      },
    ])

    let navigation!: Promise<void>
    await act(async () => {
      navigation = router.navigate('/slow')
      await Promise.resolve()
    })
    expect(progressBar()).toBeNull()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(100)
      await navigation
    })

    expect(router.state.navigation.state).toBe('idle')
    expect(progressBar()).toBeNull()
  })

  it('keeps the delay when returning to a previously visible history entry', async () => {
    vi.useFakeTimers()
    const visits = deferredLoaders()
    const router = renderRouter([
      {
        Component: Page,
        loader: visits.loader,
        path: 'visited',
        shouldRevalidate: () => true,
      },
    ])

    let navigation!: Promise<void>
    await act(async () => {
      navigation = router.navigate('/visited')
      await Promise.resolve()
    })
    await advance(150)
    expect(progressBar()).not.toBeNull()
    await act(async () => {
      visits.resolveNext()
      await navigation
    })
    expect(progressBar()).toBeNull()

    await act(async () => {
      await router.navigate(-1)
    })
    expect(progressBar()).toBeNull()

    await navigateDeltaWithoutWaiting(router, 1)
    expect(progressBar()).toBeNull()
    await advance(149)
    expect(progressBar()).toBeNull()

    await advance(1)
    expect(progressBar()).not.toBeNull()
  })
})

// Renders a view inside a memory router, as its route would, for unit tests.
// Stories use `createRoutesStub` instead; that stub keeps its router private,
// so a test could neither drive navigation nor dispose it.

import { act, render } from '@testing-library/react'
import { useEffect, useState, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import {
  createMemoryRouter,
  Outlet,
  RouterProvider,
  useLocation,
  type ActionFunction,
  type Location,
  type RouteObject,
} from 'react-router'
import { onTestFinished } from 'vitest'

export interface RouteOptions {
  /** The URL the router starts at. */
  entry?: string
  /** Answers form submissions to the rendered route. */
  action?: ActionFunction
  /** Routes beside the rendered one, such as a form's action or another page. */
  routes?: RouteObject[]
  /** The CSRF token the app shell hands its forms. */
  csrf?: string
}

/** A route's view, or a function that reads it for each location, as a loader would. */
export type RouteView = ReactNode | ((location: Location) => ReactNode)

/**
 * Mounts `view` as the route for every path, with Testing Library. The router
 * is disposed when the test finishes.
 */
export function renderRoute(view: RouteView, options: RouteOptions = {}) {
  const router = memoryRouter(view, options)
  onTestFinished(() => router.dispose())
  return { ...render(<RouterProvider router={router} />), router }
}

/**
 * Mounts `view` for `first`, as `renderRoute` does. `update` renders the next page in
 * place, as a live read replaces the page a loader supplied.
 */
export function renderLivePage<Page>(
  view: (page: Page) => ReactNode,
  first: Page,
  options: RouteOptions = {},
) {
  let show!: (next: Page) => void
  function LivePage() {
    const [page, setPage] = useState(first)
    useEffect(() => {
      show = setPage
    }, [])
    return view(page)
  }
  return { ...renderRoute(<LivePage />, options), update: (next: Page) => act(() => show(next)) }
}

/** Renders `view` as the route at `entry` to static HTML, as the server would. */
export function markupRoute(view: ReactNode, entry = '/') {
  const router = memoryRouter(view, { entry })
  try {
    return renderToStaticMarkup(<RouterProvider router={router} />)
  } finally {
    router.dispose()
  }
}

function memoryRouter(view: RouteView, { entry = '/', action, routes = [], csrf }: RouteOptions) {
  function Route() {
    // The location is an argument, not only a trigger: the compiler memoizes
    // the view on what it reads, and would otherwise keep the first one.
    const location = useLocation()
    return typeof view === 'function' ? view(location) : view
  }
  const page: RouteObject[] = [...routes, { path: '*', action, Component: Route }]
  if (csrf === undefined) return createMemoryRouter(page, { initialEntries: [entry] })
  const shell = { form: { csrf } }
  return createMemoryRouter(
    [{ id: 'routes/app-shell', loader: () => shell, Component: Outlet, children: page }],
    { initialEntries: [entry], hydrationData: { loaderData: { 'routes/app-shell': shell } } },
  )
}

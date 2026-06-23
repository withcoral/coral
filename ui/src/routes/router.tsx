import { Navigate, createBrowserRouter } from 'react-router'

import { App } from '@/App'
import { SourcesRoute, sourcesLoader } from './sources'
import { TraceDetailRoute, TracesRoute, traceDetailLoader, tracesLoader } from './traces'

function RouteHydrateFallback() {
  return <div aria-label="Loading Coral" />
}

export function createAppRouter() {
  return createBrowserRouter([
    {
      path: '/',
      Component: App,
      hydrateFallbackElement: <RouteHydrateFallback />,
      children: [
        { index: true, Component: TracesRoute, loader: tracesLoader },
        { path: 'traces', Component: TracesRoute, loader: tracesLoader },
        { path: 'traces/:traceId', Component: TraceDetailRoute, loader: traceDetailLoader },
        { path: 'sources', Component: SourcesRoute, loader: sourcesLoader },
        { path: '*', element: <Navigate to="/" replace /> },
      ],
    },
  ])
}

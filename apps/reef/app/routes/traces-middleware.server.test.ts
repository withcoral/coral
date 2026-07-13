import { createRequestHandler, type LoaderFunctionArgs, type ServerBuild } from 'react-router'
import { describe, expect, it } from 'vitest'

import { middleware } from './traces'

const routeComponent = () => null

const build = {
  assets: {
    entry: { imports: [], module: '' },
    routes: {},
    url: '',
    version: '',
  },
  assetsBuildDirectory: '',
  basename: '/',
  entry: {
    module: {
      default: async () => new Response('document'),
    },
  },
  future: {
    v8_middleware: true,
    v8_passThroughRequests: false,
    v8_trailingSlashAwareDataRequests: false,
  },
  isSpaMode: false,
  prerender: [],
  publicPath: '/',
  routeDiscovery: { manifestPath: '/__manifest', mode: 'lazy' },
  routes: {
    root: {
      id: 'root',
      module: { default: routeComponent },
      path: '',
    },
    'routes/trace-detail': {
      id: 'routes/trace-detail',
      module: {
        default: routeComponent,
        loader: ({ params }: LoaderFunctionArgs) => ({ traceId: params.traceId }),
      },
      parentId: 'routes/traces',
      path: ':traceId',
    },
    'routes/traces': {
      id: 'routes/traces',
      module: {
        default: routeComponent,
        loader: () => ({ traces: [] }),
        middleware,
      },
      parentId: 'root',
      path: 'workspaces/:workspaceId/traces',
    },
  },
  ssr: true,
} as unknown as ServerBuild

describe('traces response cache policy', () => {
  it.each(['/workspaces/analytics/traces.data', '/workspaces/analytics/traces/example.data'])(
    'marks the final %s response as private and non-cacheable',
    async (pathname) => {
      const handleRequest = createRequestHandler(build, 'test')

      const response = await handleRequest(new Request(`http://reef.test${pathname}`))

      expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    },
  )
})

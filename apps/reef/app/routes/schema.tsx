import { fetchSchemaFromCoral } from '@/lib/schema-explorer'
import { SchemaExplorer, SchemaExplorerError } from '@/views/schema-explorer/schema'

import type { Route } from './+types/schema'

// The catalog is fetched over gRPC-web via the browser transport + desktop
// bridge, so this must be a clientLoader (until the SSR/BFF stack lands and a
// server loader can talk to the sidecar directly). The schema promise is
// deferred (not awaited) so the route renders immediately and the view streams
// it in with Suspense — SPA mode forbids a route HydrateFallback. The abort
// signal cancels the catalog request when the navigation is superseded.
export function clientLoader({ request }: Route.ClientLoaderArgs) {
  return { schema: fetchSchemaFromCoral(request.signal) }
}

clientLoader.hydrate = true as const

export default function SchemaRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaExplorer schema={loaderData.schema} />
}

export { SchemaExplorerError as ErrorBoundary }

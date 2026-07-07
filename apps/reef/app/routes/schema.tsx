import { fetchSchemaFromCoral } from '@/lib/schema-explorer'
import { SchemaExplorer } from '@/views/schema-explorer'

import type { Route } from './+types/schema'

// Desktop is SPA-only (no server) and the catalog is fetched over gRPC-web via
// the browser transport + desktop bridge, so this must be a clientLoader. The
// schema promise is deferred (not awaited) so the route renders immediately and
// the view streams it in with Suspense — SPA mode forbids a route HydrateFallback.
export function clientLoader(_args: Route.ClientLoaderArgs) {
  return { schema: fetchSchemaFromCoral() }
}

clientLoader.hydrate = true as const

export default function SchemaRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaExplorer schema={loaderData.schema} />
}

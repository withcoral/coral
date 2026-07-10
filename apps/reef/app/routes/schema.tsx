import { fetchSchemaFromCoral } from '@/lib/schema-explorer'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { SchemaExplorer, SchemaExplorerError } from '@/views/schema-explorer/schema'

import type { Route } from './+types/schema'

// The schema is this page's critical data: await it in the server loader so the
// global navigation progress bar is the pending UI and a failure lands in the
// route ErrorBoundary. The abort signal cancels the catalog request when the
// navigation is superseded.
export async function loader({ request }: Route.LoaderArgs) {
  return { schema: await fetchSchemaFromCoral(catalogClientForRequest(request), request.signal) }
}

export default function SchemaRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaExplorer schema={loaderData.schema} />
}

export { SchemaExplorerError as ErrorBoundary }

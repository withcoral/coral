import { fetchSchemaFromCoral } from '@/lib/schema-explorer'
import { requestAuthContext } from '@/auth/server-context'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { SchemaExplorer, SchemaExplorerError } from '@/views/schema-explorer/schema'

import type { Route } from './+types/schema'

// The schema is this page's critical data: await it in the server loader so the
// global navigation progress bar is the pending UI and a failure lands in the
// route ErrorBoundary. The abort signal cancels the catalog request when the
// navigation is superseded.
export async function loader({ context, params, request }: Route.LoaderArgs) {
  const workspace = workspaceFromParams(params)
  return {
    schema: await fetchSchemaFromCoral(
      catalogClientForRequest(request, context.get(requestAuthContext).accessToken),
      workspace,
      request.signal,
    ),
  }
}

export default function SchemaRoute({ loaderData, params }: Route.ComponentProps) {
  return <SchemaExplorer schema={loaderData.schema} workspaceId={params.workspaceId} />
}

export { SchemaExplorerError as ErrorBoundary }

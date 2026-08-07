import { fetchTableColumnsFromCoral } from '@/lib/schema-explorer'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

import type { Route } from './+types/schema-table'

// Columns are this panel's critical data: await them so the global navigation
// progress bar is the pending UI (deferring them doesn't work here — table
// switches re-suspend an already-revealed Suspense boundary inside the router
// transition, so React keeps the old columns instead of showing a fallback).
// The abort signal matters: large tables fan out concurrent paginated
// ListColumns calls, so switching tables quickly would otherwise pile up
// orphaned requests.
export async function loader({ params, request }: Route.LoaderArgs) {
  const workspace = workspaceFromParams(params)
  return {
    columns: await fetchTableColumnsFromCoral(
      catalogClientForRequest(request),
      workspace,
      undefined,
      params.schemaName,
      params.tableName,
      request.signal,
    ),
  }
}

export default function SchemaTableRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaTableView columns={loaderData.columns} />
}

export { SchemaTableError as ErrorBoundary }

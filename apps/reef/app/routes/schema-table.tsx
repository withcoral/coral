import { fetchTableColumnsFromCoral } from '@/lib/schema-explorer'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

import type { Route } from './+types/schema-table'

// Columns are deferred (the promise is returned un-awaited) so nested
// table-to-table navigation keeps the layout and shows a local pending state
// instead of blocking on the global progress bar. The abort signal matters
// here: large tables fan out concurrent paginated ListColumns calls, so
// switching tables quickly would otherwise pile up orphaned requests.
export function loader({ params, request }: Route.LoaderArgs) {
  return {
    columns: fetchTableColumnsFromCoral(
      catalogClientForRequest(request),
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

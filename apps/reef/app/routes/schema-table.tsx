import { fetchTableColumnsFromCoral } from '@/lib/schema-explorer'
import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

import type { Route } from './+types/schema-table'

// Deferred like the parent schema loader; the columns stream in under Suspense.
// The abort signal matters most here: large tables fan out concurrent paginated
// ListColumns calls, so switching tables quickly would otherwise pile up
// orphaned in-flight requests against the sidecar.
export function clientLoader({ params, request }: Route.ClientLoaderArgs) {
  return {
    columns: fetchTableColumnsFromCoral(params.schemaName, params.tableName, request.signal),
  }
}

clientLoader.hydrate = true as const

export default function SchemaTableRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaTableView columns={loaderData.columns} />
}

export { SchemaTableError as ErrorBoundary }

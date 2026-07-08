import { Suspense } from 'react'
import { Await, useOutletContext } from 'react-router'

import { fetchTableColumnsFromCoral, type SchemaResponse } from '@/lib/schema-explorer'
import {
  ColumnsLoadError,
  ColumnsPending,
  ColumnsTable,
  findSchemaTable,
  TableDetailLayout,
} from '@/views/schema-explorer'

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

export default function SchemaTableRoute({ params, loaderData }: Route.ComponentProps) {
  // The parent schema layout resolves its schema before rendering this Outlet,
  // so table metadata (description, required filters) is available synchronously.
  const schema = useOutletContext<SchemaResponse>()
  const table = findSchemaTable(schema, params.schemaName, params.tableName)

  return (
    <TableDetailLayout schemaName={params.schemaName} tableName={params.tableName} table={table}>
      <Suspense fallback={<ColumnsPending />}>
        <Await errorElement={<ColumnsLoadError />} resolve={loaderData.columns}>
          {(columns) => <ColumnsTable columns={columns} />}
        </Await>
      </Suspense>
    </TableDetailLayout>
  )
}

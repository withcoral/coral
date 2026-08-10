import { fetchTableColumnsFromCoral } from '@/lib/schema-explorer'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

import type { Route } from './+types/schema-catalog-table'

export async function loader({ params, request }: Route.LoaderArgs) {
  const workspace = workspaceFromParams(params)
  return {
    columns: await fetchTableColumnsFromCoral(
      catalogClientForRequest(request),
      workspace,
      {
        catalogName: params.catalogName,
        schemaName: params.schemaName,
        tableName: params.tableName,
      },
      request.signal,
    ),
  }
}

export default function SchemaCatalogTableRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaTableView columns={loaderData.columns} />
}

export { SchemaTableError as ErrorBoundary }

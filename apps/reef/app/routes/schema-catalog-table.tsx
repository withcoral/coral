import { requestAuthContext } from '@/auth/server-context'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { fetchTableColumnsFromCoral } from '@/lib/schema-explorer'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

import type { Route } from './+types/schema-catalog-table'

export async function loader({ context, params, request }: Route.LoaderArgs) {
  const workspace = workspaceFromParams(params)
  return {
    columns: await fetchTableColumnsFromCoral(
      catalogClientForRequest(request, context.get(requestAuthContext).accessToken),
      workspace,
      params.catalogName,
      params.schemaName,
      params.tableName,
      request.signal,
    ),
  }
}

export default function SchemaCatalogTableRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaTableView columns={loaderData.columns} />
}

export { SchemaTableError as ErrorBoundary }

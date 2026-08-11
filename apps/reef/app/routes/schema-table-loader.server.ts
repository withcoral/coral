import type { RouterContextProvider } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import { catalogClientForRequest } from '@/lib/coral-request.server'
import { fetchTableColumnsFromCoral, type ColumnDef } from '@/lib/schema-explorer'
import { workspaceFromParams } from '@/lib/workspace-routing'

interface SchemaTableRouteLoaderArgs {
  context: Readonly<RouterContextProvider>
  params: {
    catalogName?: string
    schemaName: string
    tableName: string
    workspaceId?: string
  }
  request: Request
}

export interface SchemaTableRouteData {
  columns: ColumnDef[]
}

// Columns are this panel's critical data: await them so the global navigation
// progress bar is the pending UI (deferring them doesn't work here — table
// switches re-suspend an already-revealed Suspense boundary inside the router
// transition, so React keeps the old columns instead of showing a fallback).
// The abort signal matters: large tables fan out concurrent paginated
// ListColumns calls, so switching tables quickly would otherwise pile up
// orphaned requests.
export async function loadSchemaTableRoute({
  context,
  params,
  request,
}: SchemaTableRouteLoaderArgs): Promise<SchemaTableRouteData> {
  return {
    columns: await fetchTableColumnsFromCoral(
      catalogClientForRequest(request, context.get(requestAuthContext).accessToken),
      workspaceFromParams(params),
      {
        catalogName: params.catalogName ?? '',
        schemaName: params.schemaName,
        tableName: params.tableName,
      },
      request.signal,
    ),
  }
}

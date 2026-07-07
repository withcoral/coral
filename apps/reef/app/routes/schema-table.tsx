import { useNavigation, useParams, useRevalidator, useRouteLoaderData } from 'react-router'

import { fetchTableColumnsFromCoral, type TableDef } from '@/lib/schema-explorer'
import {
  ColumnsError,
  ColumnsPending,
  ColumnsTable,
  TableDetailLayout,
} from '@/views/schema-explorer'

import type { clientLoader as schemaClientLoader } from './schema'
import type { Route } from './+types/schema-table'

export async function clientLoader({ params }: Route.ClientLoaderArgs) {
  return { columns: await fetchTableColumnsFromCoral(params.schemaName, params.tableName) }
}

// No server loader, so hydrate a direct load of a table URL from the client.
clientLoader.hydrate = true as const

// Table metadata (description, required filters) lives in the parent schema
// loader; look it up by the route params.
function useTableMeta(schemaName: string, tableName: string): TableDef | undefined {
  const parent = useRouteLoaderData<typeof schemaClientLoader>('routes/schema')
  return parent?.schema.connectors
    .find((connector) => connector.name === schemaName)
    ?.tables.find((table) => table.name === tableName)
}

export function HydrateFallback() {
  const params = useParams()
  const schemaName = params.schemaName ?? ''
  const tableName = params.tableName ?? ''
  const table = useTableMeta(schemaName, tableName)
  return (
    <TableDetailLayout schemaName={schemaName} tableName={tableName} table={table}>
      <ColumnsPending />
    </TableDetailLayout>
  )
}

export default function SchemaTableRoute({ params, loaderData }: Route.ComponentProps) {
  const navigation = useNavigation()
  const table = useTableMeta(params.schemaName, params.tableName)
  // Switching tables keeps the previous columns mounted until the next loader
  // resolves; show the spinner instead of stale data during that transition.
  const switching =
    navigation.state === 'loading' && !!navigation.location?.pathname.startsWith('/schema/')
  return (
    <TableDetailLayout schemaName={params.schemaName} tableName={params.tableName} table={table}>
      {switching ? <ColumnsPending /> : <ColumnsTable columns={loaderData.columns} />}
    </TableDetailLayout>
  )
}

export function ErrorBoundary({ error, params }: Route.ErrorBoundaryProps) {
  const { revalidate } = useRevalidator()
  const table = useTableMeta(params.schemaName ?? '', params.tableName ?? '')
  const message = error instanceof Error ? error.message : String(error)
  return (
    <TableDetailLayout
      schemaName={params.schemaName ?? ''}
      tableName={params.tableName ?? ''}
      table={table}
    >
      <ColumnsError message={message} onRetry={() => void revalidate()} />
    </TableDetailLayout>
  )
}

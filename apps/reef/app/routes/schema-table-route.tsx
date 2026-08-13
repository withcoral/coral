import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

import type { SchemaTableRouteData } from './schema-table-loader.server'

export function SchemaTableRoute({ loaderData }: { loaderData: SchemaTableRouteData }) {
  return <SchemaTableView columns={loaderData.columns} />
}

export { SchemaTableError }

import type { ColumnDef } from '@/lib/schema-explorer'
import { SchemaTableError, SchemaTableView } from '@/views/schema-explorer/schema-table'

export function SchemaTableRoute({ loaderData }: { loaderData: { columns: ColumnDef[] } }) {
  return <SchemaTableView columns={loaderData.columns} />
}

export { SchemaTableError }

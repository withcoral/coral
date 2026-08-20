import { useOutletContext, useParams, useRouteError } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import type { ColumnDef, SchemaResponse, TableDef } from '@/lib/schema-explorer'
import { Pill } from '@/wax/components/pill'
import { Table } from '@/wax/components'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'
import { findSchemaTable } from './schema'
import { formatError, useRouteRetry } from './shared'

interface DisplayColumn {
  description?: string
  filterable?: boolean
  name: string
  nullable: boolean
  type: string
  virtual?: boolean
}

// The parent schema layout resolves its schema before rendering this Outlet, so
// table metadata (description, required filters) is available synchronously.
function useSelectedTable(): { schemaName: string; tableName: string; table?: TableDef } {
  const schema = useOutletContext<SchemaResponse>()
  const params = useParams()
  const schemaName = params.schemaName ?? ''
  const tableName = params.tableName ?? ''
  return { schemaName, tableName, table: findSchemaTable(schema, schemaName, tableName) }
}

// Detail-panel scaffold for a selected table: title, description, required
// filters, and a "Columns" section body.
function TableDetailLayout({ children }: { children: React.ReactNode }) {
  const { schemaName, tableName, table } = useSelectedTable()
  return (
    <div className={styles.detailContent}>
      <div className={styles.detailHeader}>
        <div>
          <Typography.HeadingSmall as="h2">
            {schemaName}.{tableName}
          </Typography.HeadingSmall>
          {table?.description ? (
            <Typography.BodySmall as="p" className={styles.description}>
              {table.description}
            </Typography.BodySmall>
          ) : null}
        </div>
        {table && table.requiredFilters.length > 0 ? (
          <Tooltip
            content={`The following filters are required: ${table.requiredFilters.join(', ')}`}
            side="top"
          >
            <div
              aria-label={`Required filters: ${table.requiredFilters.join(', ')}`}
              className={styles.requiredFilterGroup}
              tabIndex={0}
            >
              {table.requiredFilters.map((filter) => (
                <Pill color="orange" key={filter}>
                  {filter}
                </Pill>
              ))}
            </div>
          </Tooltip>
        ) : null}
      </div>

      <div className={styles.section}>
        <Typography.BodySmallStrong>Columns</Typography.BodySmallStrong>
        {children}
      </div>
    </div>
  )
}

// Route ErrorBoundary for /schema/:schemaName/:tableName — replaces only the
// detail panel (the tree stays mounted in the parent layout). Re-exported by
// the route module.
export function SchemaTableError() {
  const error = useRouteError()
  const retry = useRouteRetry()
  return (
    <TableDetailLayout>
      <ErrorBanner message={formatError(error)} onRetry={retry} title="Failed to load columns" />
    </TableDetailLayout>
  )
}

export function SchemaTableView({ columns }: { columns: ColumnDef[] }) {
  return (
    <TableDetailLayout>
      <SchemaColumnsTable columns={columns} emptyMessage="No columns reported for this table." />
    </TableDetailLayout>
  )
}

const SCHEMA_COLUMNS: Table.Column[] = [
  { label: 'name', width: 'content' },
  { label: 'type', width: 'content' },
  { label: 'nullable', width: 'content' },
  { label: 'description', width: 'content' },
]

export function SchemaColumnsTable({
  columns,
  emptyMessage,
}: {
  columns: DisplayColumn[]
  emptyMessage: string
}) {
  if (columns.length === 0) {
    return (
      <Typography.BodySmall className={styles.emptyInline} variant="tertiary">
        {emptyMessage}
      </Typography.BodySmall>
    )
  }

  return (
    <Table.Container columns={SCHEMA_COLUMNS} density="compact">
      <Table.Head />
      <Table.Body>
        {columns.map((column) => (
          <Table.Row className={column.virtual ? styles.virtualRow : undefined} key={column.name}>
            <Table.Cell mono>
              {column.name}
              {column.filterable ? (
                <Tooltip
                  content="Required filter: queries for this table must include a filter on this field."
                  side="top"
                >
                  <span aria-label="Required filter" className={styles.requiredStar} tabIndex={0}>
                    *
                  </span>
                </Tooltip>
              ) : null}
            </Table.Cell>
            <Table.Cell mono>{column.type}</Table.Cell>
            <Table.Cell mono>{column.nullable ? 'yes' : 'no'}</Table.Cell>
            <Table.Cell>{column.description ?? '-'}</Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Container>
  )
}

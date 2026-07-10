import classNames from 'classnames'
import { Suspense, use } from 'react'
import { useOutletContext, useParams, useRouteError } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import type { ColumnDef, SchemaResponse, TableDef } from '@/lib/schema-explorer'
import { Icon } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'
import { findSchemaTable } from './schema'
import { formatError, useRouteRetry } from './shared'

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

function ColumnsPending() {
  return (
    <div className={styles.loadingState}>
      <Icon color="secondary" name="Loader" size="18" />
      <Typography.BodySmall variant="tertiary">Loading columns</Typography.BodySmall>
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

// Deferred columns stream in under Suspense; `use()` propagates a rejection to
// the route ErrorBoundary above.
export function SchemaTableView({ columns }: { columns: Promise<ColumnDef[]> }) {
  return (
    <TableDetailLayout>
      <Suspense fallback={<ColumnsPending />}>
        <ResolvedColumns columns={columns} />
      </Suspense>
    </TableDetailLayout>
  )
}

function ResolvedColumns({ columns: columnsPromise }: { columns: Promise<ColumnDef[]> }) {
  const columns = use(columnsPromise)
  if (columns.length === 0) {
    return (
      <Typography.BodySmall className={styles.emptyInline} variant="tertiary">
        No columns reported for this table.
      </Typography.BodySmall>
    )
  }

  return (
    <table className={styles.dataGrid}>
      <thead className={styles.dataGridHead}>
        <tr>
          <th className={styles.dataGridHeadCell}>name</th>
          <th className={styles.dataGridHeadCell}>type</th>
          <th className={styles.dataGridHeadCell}>nullable</th>
          <th className={styles.dataGridHeadCell}>description</th>
        </tr>
      </thead>
      <tbody>
        {columns.map((column) => (
          <tr
            className={classNames(styles.dataGridRow, { [styles.virtualRow]: column.virtual })}
            key={column.name}
          >
            <td className={styles.dataGridCellMono}>
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
            </td>
            <td className={styles.dataGridCellMono}>{column.type}</td>
            <td className={styles.dataGridCellMono}>{column.nullable ? 'yes' : 'no'}</td>
            <td className={styles.dataGridCellText}>{column.description ?? '-'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

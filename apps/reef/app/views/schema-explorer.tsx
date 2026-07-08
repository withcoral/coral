import classNames from 'classnames'
import { Suspense, useMemo, useState } from 'react'
import { Await, NavLink, Outlet, useAsyncError, useParams, useRevalidator } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import type { ColumnDef, SchemaGroup, SchemaResponse, TableDef } from '@/lib/schema-explorer'
import { Container as ButtonContainer, IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Pill } from '@/wax/components/pill'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Skeleton } from '@/wax/components/skeleton'
import { Table } from '@/wax/components/table'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'

function formatError(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

export function findSchemaTable(
  schema: SchemaResponse,
  schemaName: string,
  tableName: string,
): TableDef | undefined {
  return schema.connectors
    .find((connector) => connector.name === schemaName)
    ?.tables.find((table) => table.name === tableName)
}

function SkeletonTree() {
  return (
    <div className={styles.skeletonContainer}>
      {Array.from({ length: 5 }).map((_, index) => (
        <div className={styles.skeletonGroup} key={index}>
          <Skeleton borderRadius={4} height={20} width={140} />
          <div className={styles.skeletonChildren}>
            <Skeleton borderRadius={4} height={16} width={112} />
            <Skeleton borderRadius={4} height={16} width={132} />
          </div>
        </div>
      ))}
    </div>
  )
}

function schemaMatchesSearch(schema: SchemaGroup, search: string) {
  if (!search) return true
  return (
    schemaNameMatchesSearch(schema, search) ||
    schema.tables.some((table) => tableMatchesSearch(table, search))
  )
}

function schemaNameMatchesSearch(schema: SchemaGroup, search: string) {
  return schema.name.toLowerCase().includes(search)
}

function tableMatchesSearch(table: TableDef, search: string) {
  if (!search) return true
  if (table.name.toLowerCase().includes(search)) return true
  if (table.description?.toLowerCase().includes(search)) return true
  return false
}

function visibleTablesForSchema(schema: SchemaGroup, search: string) {
  if (!search || schemaNameMatchesSearch(schema, search)) return schema.tables
  return schema.tables.filter((table) => tableMatchesSearch(table, search))
}

function tablePath(schemaName: string, tableName: string) {
  return `/schema/${encodeURIComponent(schemaName)}/${encodeURIComponent(tableName)}`
}

// Header + two-panel scaffold shared by the loaded view and the loading/error
// states so all three look the same.
function Frame({ children }: { children: React.ReactNode }) {
  return (
    <section aria-label="Schema explorer" className={styles.root}>
      <div className={styles.header}>
        <div className={styles.headerTitle}>
          <Typography.HeadingSmall as="h1">Schema explorer</Typography.HeadingSmall>
        </div>
      </div>
      {children}
    </section>
  )
}

// Suspense fallback while the schema clientLoader promise resolves.
export function SchemaExplorerSkeleton() {
  return (
    <Frame>
      <div className={styles.body}>
        <div className={styles.treePanel}>
          <div className={styles.treeContent}>
            <SkeletonTree />
          </div>
        </div>
        <div className={styles.detailPanel} />
      </div>
    </Frame>
  )
}

// Await errorElement when the schema clientLoader promise rejects.
function SchemaLoadError() {
  const error = useAsyncError()
  const { revalidate } = useRevalidator()
  return (
    <Frame>
      <div className={styles.body}>
        <div className={styles.treePanel}>
          <div className={styles.treeContent}>
            <div className={styles.treeError}>
              <ErrorBanner
                message={formatError(error)}
                onRetry={() => void revalidate()}
                title="Failed to load schema"
              />
            </div>
          </div>
        </div>
        <div className={styles.detailPanel} />
      </div>
    </Frame>
  )
}

// Deferred loader data: stream in the skeleton, then the resolved tree. SPA mode
// forbids HydrateFallback on non-root routes, so loading is handled with
// Suspense/Await rather than a route-level fallback.
export function SchemaExplorer({ schema }: { schema: Promise<SchemaResponse> }) {
  return (
    <Suspense fallback={<SchemaExplorerSkeleton />}>
      <Await errorElement={<SchemaLoadError />} resolve={schema}>
        {(resolved: SchemaResponse) => <SchemaExplorerContent schema={resolved} />}
      </Await>
    </Suspense>
  )
}

function SchemaExplorerContent({ schema }: { schema: SchemaResponse }) {
  const activeTable = useParams()
  const [search, setSearch] = useState('')
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(
    () => new Set(schema.connectors.map((connector) => connector.name)),
  )

  const normalizedSearch = search.trim().toLowerCase()
  const filteredSchemas = useMemo(
    () => schema.connectors.filter((connector) => schemaMatchesSearch(connector, normalizedSearch)),
    [normalizedSearch, schema],
  )

  const filteredTableCount = filteredSchemas.reduce(
    (count, connector) => count + visibleTablesForSchema(connector, normalizedSearch).length,
    0,
  )

  const toggleSchema = (name: string) => {
    setExpandedSchemas((previous) => {
      const next = new Set(previous)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  return (
    <section aria-label="Schema explorer" className={styles.root}>
      <div className={styles.header}>
        <div className={styles.headerTitle}>
          <Typography.HeadingSmall as="h1">Schema explorer</Typography.HeadingSmall>
          <Typography.BodySmall as="span" variant="tertiary">
            {filteredSchemas.length} {filteredSchemas.length === 1 ? 'schema' : 'schemas'} /{' '}
            {filteredTableCount} {filteredTableCount === 1 ? 'table' : 'tables'}
          </Typography.BodySmall>
        </div>
      </div>

      <div className={styles.body}>
        <div className={styles.treePanel}>
          <div className={styles.treePanelToolbar}>
            <div className={styles.searchRow}>
              <TextInput
                icon="Search"
                onChange={setSearch}
                placeholder="Filter schemas and tables"
                type="search"
                value={search}
              />
              {search ? (
                <IconButton
                  className={styles.clearButton}
                  name="X"
                  onClick={() => setSearch('')}
                  size="22"
                  tooltipText="Clear filter"
                  variant="bare"
                />
              ) : null}
            </div>
          </div>

          <div className={styles.treeContent}>
            <ScrollArea constrainWidth>
              {normalizedSearch && filteredSchemas.length === 0 ? (
                <div className={styles.treeEmpty}>
                  <Typography.BodySmall variant="tertiary">
                    No results for "{search}"
                  </Typography.BodySmall>
                </div>
              ) : filteredSchemas.length === 0 ? (
                <div className={styles.treeEmpty}>
                  <Typography.BodySmall variant="tertiary">
                    No queryable tables in this workspace.
                  </Typography.BodySmall>
                </div>
              ) : (
                <div className={styles.treeList}>
                  {filteredSchemas.map((connector) => {
                    const expanded = expandedSchemas.has(connector.name)
                    const connectorChildrenId = `schema-${connector.name}-tables`
                    const visibleTables = visibleTablesForSchema(connector, normalizedSearch)
                    return (
                      <div key={connector.name}>
                        <ButtonContainer
                          aria-controls={connectorChildrenId}
                          aria-expanded={expanded}
                          className={styles.treeRow}
                          fullWidth
                          onClick={() => toggleSchema(connector.name)}
                          size="22"
                          variant="bare"
                        >
                          <Icon
                            color="secondary"
                            name={expanded ? 'ChevronDown' : 'ChevronRight'}
                            size="14"
                          />
                          <Typography.BodyStrong as="span" className={styles.connectorName}>
                            {connector.name}
                          </Typography.BodyStrong>
                          <Typography.BodySmall
                            as="span"
                            className={styles.connectorTableCount}
                            variant="tertiary"
                          >
                            {visibleTables.length}
                          </Typography.BodySmall>
                        </ButtonContainer>

                        {expanded ? (
                          <div className={styles.connectorChildren} id={connectorChildrenId}>
                            {visibleTables.map((table) => (
                              <ButtonContainer
                                as={NavLink}
                                className={styles.treeRow}
                                fullWidth
                                isActive={
                                  activeTable.schemaName === connector.name &&
                                  activeTable.tableName === table.name
                                }
                                key={tablePath(connector.name, table.name)}
                                size="22"
                                to={tablePath(connector.name, table.name)}
                                variant="bare"
                              >
                                <Icon color="secondary" name="Table2" size="14" />
                                <Typography.BodyStrong as="span" className={styles.tableName}>
                                  {table.name}
                                </Typography.BodyStrong>
                              </ButtonContainer>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    )
                  })}
                </div>
              )}
            </ScrollArea>
          </div>
        </div>

        <div className={styles.detailPanel}>
          <Outlet context={schema} />
        </div>
      </div>
    </section>
  )
}

// Rendered at /schema (index) before a table is selected.
export function TableDetailEmpty() {
  return (
    <div className={styles.detailEmpty}>
      <Typography.Body variant="secondary">
        Select a table from the schema tree to inspect its columns.
      </Typography.Body>
    </div>
  )
}

// Detail-panel scaffold for a selected table: title, description, required
// filters, and a "Columns" section whose body is supplied by the child route.
export function TableDetailLayout({
  schemaName,
  tableName,
  table,
  children,
}: {
  schemaName: string
  tableName: string
  table?: TableDef
  children: React.ReactNode
}) {
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

export function ColumnsPending() {
  return (
    <div className={styles.loadingState}>
      <Icon color="secondary" name="Loader" size="18" />
      <Typography.BodySmall variant="tertiary">Loading columns</Typography.BodySmall>
    </div>
  )
}

// Await errorElement when the columns clientLoader promise rejects.
export function ColumnsLoadError() {
  const error = useAsyncError()
  const { revalidate } = useRevalidator()
  return (
    <ErrorBanner
      message={formatError(error)}
      onRetry={() => void revalidate()}
      title="Failed to load columns"
    />
  )
}

export function ColumnsTable({ columns }: { columns: ColumnDef[] }) {
  if (columns.length === 0) {
    return (
      <Typography.BodySmall className={styles.emptyInline} variant="tertiary">
        No columns reported for this table.
      </Typography.BodySmall>
    )
  }

  return (
    <Table.Wrapper>
      <Table.Root>
        <Table.Head>
          <Table.Row>
            <Table.HeaderCell>Name</Table.HeaderCell>
            <Table.HeaderCell>Type</Table.HeaderCell>
            <Table.HeaderCell>Description</Table.HeaderCell>
          </Table.Row>
        </Table.Head>
        <Table.Body>
          {columns.map((column) => (
            <Table.Row
              className={classNames({ [styles.virtualRow]: column.virtual })}
              key={column.name}
            >
              <Table.Cell>
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
              <Table.Cell>{column.type}</Table.Cell>
              <Table.Cell className={styles.cellTruncate} mono={false}>
                {column.description ?? '-'}
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>
    </Table.Wrapper>
  )
}

import classNames from 'classnames'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { ErrorBanner } from '@/components/error-banner'
import {
  fetchSchemaFromCoral,
  fetchTableColumnsFromCoral,
  type ColumnDef,
  type SchemaGroup,
  type SchemaResponse,
  type TableDef,
} from '@/lib/schema-explorer'
import { IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Pill } from '@/wax/components/pill'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Skeleton } from '@/wax/components/skeleton'
import { Table } from '@/wax/components/table'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'

interface SelectedTable {
  schemaName: string
  table: TableDef
}

interface ColumnLoadState {
  error?: string
  key?: string
  loading: boolean
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

function formatError(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

function tableKey(schemaName: string, tableName: string) {
  return `${schemaName}.${tableName}`
}

function withTableColumns(
  schema: SchemaResponse | null,
  schemaName: string,
  tableName: string,
  columns: ColumnDef[],
): SchemaResponse | null {
  if (!schema) return schema

  return {
    connectors: schema.connectors.map((connector) =>
      connector.name === schemaName
        ? {
            ...connector,
            tables: connector.tables.map((table) =>
              table.name === tableName ? { ...table, columns, columnsLoaded: true } : table,
            ),
          }
        : connector,
    ),
  }
}

export function SchemaExplorer() {
  const [schema, setSchema] = useState<SchemaResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set())
  const [selectedTable, setSelectedTable] = useState<SelectedTable | null>(null)
  const [columnState, setColumnState] = useState<ColumnLoadState>({ loading: false })

  const loadSchema = useCallback(async () => {
    setLoading(true)
    setLoadError(null)

    try {
      const data = await fetchSchemaFromCoral()
      setSchema(data)
      setExpandedSchemas(new Set(data.connectors.map((connector) => connector.name)))
      setSelectedTable(null)
      setColumnState({ loading: false })
    } catch (error) {
      setSchema({ connectors: [] })
      setLoadError(formatError(error))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadSchema()
  }, [loadSchema])

  const normalizedSearch = search.trim().toLowerCase()
  const filteredSchemas = useMemo(
    () =>
      schema?.connectors.filter((connector) => schemaMatchesSearch(connector, normalizedSearch)) ??
      [],
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

  const loadTableColumns = useCallback(async (schemaName: string, table: TableDef) => {
    if (table.columnsLoaded) return

    const key = tableKey(schemaName, table.name)
    setColumnState({ key, loading: true })

    try {
      const columns = await fetchTableColumnsFromCoral(schemaName, table.name)
      setSchema((previous) => withTableColumns(previous, schemaName, table.name, columns))
      setSelectedTable((previous) =>
        previous?.schemaName === schemaName && previous.table.name === table.name
          ? {
              schemaName,
              table: {
                ...previous.table,
                columns,
                columnsLoaded: true,
              },
            }
          : previous,
      )
      setColumnState((previous) => (previous.key === key ? { loading: false } : previous))
    } catch (error) {
      setColumnState((previous) =>
        previous.key === key ? { error: formatError(error), key, loading: false } : previous,
      )
    }
  }, [])

  const handleSelectTable = (schemaName: string, table: TableDef) => {
    setSelectedTable({ schemaName, table })
    void loadTableColumns(schemaName, table)
  }

  const selectedTableKey = selectedTable
    ? tableKey(selectedTable.schemaName, selectedTable.table.name)
    : undefined
  const selectedColumnsLoading =
    !!selectedTableKey && columnState.key === selectedTableKey && columnState.loading
  const selectedColumnsError =
    selectedTableKey && columnState.key === selectedTableKey ? columnState.error : undefined

  return (
    <section aria-label="Schema explorer" className={styles.root}>
      <div className={styles.header}>
        <Typography.HeadingSmall as="h1">Schema explorer</Typography.HeadingSmall>
        <Typography.BodySmall as="span" variant="tertiary">
          {filteredSchemas.length} {filteredSchemas.length === 1 ? 'schema' : 'schemas'} /{' '}
          {filteredTableCount} {filteredTableCount === 1 ? 'table' : 'tables'}
        </Typography.BodySmall>
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
              {loading ? (
                <SkeletonTree />
              ) : loadError ? (
                <div className={styles.treeError}>
                  <ErrorBanner
                    message={loadError}
                    onRetry={loadSchema}
                    title="Failed to load schema"
                  />
                </div>
              ) : normalizedSearch && filteredSchemas.length === 0 ? (
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
                        <button
                          aria-controls={connectorChildrenId}
                          aria-expanded={expanded}
                          className={styles.connectorButton}
                          onClick={() => toggleSchema(connector.name)}
                          type="button"
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
                        </button>

                        {expanded ? (
                          <div className={styles.connectorChildren} id={connectorChildrenId}>
                            {visibleTables.map((table) => {
                              const key = tableKey(connector.name, table.name)
                              const isSelected =
                                selectedTable?.schemaName === connector.name &&
                                selectedTable.table.name === table.name

                              return (
                                <button
                                  aria-current={isSelected ? 'true' : undefined}
                                  className={classNames(styles.tableButton, {
                                    [styles.tableButtonSelected]: isSelected,
                                  })}
                                  key={key}
                                  onClick={() => handleSelectTable(connector.name, table)}
                                  type="button"
                                >
                                  <Icon color="secondary" name="Table2" size="14" />
                                  <Typography.BodyStrong as="span" className={styles.tableName}>
                                    {table.name}
                                  </Typography.BodyStrong>
                                </button>
                              )
                            })}
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
          {selectedTable ? (
            <div className={styles.detailContent}>
              <div className={styles.detailHeader}>
                <div>
                  <Typography.HeadingSmall as="h2">
                    {selectedTable.schemaName}.{selectedTable.table.name}
                  </Typography.HeadingSmall>
                  {selectedTable.table.description ? (
                    <Typography.BodySmall as="p" className={styles.description}>
                      {selectedTable.table.description}
                    </Typography.BodySmall>
                  ) : null}
                </div>
                {selectedTable.table.requiredFilters.length > 0 ? (
                  <Tooltip
                    content={`The following filters are required: ${selectedTable.table.requiredFilters.join(', ')}`}
                    side="top"
                  >
                    <div
                      aria-label={`Required filters: ${selectedTable.table.requiredFilters.join(', ')}`}
                      className={styles.requiredFilterGroup}
                      tabIndex={0}
                    >
                      {selectedTable.table.requiredFilters.map((filter) => (
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
                {selectedColumnsLoading ? (
                  <div className={styles.loadingState}>
                    <Icon
                      className={styles.spinAnimation}
                      color="secondary"
                      name="Loader"
                      size="18"
                    />
                    <Typography.BodySmall variant="tertiary">Loading columns</Typography.BodySmall>
                  </div>
                ) : selectedColumnsError ? (
                  <ErrorBanner
                    message={selectedColumnsError}
                    onRetry={() =>
                      selectedTable
                        ? loadTableColumns(selectedTable.schemaName, selectedTable.table)
                        : undefined
                    }
                    title="Failed to load columns"
                  />
                ) : selectedTable.table.columnsLoaded && selectedTable.table.columns.length > 0 ? (
                  <Table.Wrapper variant="compact">
                    <Table.Root>
                      <Table.Head>
                        <Table.Row>
                          <Table.HeaderCell>Name</Table.HeaderCell>
                          <Table.HeaderCell>Type</Table.HeaderCell>
                          <Table.HeaderCell>Description</Table.HeaderCell>
                        </Table.Row>
                      </Table.Head>
                      <Table.Body>
                        {selectedTable.table.columns.map((column) => (
                          <Table.Row
                            className={classNames({
                              [styles.virtualRow]: column.virtual,
                            })}
                            key={column.name}
                          >
                            <Table.Cell>
                              {column.name}
                              {column.filterable ? (
                                <Tooltip
                                  content="Required filter: queries for this table must include a filter on this field."
                                  side="top"
                                >
                                  <span
                                    aria-label="Required filter"
                                    className={styles.requiredStar}
                                    tabIndex={0}
                                  >
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
                ) : selectedTable.table.columnsLoaded ? (
                  <Typography.BodySmall className={styles.emptyInline} variant="tertiary">
                    No columns reported for this table.
                  </Typography.BodySmall>
                ) : (
                  <div className={styles.loadingState}>
                    <Icon
                      className={styles.spinAnimation}
                      color="secondary"
                      name="Loader"
                      size="18"
                    />
                    <Typography.BodySmall variant="tertiary">Loading columns</Typography.BodySmall>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className={styles.detailEmpty}>
              <Typography.Body variant="secondary">
                Select a table from the schema tree to inspect its columns.
              </Typography.Body>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}

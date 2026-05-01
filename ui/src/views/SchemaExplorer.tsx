import { useState, useEffect, useCallback } from 'react'
import classnames from 'classnames'

import * as Button from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Skeleton } from '@/wax/components/skeleton'
import { Table } from '@/wax/components/table'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import { ApiErrorBox } from '@/components/api-error-box'
import { ServerError } from '@/components/server-error/server-error'
import { PageHeader } from '@/components/page-header/page-header'
import { ShikiEditor } from '@/components/shiki-editor'
import { executeSchemaQuery, fetchSchemaFromCoral, type SchemaResponse, type TableDef } from '@/lib/schema'
import { buildDefaultQuery, requiredFilterSet } from './schema-explorer-queries'

import * as styles from './schema-explorer.css'

function SkeletonTree() {
  return (
    <div className={styles.skeletonContainer}>
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className={styles.skeletonGroup}>
          <Skeleton width={128} height={20} borderRadius={4} />
          <div className={styles.skeletonChildren}>
            <Skeleton width={96} height={16} borderRadius={4} />
            <Skeleton width={112} height={16} borderRadius={4} />
          </div>
        </div>
      ))}
    </div>
  )
}

export function SchemaExplorer() {
  const [schema, setSchema] = useState<SchemaResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [expandedPlugins, setExpandedConnectors] = useState<Set<string>>(new Set())
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set())
  const [selectedTable, setSelectedTable] = useState<{ connector: string; table: TableDef } | null>(null)
  const [sampleData, setSampleData] = useState<{ loading: boolean; rows?: Record<string, unknown>[]; columns?: string[]; error?: string }>({ loading: false })
  const [queryText, setQueryText] = useState('')

  const loadSchema = useCallback(async () => {
    setLoading(true)
    setLoadError(null)

    try {
      const data = await fetchSchemaFromCoral()
      setSchema(data)
      setExpandedConnectors(new Set(data.connectors.map((c) => c.name)))
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : 'Failed to load schema')
      setSchema({ connectors: [] })
    }

    setLoading(false)
  }, [])

  useEffect(() => { void loadSchema() }, [loadSchema])

  useEffect(() => {
    if (selectedTable) {
      const defaultQuery = buildDefaultQuery(selectedTable.connector, selectedTable.table)
      setQueryText(defaultQuery)
    } else {
      setQueryText('')
    }
  }, [selectedTable])

  const togglePlugin = (name: string) => {
    setExpandedConnectors((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  const toggleTable = (key: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const filteredPlugins = schema?.connectors.filter((c) => {
    if (!search) return true
    const s = search.toLowerCase()
    if (c.name.includes(s)) return true
    return c.tables.some((t) => t.name.includes(s) || t.columns.some((col) => col.name.includes(s)))
  }) ?? []

  const runQuery = useCallback(async () => {
    if (!selectedTable || !queryText.trim()) return
    setSampleData({ loading: true })

    const finalQuery = queryText.trim()
    const result = await executeSchemaQuery(finalQuery)
    if (result.error) {
      setSampleData({ loading: false, error: result.error })
    } else {
      setSampleData({ loading: false, rows: result.rows, columns: result.columns })
    }
  }, [selectedTable, queryText])

  return (
    <div className={styles.root}>
      <PageHeader title="Schema explorer">
        <Typography.Body as="span" variant="tertiary">
          {filteredPlugins.length} {filteredPlugins.length === 1 ? 'connector' : 'connectors'}
        </Typography.Body>
        <Button.Container size="32" variant="bare">
          <Button.Icon name="Plus" />
          <Button.Text>Add connector</Button.Text>
        </Button.Container>
      </PageHeader>

      <div className={styles.body}>
      <div className={styles.treePanel}>
        <div className={styles.treePanelToolbar}>
          <div className={styles.searchRow}>
            <TextInput
              value={search}
              onChange={setSearch}
              placeholder="Filter tables & columns..."
              icon="Search"
            />
            {search && (
              <button
                type="button"
                onClick={() => setSearch('')}
                className={styles.clearButton}
              >
                <Icon name="X" size="14" color="tertiary" />
              </button>
            )}
          </div>
        </div>
        <div className={styles.treeContent}>
          <ScrollArea constrainWidth>
            {loading ? (
              <SkeletonTree />
            ) : loadError ? null : search && filteredPlugins.length === 0 ? (
              <div className={styles.treeEmpty}>
                <Typography.BodySmall variant="tertiary">No results for &ldquo;{search}&rdquo;</Typography.BodySmall>
              </div>
            ) : (
              <div className={styles.treeList}>
                {filteredPlugins.map((connector) => (
                  <div key={connector.name}>
                    <button
                      type="button"
                      className={styles.connectorButton}
                      onClick={() => togglePlugin(connector.name)}
                    >
                      <Icon
                        name={expandedPlugins.has(connector.name) ? 'ChevronDown' : 'ChevronRight'}
                        size="14"
                        color="secondary"
                      />
                      <Typography.BodyStrong as="span" variant="primary">
                        {connector.name}
                      </Typography.BodyStrong>
                      <Typography.BodySmall as="span" variant="tertiary" className={styles.connectorTableCount}>
                        {connector.tables.length} tables
                      </Typography.BodySmall>
                    </button>

                    {expandedPlugins.has(connector.name) && (
                      <div className={styles.connectorChildren}>
                        {connector.tables
                          .filter((t) => !search || t.name.includes(search.toLowerCase()) || t.columns.some((c) => c.name.includes(search.toLowerCase())))
                          .map((table) => {
                            const key = `${connector.name}.${table.name}`
                            const isSelected = selectedTable?.connector === connector.name && selectedTable?.table.name === table.name
                            return (
                              <div key={key}>
                                <button
                                  type="button"
                                  className={classnames(
                                    styles.tableButton,
                                    { [styles.tableButtonSelected]: isSelected },
                                  )}
                                  onClick={() => {
                                    toggleTable(key)
                                    setSelectedTable({ connector: connector.name, table })
                                    setSampleData({ loading: false })
                                  }}
                                >
                                  <Icon
                                    name={expandedTables.has(key) ? 'ChevronDown' : 'ChevronRight'}
                                    size="14"
                                    color="secondary"
                                  />
                                  <Icon name="Table2" size="14" color="secondary" />
                                  <Typography.BodyStrong as="span" variant="primary" className={styles.tableName}>{table.name}</Typography.BodyStrong>
                                </button>

                                {expandedTables.has(key) && (
                                  <div className={styles.columnChildren}>
                                    {(() => {
                                      const required = requiredFilterSet(table)
                                      return table.columns.map((col) => (
                                      <div
                                        key={col.name}
                                        className={styles.columnRow}
                                      >
                                        <Icon name="Columns3" size="14" color="tertiary" />
                                        <Typography.BodySmall as="span" variant="primary" className={styles.columnName}>
                                          {col.name}
                                          {required.has(col.name) && <span className={styles.requiredStar}>*</span>}
                                        </Typography.BodySmall>
                                        <Typography.BodySmall variant="tertiary">{col.type}{col.virtual && ' (virtual)'}</Typography.BodySmall>
                                      </div>
                                    ))
                                    })()}
                                  </div>
                                )}
                              </div>
                            )
                          })}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </ScrollArea>
        </div>
      </div>

      <div className={styles.detailPanel}>
        {selectedTable ? (
          <div className={styles.detailContent}>
            <div>
              <Typography.HeadingSmall>
                {selectedTable.connector}.{selectedTable.table.name}
              </Typography.HeadingSmall>
              {selectedTable.table.description && (
                <Typography.BodySmall as="p" className={styles.description}>
                  {selectedTable.table.description}
                </Typography.BodySmall>
              )}
            </div>

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
                  {(() => {
                    const required = requiredFilterSet(selectedTable.table)
                    return selectedTable.table.columns.map((col) => (
                    <Table.Row key={col.name}>
                      <Table.Cell className={classnames({ [styles.virtualRow]: col.virtual })}>
                        {col.name}
                        {required.has(col.name) && <span className={styles.requiredStar}>*</span>}
                      </Table.Cell>
                      <Table.Cell>{col.type}{col.virtual && ' (virtual)'}</Table.Cell>
                      <Table.Cell mono={false} className={styles.cellTruncate}>
                        {col.description ?? '\u2014'}
                      </Table.Cell>
                    </Table.Row>
                  ))
                  })()}
                </Table.Body>
              </Table.Root>
            </Table.Wrapper>

            <div className={styles.section}>
              <Typography.BodySmallStrong>Query</Typography.BodySmallStrong>
              <ShikiEditor
                className={styles.queryEditor}
                value={queryText}
                onChange={setQueryText}
                placeholder={selectedTable ? buildDefaultQuery(selectedTable.connector, selectedTable.table) : undefined}
              />
              <div className={styles.queryActions}>
                <Tooltip content="Execute the SQL query" side="bottom">
                  <Button.Container
                    variant="primary"
                    size="32"
                    onClick={() => void runQuery()}
                    disabled={sampleData.loading || !queryText.trim()}
                  >
                    <Button.Icon name={sampleData.loading ? 'Loader' : 'Play'} />
                    <Button.Text>Execute</Button.Text>
                  </Button.Container>
                </Tooltip>
              </div>
            </div>

            <div className={styles.section}>
              {sampleData.error && (
                <ApiErrorBox error={sampleData.error} />
              )}
              {sampleData.rows && (() => {
                const cols = sampleData.columns ?? selectedTable.table.columns.map(c => c.name)
                return (
                  <Table.Wrapper>
                    <Table.Root>
                      <Table.Head>
                        <Table.Row>
                          {cols.map((col) => (
                            <Table.HeaderCell key={col}>{col}</Table.HeaderCell>
                          ))}
                        </Table.Row>
                      </Table.Head>
                      <Table.Body>
                        {sampleData.rows!.map((row, i) => (
                          <Table.Row key={i}>
                            {cols.map((col) => (
                              <Table.Cell key={col}>
                                {String(row[col] ?? '')}
                              </Table.Cell>
                            ))}
                          </Table.Row>
                        ))}
                      </Table.Body>
                    </Table.Root>
                  </Table.Wrapper>
                )
              })()}
            </div>
          </div>
        ) : (
          <div className={styles.detailEmpty}>
            {loadError ? (
              <ServerError title="Failed to load schema" error={loadError} />
            ) : (
              <div className={styles.detailEmptyCenter}>
                <Typography.Body variant="secondary">
                  Select a table from the tree to view its schema
                </Typography.Body>
              </div>
            )}
          </div>
        )}
      </div>
      </div>
    </div>
  )
}

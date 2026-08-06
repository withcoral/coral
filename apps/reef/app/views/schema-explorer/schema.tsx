import { useMemo, useState } from 'react'
import { NavLink, Outlet, useParams, useRouteError } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import type {
  SchemaGroup,
  SchemaItemDef,
  SchemaResponse,
  TableDef,
  TableFunctionDef,
} from '@/lib/schema-explorer'
import { routePath } from '@/routing/routemap'
import { PageHeader } from '@/views/traces/page-header'
import { Container as ButtonContainer } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'
import { formatError, useRouteRetry } from './shared'

export function findSchemaTable(
  schema: SchemaResponse,
  catalogName: string | undefined,
  schemaName: string,
  tableName: string,
): TableDef | undefined {
  return schema.connectors
    .find((connector) => connector.catalogName === catalogName && connector.name === schemaName)
    ?.items.find((item): item is TableDef => item.kind === 'table' && item.name === tableName)
}

export function findSchemaTableFunction(
  schema: SchemaResponse,
  catalogName: string | undefined,
  schemaName: string,
  functionName: string,
): TableFunctionDef | undefined {
  return schema.connectors
    .find((connector) => connector.catalogName === catalogName && connector.name === schemaName)
    ?.items.find(
      (item): item is TableFunctionDef =>
        item.kind === 'tableFunction' && item.name === functionName,
    )
}

function schemaMatchesSearch(schema: SchemaGroup, search: string) {
  if (!search) return true
  return (
    schemaNameMatchesSearch(schema, search) ||
    schema.items.some((item) => catalogItemMatchesSearch(item, search))
  )
}

function schemaNameMatchesSearch(schema: SchemaGroup, search: string) {
  return schemaDisplayName(schema).toLowerCase().includes(search)
}

function catalogItemMatchesSearch(item: SchemaItemDef, search: string) {
  if (!search) return true
  if (item.name.toLowerCase().includes(search)) return true
  if (item.description?.toLowerCase().includes(search)) return true
  if (
    item.kind === 'tableFunction' &&
    item.arguments.some((argument) => argument.name.toLowerCase().includes(search))
  ) {
    return true
  }
  return false
}

function visibleItemsForSchema(schema: SchemaGroup, search: string) {
  if (!search || schemaNameMatchesSearch(schema, search)) return schema.items
  return schema.items.filter((item) => catalogItemMatchesSearch(item, search))
}

function schemaKey(schema: SchemaGroup) {
  return JSON.stringify([schema.catalogName ?? null, schema.name])
}

function schemaDisplayName(schema: SchemaGroup) {
  return schema.catalogName ? `${schema.catalogName}.${schema.name}` : schema.name
}

function catalogItemPath(workspaceId: string, schema: SchemaGroup, item: SchemaItemDef) {
  if (schema.catalogName) {
    return item.kind === 'table'
      ? routePath('workspaceSchemaCatalogTable', {
          catalogName: schema.catalogName,
          schemaName: schema.name,
          tableName: item.name,
          workspaceId,
        })
      : routePath('workspaceSchemaCatalogTableFunction', {
          catalogName: schema.catalogName,
          functionName: item.name,
          schemaName: schema.name,
          workspaceId,
        })
  }
  return item.kind === 'table'
    ? routePath('workspaceSchemaTable', {
        schemaName: schema.name,
        tableName: item.name,
        workspaceId,
      })
    : routePath('workspaceSchemaTableFunction', {
        functionName: item.name,
        schemaName: schema.name,
        workspaceId,
      })
}

export function tableFunctionTreeArguments(tableFunction: TableFunctionDef) {
  const argumentsToShow = tableFunction.arguments
    .filter((argument) => argument.required)
    .map((argument) => argument.name)
  if (tableFunction.arguments.some((argument) => !argument.required)) argumentsToShow.push('...')
  return `(${argumentsToShow.join(', ')})`
}

// Header + two-panel scaffold for the error state so it looks like the loaded page.
function Frame({ children }: { children: React.ReactNode }) {
  return (
    <section aria-label="Schema explorer" className={styles.root}>
      <PageHeader title="Schema explorer" />
      {children}
    </section>
  )
}

// Route ErrorBoundary for /workspaces/:workspaceId/schema: the schema is this
// page's critical data, so a failed load errors the whole page (re-exported by
// the route module).
export function SchemaExplorerError() {
  const error = useRouteError()
  const retry = useRouteRetry()
  return (
    <Frame>
      <div className={styles.body}>
        <div className={styles.treePanel}>
          <div className={styles.treeContent}>
            <div className={styles.treeError}>
              <ErrorBanner
                message={formatError(error)}
                onRetry={retry}
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

// The schema is awaited in the server loader (critical data): the global
// navigation progress bar is the pending UI and failures land in the route
// ErrorBoundary, so this view only renders the loaded state.
export function SchemaExplorer({
  schema,
  workspaceId,
}: {
  schema: SchemaResponse
  workspaceId: string
}) {
  const activeItem = useParams()
  const [search, setSearch] = useState('')
  // Collapsed by default so the initial render is one row per schema, not one per
  // catalog item — expanding every schema up front renders hundreds of rows and makes the
  // first paint sluggish. A search expands matching schemas (see `expanded` below).
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(() => new Set())

  const normalizedSearch = search.trim().toLowerCase()
  const filteredSchemas = useMemo(
    () => schema.connectors.filter((connector) => schemaMatchesSearch(connector, normalizedSearch)),
    [normalizedSearch, schema],
  )

  const toggleSchema = (key: string) => {
    setExpandedSchemas((previous) => {
      const next = new Set(previous)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  return (
    <section aria-label="Schema explorer" className={styles.root}>
      <PageHeader title="Schema explorer">
        <div className={styles.headerSearch}>
          <TextInput
            icon="Search"
            onChange={setSearch}
            placeholder="Filter schemas and tables"
            type="search"
            value={search}
          />
        </div>
      </PageHeader>

      <div className={styles.body}>
        <div className={styles.treePanel}>
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
                    No queryable tables or table functions in this workspace.
                  </Typography.BodySmall>
                </div>
              ) : (
                <div className={styles.treeList}>
                  {filteredSchemas.map((connector) => {
                    const connectorKey = schemaKey(connector)
                    // A search forces matching schemas open so results are visible.
                    const expanded = normalizedSearch !== '' || expandedSchemas.has(connectorKey)
                    const connectorChildrenId = `schema-${encodeURIComponent(connectorKey)}-items`
                    const visibleItems = visibleItemsForSchema(connector, normalizedSearch)
                    return (
                      <div key={connectorKey}>
                        <ButtonContainer
                          aria-controls={expanded ? connectorChildrenId : undefined}
                          aria-expanded={expanded}
                          className={styles.treeRow}
                          fullWidth
                          onClick={() => toggleSchema(connectorKey)}
                          size="22"
                          variant="bare"
                        >
                          <Icon
                            color="secondary"
                            name={expanded ? 'ChevronDown' : 'ChevronRight'}
                            size="14"
                          />
                          <Typography.BodyStrong className={styles.connectorName}>
                            {schemaDisplayName(connector)}
                          </Typography.BodyStrong>
                          <Typography.BodySmall
                            className={styles.connectorItemCount}
                            variant="tertiary"
                          >
                            {visibleItems.length}
                          </Typography.BodySmall>
                        </ButtonContainer>

                        {expanded ? (
                          <div className={styles.connectorChildren} id={connectorChildrenId}>
                            {visibleItems.map((item) => {
                              const path = catalogItemPath(workspaceId, connector, item)
                              const active =
                                activeItem.catalogName === connector.catalogName &&
                                activeItem.schemaName === connector.name &&
                                (item.kind === 'table'
                                  ? activeItem.tableName === item.name
                                  : activeItem.functionName === item.name)
                              return (
                                <ButtonContainer
                                  as={NavLink}
                                  className={styles.treeRow}
                                  fullWidth
                                  isActive={active}
                                  key={path}
                                  size="22"
                                  to={path}
                                  variant="bare"
                                >
                                  <Icon
                                    color="secondary"
                                    name={item.kind === 'table' ? 'Table2' : 'SquareFunction'}
                                    size="14"
                                  />
                                  {item.kind === 'table' ? (
                                    <Typography.BodyStrong className={styles.itemName}>
                                      {item.name}
                                    </Typography.BodyStrong>
                                  ) : (
                                    <span className={styles.itemName}>
                                      <Typography.BodyStrong>{item.name}</Typography.BodyStrong>
                                      <Typography.Body>
                                        {tableFunctionTreeArguments(item)}
                                      </Typography.Body>
                                    </span>
                                  )}
                                </ButtonContainer>
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
          <Outlet context={schema} />
        </div>
      </div>
    </section>
  )
}

import { useMemo, useState } from 'react'
import { NavLink, Outlet, useParams, useRouteError } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import type {
  CatalogGroup,
  SchemaGroup,
  SchemaItemDef,
  SchemaResponse,
  SchemaRoot,
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
  catalogName: string,
  schemaName: string,
  tableName: string,
): TableDef | undefined {
  const schemaGroup = findSchemaGroup(schema, catalogName, schemaName)
  return schemaGroup?.items.find(
    (item): item is TableDef => item.kind === 'table' && item.name === tableName,
  )
}

export function findSchemaTableFunction(
  schema: SchemaResponse,
  catalogName: string,
  schemaName: string,
  functionName: string,
): TableFunctionDef | undefined {
  const schemaGroup = findSchemaGroup(schema, catalogName, schemaName)
  return schemaGroup?.items.find(
    (item): item is TableFunctionDef => item.kind === 'tableFunction' && item.name === functionName,
  )
}

function findSchemaGroup(
  schema: SchemaResponse,
  catalogName: string,
  schemaName: string,
): SchemaGroup | undefined {
  const root = schema.roots.find((candidate) =>
    catalogName
      ? candidate.kind === 'catalog' && candidate.catalog.name === catalogName
      : candidate.kind === 'schema' && candidate.schema.name === schemaName,
  )
  if (!root) return undefined
  return root.kind === 'schema'
    ? root.schema
    : root.catalog.schemas.find((candidate) => candidate.name === schemaName)
}

function catalogMatchesSearch(catalog: CatalogGroup, search: string) {
  if (!search || catalog.name.toLowerCase().includes(search)) return true
  return catalog.schemas.some((schema) => schemaMatchesSearch(schema, search))
}

function rootMatchesSearch(root: SchemaRoot, search: string) {
  return root.kind === 'schema'
    ? schemaMatchesSearch(root.schema, search)
    : catalogMatchesSearch(root.catalog, search)
}

function schemaMatchesSearch(schema: SchemaGroup, search: string) {
  if (!search) return true
  return (
    schemaNameMatchesSearch(schema, search) ||
    schema.items.some((item) => catalogItemMatchesSearch(item, search))
  )
}

function schemaNameMatchesSearch(schema: SchemaGroup, search: string) {
  return schema.name.toLowerCase().includes(search)
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

function visibleSchemasForCatalog(catalog: CatalogGroup, search: string) {
  if (!search || catalog.name.toLowerCase().includes(search)) return catalog.schemas
  return catalog.schemas.filter((schema) => schemaMatchesSearch(schema, search))
}

function tablePath(
  workspaceId: string,
  catalogName: string,
  schemaName: string,
  tableName: string,
) {
  return catalogName
    ? routePath('workspaceCatalogSchemaTable', {
        catalogName,
        schemaName,
        tableName,
        workspaceId,
      })
    : routePath('workspaceSchemaTable', { schemaName, tableName, workspaceId })
}

function tableFunctionPath(
  workspaceId: string,
  catalogName: string,
  schemaName: string,
  functionName: string,
) {
  return catalogName
    ? routePath('workspaceCatalogSchemaTableFunction', {
        catalogName,
        functionName,
        schemaName,
        workspaceId,
      })
    : routePath('workspaceSchemaTableFunction', {
        functionName,
        schemaName,
        workspaceId,
      })
}

function catalogItemPath(
  workspaceId: string,
  catalogName: string,
  schemaName: string,
  item: SchemaItemDef,
) {
  return item.kind === 'table'
    ? tablePath(workspaceId, catalogName, schemaName, item.name)
    : tableFunctionPath(workspaceId, catalogName, schemaName, item.name)
}

function expansionKey(catalogName: string, schemaName: string) {
  return `${catalogName}\0${schemaName}`
}

export function schemaTreeChildrenId(
  kind: 'catalog' | 'catalogSchema' | 'schema',
  ...names: string[]
) {
  return `schema-tree-${kind}-${encodeURIComponent(JSON.stringify(names))}`
}

function toggleExpanded(
  setExpanded: React.Dispatch<React.SetStateAction<Set<string>>>,
  key: string,
) {
  setExpanded((previous) => {
    const next = new Set(previous)
    if (next.has(key)) next.delete(key)
    else next.add(key)
    return next
  })
}

export function tableFunctionTreeArguments(tableFunction: TableFunctionDef) {
  const argumentsToShow = tableFunction.arguments
    .filter((argument) => argument.required)
    .map((argument) => argument.name)
  if (tableFunction.arguments.some((argument) => !argument.required)) argumentsToShow.push('...')
  return `(${argumentsToShow.join(', ')})`
}

function SchemaItemLinks({
  activeItem,
  catalogName,
  childrenId,
  items,
  schemaName,
  workspaceId,
}: {
  activeItem: Readonly<Record<string, string | undefined>>
  catalogName: string
  childrenId: string
  items: SchemaItemDef[]
  schemaName: string
  workspaceId: string
}) {
  return (
    <div className={styles.connectorChildren} id={childrenId}>
      {items.map((item) => {
        const path = catalogItemPath(workspaceId, catalogName, schemaName, item)
        const active =
          activeItem.schemaName === schemaName &&
          (catalogName ? activeItem.catalogName === catalogName : !activeItem.catalogName) &&
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
              <Typography.BodyStrong className={styles.itemName}>{item.name}</Typography.BodyStrong>
            ) : (
              <span className={styles.itemName}>
                <Typography.BodyStrong>{item.name}</Typography.BodyStrong>
                <Typography.Body>{tableFunctionTreeArguments(item)}</Typography.Body>
              </span>
            )}
          </ButtonContainer>
        )
      })}
    </div>
  )
}

function CatalogTreeGroup({
  activeItem,
  catalog,
  expandedCatalogs,
  expandedSchemas,
  normalizedSearch,
  setExpandedCatalogs,
  setExpandedSchemas,
  workspaceId,
}: {
  activeItem: Readonly<Record<string, string | undefined>>
  catalog: CatalogGroup
  expandedCatalogs: Set<string>
  expandedSchemas: Set<string>
  normalizedSearch: string
  setExpandedCatalogs: React.Dispatch<React.SetStateAction<Set<string>>>
  setExpandedSchemas: React.Dispatch<React.SetStateAction<Set<string>>>
  workspaceId: string
}) {
  const catalogMatches = catalog.name.toLowerCase().includes(normalizedSearch)
  const descendantSearch = catalogMatches ? '' : normalizedSearch
  const visibleSchemas = visibleSchemasForCatalog(catalog, normalizedSearch)
  const expanded = normalizedSearch !== '' || expandedCatalogs.has(catalog.name)
  const catalogChildrenId = schemaTreeChildrenId('catalog', catalog.name)
  return (
    <div>
      <ButtonContainer
        aria-controls={expanded ? catalogChildrenId : undefined}
        aria-expanded={expanded}
        className={styles.treeRow}
        fullWidth
        onClick={() => toggleExpanded(setExpandedCatalogs, catalog.name)}
        size="22"
        variant="bare"
      >
        <Icon color="secondary" name={expanded ? 'ChevronDown' : 'ChevronRight'} size="14" />
        <Typography.BodyStrong className={styles.connectorName}>
          {catalog.name}
        </Typography.BodyStrong>
        <Typography.BodySmall className={styles.connectorItemCount} variant="tertiary">
          {visibleSchemas.length}
        </Typography.BodySmall>
      </ButtonContainer>

      {expanded ? (
        <div className={styles.connectorChildren} id={catalogChildrenId}>
          {visibleSchemas.map((providerSchema) => {
            const schemaKey = expansionKey(catalog.name, providerSchema.name)
            const schemaExpanded = normalizedSearch !== '' || expandedSchemas.has(schemaKey)
            const schemaChildrenId = schemaTreeChildrenId(
              'catalogSchema',
              catalog.name,
              providerSchema.name,
            )
            const visibleItems = visibleItemsForSchema(providerSchema, descendantSearch)
            return (
              <div key={schemaKey}>
                <ButtonContainer
                  aria-controls={schemaExpanded ? schemaChildrenId : undefined}
                  aria-expanded={schemaExpanded}
                  className={styles.treeRow}
                  fullWidth
                  onClick={() => toggleExpanded(setExpandedSchemas, schemaKey)}
                  size="22"
                  variant="bare"
                >
                  <Icon
                    color="secondary"
                    name={schemaExpanded ? 'ChevronDown' : 'ChevronRight'}
                    size="14"
                  />
                  <Typography.BodyStrong className={styles.connectorName}>
                    {providerSchema.name}
                  </Typography.BodyStrong>
                  <Typography.BodySmall className={styles.connectorItemCount} variant="tertiary">
                    {visibleItems.length}
                  </Typography.BodySmall>
                </ButtonContainer>

                {schemaExpanded ? (
                  <SchemaItemLinks
                    activeItem={activeItem}
                    catalogName={catalog.name}
                    childrenId={schemaChildrenId}
                    items={visibleItems}
                    schemaName={providerSchema.name}
                    workspaceId={workspaceId}
                  />
                ) : null}
              </div>
            )
          })}
        </div>
      ) : null}
    </div>
  )
}

function SchemaTreeGroup({
  activeItem,
  expandedSchemas,
  normalizedSearch,
  schema,
  setExpandedSchemas,
  workspaceId,
}: {
  activeItem: Readonly<Record<string, string | undefined>>
  expandedSchemas: Set<string>
  normalizedSearch: string
  schema: SchemaGroup
  setExpandedSchemas: React.Dispatch<React.SetStateAction<Set<string>>>
  workspaceId: string
}) {
  const expanded = normalizedSearch !== '' || expandedSchemas.has(schema.name)
  const schemaChildrenId = schemaTreeChildrenId('schema', schema.name)
  const visibleItems = visibleItemsForSchema(schema, normalizedSearch)
  return (
    <div>
      <ButtonContainer
        aria-controls={expanded ? schemaChildrenId : undefined}
        aria-expanded={expanded}
        className={styles.treeRow}
        fullWidth
        onClick={() => toggleExpanded(setExpandedSchemas, schema.name)}
        size="22"
        variant="bare"
      >
        <Icon color="secondary" name={expanded ? 'ChevronDown' : 'ChevronRight'} size="14" />
        <Typography.BodyStrong className={styles.connectorName}>
          {schema.name}
        </Typography.BodyStrong>
        <Typography.BodySmall className={styles.connectorItemCount} variant="tertiary">
          {visibleItems.length}
        </Typography.BodySmall>
      </ButtonContainer>

      {expanded ? (
        <SchemaItemLinks
          activeItem={activeItem}
          catalogName=""
          childrenId={schemaChildrenId}
          items={visibleItems}
          schemaName={schema.name}
          workspaceId={workspaceId}
        />
      ) : null}
    </div>
  )
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
  // Collapsed by default so the initial render is one row per schema or catalog.
  // A search exposes matching descendants without mutating these expansion sets.
  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(() => new Set())
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(() => new Set())

  const normalizedSearch = search.trim().toLowerCase()
  const filteredRoots = useMemo(
    () => schema.roots.filter((root) => rootMatchesSearch(root, normalizedSearch)),
    [normalizedSearch, schema.roots],
  )

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
              {normalizedSearch && filteredRoots.length === 0 ? (
                <div className={styles.treeEmpty}>
                  <Typography.BodySmall variant="tertiary">
                    No results for "{search}"
                  </Typography.BodySmall>
                </div>
              ) : filteredRoots.length === 0 ? (
                <div className={styles.treeEmpty}>
                  <Typography.BodySmall variant="tertiary">
                    No queryable tables or table functions in this workspace.
                  </Typography.BodySmall>
                </div>
              ) : (
                <div className={styles.treeList}>
                  {filteredRoots.map((root) =>
                    root.kind === 'schema' ? (
                      <SchemaTreeGroup
                        activeItem={activeItem}
                        expandedSchemas={expandedSchemas}
                        key={`schema:${root.schema.name}`}
                        normalizedSearch={normalizedSearch}
                        schema={root.schema}
                        setExpandedSchemas={setExpandedSchemas}
                        workspaceId={workspaceId}
                      />
                    ) : (
                      <CatalogTreeGroup
                        activeItem={activeItem}
                        catalog={root.catalog}
                        expandedCatalogs={expandedCatalogs}
                        expandedSchemas={expandedSchemas}
                        key={`catalog:${root.catalog.name}`}
                        normalizedSearch={normalizedSearch}
                        setExpandedCatalogs={setExpandedCatalogs}
                        setExpandedSchemas={setExpandedSchemas}
                        workspaceId={workspaceId}
                      />
                    ),
                  )}
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

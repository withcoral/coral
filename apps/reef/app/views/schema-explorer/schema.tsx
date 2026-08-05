import { useMemo, useState } from 'react'
import { NavLink, Outlet, useParams, useRouteError } from 'react-router'

import { ErrorBanner } from '@/components/error-banner'
import type {
  CatalogNamespace,
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
  catalogName: string,
  schemaName: string,
  tableName: string,
): TableDef | undefined {
  let schemaGroup: SchemaGroup | undefined
  if (catalogName) {
    schemaGroup = schema.namespaces
      .find(
        (namespace): namespace is CatalogNamespace =>
          namespace.kind === 'catalog' && namespace.name === catalogName,
      )
      ?.schemas.find((providerSchema) => providerSchema.name === schemaName)
  } else {
    const namespace = schema.namespaces.find(
      (candidate) => candidate.kind === 'schema' && candidate.name === schemaName,
    )
    if (namespace?.kind === 'schema') schemaGroup = namespace
  }
  return schemaGroup?.items.find(
    (item): item is TableDef => item.kind === 'table' && item.name === tableName,
  )
}

export function findSchemaTableFunction(
  schema: SchemaResponse,
  schemaName: string,
  functionName: string,
): TableFunctionDef | undefined {
  const schemaGroup = schema.namespaces.find(
    (namespace) => namespace.kind === 'schema' && namespace.name === schemaName,
  )
  if (!schemaGroup || schemaGroup.kind !== 'schema') return undefined
  return schemaGroup.items.find(
    (item): item is TableFunctionDef => item.kind === 'tableFunction' && item.name === functionName,
  )
}

function catalogMatchesSearch(catalog: CatalogNamespace, search: string) {
  if (!search || catalog.name.toLowerCase().includes(search)) return true
  return catalog.schemas.some((schema) => schemaMatchesSearch(schema, search))
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

function visibleSchemasForCatalog(catalog: CatalogNamespace, search: string) {
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
    ? routePath('workspaceSchemaCatalogTable', {
        catalogName,
        schemaName,
        tableName,
        workspaceId,
      })
    : routePath('workspaceSchemaTable', { schemaName, tableName, workspaceId })
}

function tableFunctionPath(workspaceId: string, schemaName: string, functionName: string) {
  return routePath('workspaceSchemaTableFunction', { functionName, schemaName, workspaceId })
}

function catalogItemPath(
  workspaceId: string,
  catalogName: string,
  schemaName: string,
  item: SchemaItemDef,
) {
  return item.kind === 'table'
    ? tablePath(workspaceId, catalogName, schemaName, item.name)
    : tableFunctionPath(workspaceId, schemaName, item.name)
}

function expansionKey(catalogName: string, schemaName: string) {
  return `${catalogName}\u0000${schemaName}`
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
  // Collapsed by default so the initial render is one row per root namespace.
  // A search exposes matching descendants without mutating these expansion sets.
  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(() => new Set())
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(() => new Set())

  const normalizedSearch = search.trim().toLowerCase()
  const filteredNamespaces = useMemo(
    () =>
      schema.namespaces.filter((namespace) =>
        namespace.kind === 'catalog'
          ? catalogMatchesSearch(namespace, normalizedSearch)
          : schemaMatchesSearch(namespace, normalizedSearch),
      ),
    [normalizedSearch, schema],
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
              {normalizedSearch && filteredNamespaces.length === 0 ? (
                <div className={styles.treeEmpty}>
                  <Typography.BodySmall variant="tertiary">
                    No results for "{search}"
                  </Typography.BodySmall>
                </div>
              ) : filteredNamespaces.length === 0 ? (
                <div className={styles.treeEmpty}>
                  <Typography.BodySmall variant="tertiary">
                    No queryable tables or table functions in this workspace.
                  </Typography.BodySmall>
                </div>
              ) : (
                <div className={styles.treeList}>
                  {filteredNamespaces.map((namespace) => {
                    if (namespace.kind === 'catalog') {
                      const catalogMatches = namespace.name.toLowerCase().includes(normalizedSearch)
                      const descendantSearch = catalogMatches ? '' : normalizedSearch
                      const visibleSchemas = visibleSchemasForCatalog(namespace, normalizedSearch)
                      const expanded =
                        normalizedSearch !== '' || expandedCatalogs.has(namespace.name)
                      const catalogChildrenId = `catalog-${namespace.name}-schemas`
                      return (
                        <div key={`catalog:${namespace.name}`}>
                          <ButtonContainer
                            aria-controls={expanded ? catalogChildrenId : undefined}
                            aria-expanded={expanded}
                            className={styles.treeRow}
                            fullWidth
                            onClick={() => toggleExpanded(setExpandedCatalogs, namespace.name)}
                            size="22"
                            variant="bare"
                          >
                            <Icon
                              color="secondary"
                              name={expanded ? 'ChevronDown' : 'ChevronRight'}
                              size="14"
                            />
                            <Typography.BodyStrong className={styles.connectorName}>
                              {namespace.name}
                            </Typography.BodyStrong>
                            <Typography.BodySmall
                              className={styles.connectorItemCount}
                              variant="tertiary"
                            >
                              {visibleSchemas.length}
                            </Typography.BodySmall>
                          </ButtonContainer>

                          {expanded ? (
                            <div className={styles.connectorChildren} id={catalogChildrenId}>
                              {visibleSchemas.map((providerSchema) => {
                                const schemaKey = expansionKey(namespace.name, providerSchema.name)
                                const schemaExpanded =
                                  normalizedSearch !== '' || expandedSchemas.has(schemaKey)
                                const schemaChildrenId = `catalog-${namespace.name}-schema-${providerSchema.name}-items`
                                const visibleItems = visibleItemsForSchema(
                                  providerSchema,
                                  descendantSearch,
                                )
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
                                      <Typography.BodySmall
                                        className={styles.connectorItemCount}
                                        variant="tertiary"
                                      >
                                        {visibleItems.length}
                                      </Typography.BodySmall>
                                    </ButtonContainer>

                                    {schemaExpanded ? (
                                      <SchemaItemLinks
                                        activeItem={activeItem}
                                        catalogName={namespace.name}
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

                    const expanded = normalizedSearch !== '' || expandedSchemas.has(namespace.name)
                    const connectorChildrenId = `schema-${namespace.name}-items`
                    const visibleItems = visibleItemsForSchema(namespace, normalizedSearch)
                    return (
                      <div key={`schema:${namespace.name}`}>
                        <ButtonContainer
                          aria-controls={expanded ? connectorChildrenId : undefined}
                          aria-expanded={expanded}
                          className={styles.treeRow}
                          fullWidth
                          onClick={() => toggleExpanded(setExpandedSchemas, namespace.name)}
                          size="22"
                          variant="bare"
                        >
                          <Icon
                            color="secondary"
                            name={expanded ? 'ChevronDown' : 'ChevronRight'}
                            size="14"
                          />
                          <Typography.BodyStrong className={styles.connectorName}>
                            {namespace.name}
                          </Typography.BodyStrong>
                          <Typography.BodySmall
                            className={styles.connectorItemCount}
                            variant="tertiary"
                          >
                            {visibleItems.length}
                          </Typography.BodySmall>
                        </ButtonContainer>

                        {expanded ? (
                          <SchemaItemLinks
                            activeItem={activeItem}
                            catalogName=""
                            childrenId={connectorChildrenId}
                            items={visibleItems}
                            schemaName={namespace.name}
                            workspaceId={workspaceId}
                          />
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

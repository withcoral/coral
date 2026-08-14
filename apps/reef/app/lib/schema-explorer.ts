import { create } from '@bufbuild/protobuf'
import type { Client } from '@connectrpc/connect'

import {
  CatalogItemKind,
  ListCatalogRequestSchema,
  ListColumnsRequestSchema,
  type CatalogItem,
  type CatalogService,
  type ListColumnsResponse,
} from '@/generated/coral/v1/catalog_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'

export type CatalogClient = Client<typeof CatalogService>

export interface ColumnDef {
  description?: string
  filterable: boolean
  name: string
  nullable: boolean
  ordinalPosition: number
  type: string
  virtual: boolean
}

export interface TableDef {
  columns: ColumnDef[]
  columnsLoaded: boolean
  description?: string
  kind: 'table'
  name: string
  requiredFilters: string[]
}

export interface TableFunctionArgumentDef {
  name: string
  required: boolean
  values: string[]
}

export interface TableFunctionResultColumnDef {
  description?: string
  name: string
  nullable: boolean
  type: string
}

export interface TableFunctionDef {
  arguments: TableFunctionArgumentDef[]
  description?: string
  kind: 'tableFunction'
  name: string
  resultColumns: TableFunctionResultColumnDef[]
}

export type SchemaItemDef = TableDef | TableFunctionDef

export interface SchemaGroup {
  items: SchemaItemDef[]
  name: string
}

export interface CatalogGroup {
  name: string
  schemas: SchemaGroup[]
}

export type SchemaRoot =
  | { kind: 'schema'; schema: SchemaGroup }
  | { catalog: CatalogGroup; kind: 'catalog' }

export interface SchemaResponse {
  roots: SchemaRoot[]
}

export interface TableReference {
  catalogName: string
  schemaName: string
  tableName: string
}

const COLUMN_PAGE_LIMIT = 200
const COLUMN_PAGE_CONCURRENCY = 6

export async function fetchSchemaFromCoral(
  catalogClient: CatalogClient,
  workspace: Workspace,
  signal?: AbortSignal,
): Promise<SchemaResponse> {
  const catalogItems = await listCatalogItems(catalogClient, workspace, signal)
  if (catalogItems.length === 0) return { roots: [] }

  const roots: SchemaRoot[] = []
  const schemaGroups = new Map<string, SchemaGroup>()
  const catalogGroups = new Map<
    string,
    { catalog: CatalogGroup; schemas: Map<string, SchemaGroup> }
  >()

  const schemaItems = (catalogName: string, schemaName: string): SchemaItemDef[] => {
    if (!catalogName) {
      let schema = schemaGroups.get(schemaName)
      if (!schema) {
        schema = { items: [], name: schemaName }
        schemaGroups.set(schemaName, schema)
        roots.push({ kind: 'schema', schema })
      }
      return schema.items
    }

    let catalog = catalogGroups.get(catalogName)
    if (!catalog) {
      const catalogGroup: CatalogGroup = { name: catalogName, schemas: [] }
      catalog = { catalog: catalogGroup, schemas: new Map() }
      catalogGroups.set(catalogName, catalog)
      roots.push({ catalog: catalogGroup, kind: 'catalog' })
    }

    let schema = catalog.schemas.get(schemaName)
    if (!schema) {
      schema = { items: [], name: schemaName }
      catalog.schemas.set(schemaName, schema)
      catalog.catalog.schemas.push(schema)
    }
    return schema.items
  }

  for (const item of catalogItems) {
    if (item.item.case === 'table') {
      const table = item.item.value
      if (!table.schemaName || !table.name) continue

      schemaItems(table.catalogName, table.schemaName).push({
        columns: [],
        columnsLoaded: false,
        description: optional(table.description),
        kind: 'table',
        name: table.name,
        requiredFilters: table.requiredFilters,
      })
      continue
    }

    if (item.item.case === 'tableFunction') {
      const tableFunction = item.item.value
      if (!tableFunction.schemaName || !tableFunction.name) continue

      schemaItems(tableFunction.catalogName, tableFunction.schemaName).push({
        arguments: tableFunction.arguments.map((argument) => ({
          name: argument.name,
          required: argument.required,
          values: argument.values,
        })),
        description: optional(tableFunction.description),
        kind: 'tableFunction',
        name: tableFunction.name,
        resultColumns: tableFunction.resultColumns.map((column) => ({
          description: optional(column.description),
          name: column.name,
          nullable: column.nullable,
          type: column.dataType || 'unknown',
        })),
      })
    }
  }

  return { roots }
}

async function listCatalogItems(
  catalogClient: CatalogClient,
  workspace: Workspace,
  signal?: AbortSignal,
): Promise<CatalogItem[]> {
  const response = await catalogClient.listCatalog(
    create(ListCatalogRequestSchema, {
      kind: CatalogItemKind.UNSPECIFIED,
      workspace,
    }),
    { signal },
  )

  return response.items
}

export async function fetchTableColumnsFromCoral(
  catalogClient: CatalogClient,
  workspace: Workspace,
  table: TableReference,
  signal?: AbortSignal,
): Promise<ColumnDef[]> {
  const firstPage = await listColumnsPage(catalogClient, workspace, table, 0, signal)
  const columns = columnsFromResponse(firstPage)
  const pagination = firstPage.pagination
  if (!pagination?.hasMore) return columns

  const nextOffset = pagination.nextOffset || COLUMN_PAGE_LIMIT
  if (pagination.totalCount > nextOffset) {
    const remainingPages = await mapWithConcurrency(
      pageOffsets(nextOffset, pagination.totalCount, COLUMN_PAGE_LIMIT),
      COLUMN_PAGE_CONCURRENCY,
      (offset) => listColumnsPage(catalogClient, workspace, table, offset, signal),
    )
    return columns.concat(remainingPages.flatMap(columnsFromResponse))
  }

  let offset = pagination.nextOffset
  while (offset > 0) {
    const page = await listColumnsPage(catalogClient, workspace, table, offset, signal)
    columns.push(...columnsFromResponse(page))
    if (!page.pagination?.hasMore) break
    offset = page.pagination.nextOffset
  }
  return columns
}

async function listColumnsPage(
  catalogClient: CatalogClient,
  workspace: Workspace,
  table: TableReference,
  offset: number,
  signal?: AbortSignal,
): Promise<ListColumnsResponse> {
  return catalogClient.listColumns(
    create(ListColumnsRequestSchema, {
      pagination: {
        limit: COLUMN_PAGE_LIMIT,
        offset,
      },
      catalogName: table.catalogName,
      schemaName: table.schemaName,
      tableName: table.tableName,
      workspace,
    }),
    { signal },
  )
}

function columnsFromResponse(response: ListColumnsResponse): ColumnDef[] {
  return response.columns.flatMap((result) =>
    result.column
      ? [
          {
            description: optional(result.column.description),
            filterable: result.column.isRequiredFilter,
            name: result.column.name,
            nullable: result.column.nullable,
            ordinalPosition: result.column.ordinalPosition,
            type: result.column.dataType || 'unknown',
            virtual: result.column.isVirtual,
          },
        ]
      : [],
  )
}

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = Array.from<R>({ length: items.length })
  let cursor = 0

  async function run(): Promise<void> {
    while (cursor < items.length) {
      const index = cursor
      cursor += 1
      results[index] = await worker(items[index])
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run))
  return results
}

function pageOffsets(firstOffset: number, totalCount: number, limit: number): number[] {
  const offsets: number[] = []
  for (let offset = firstOffset; offset < totalCount; offset += limit) {
    offsets.push(offset)
  }
  return offsets
}

function optional(value: string | undefined): string | undefined {
  if (value === undefined) return undefined
  const trimmed = value.trim()
  return trimmed ? trimmed : undefined
}

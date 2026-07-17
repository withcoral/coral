import { create } from '@bufbuild/protobuf'
import type { Client } from '@connectrpc/connect'

import {
  CatalogItemKind,
  ListCatalogRequestSchema,
  ListColumnsRequestSchema,
  type CatalogService,
  type ListColumnsResponse,
  type TableSummary,
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
  name: string
  requiredFilters: string[]
}

export interface SchemaGroup {
  name: string
  tables: TableDef[]
}

export interface SchemaResponse {
  connectors: SchemaGroup[]
}

const COLUMN_PAGE_LIMIT = 200
const COLUMN_PAGE_CONCURRENCY = 6

export async function fetchSchemaFromCoral(
  catalogClient: CatalogClient,
  workspace: Workspace,
  signal?: AbortSignal,
): Promise<SchemaResponse> {
  const tableSummaries = await listTables(catalogClient, workspace, signal)
  if (tableSummaries.length === 0) return { connectors: [] }

  const schemaMap = new Map<string, TableDef[]>()
  for (const summary of tableSummaries) {
    if (!summary.schemaName || !summary.name) continue

    const tables = schemaMap.get(summary.schemaName) ?? []
    tables.push({
      columns: [],
      columnsLoaded: false,
      description: optional(summary.description),
      name: summary.name,
      requiredFilters: summary.requiredFilters,
    })
    schemaMap.set(summary.schemaName, tables)
  }

  return {
    connectors: [...schemaMap.entries()]
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([name, tables]) => ({
        name,
        tables: tables.toSorted((left, right) => left.name.localeCompare(right.name)),
      })),
  }
}

async function listTables(
  catalogClient: CatalogClient,
  workspace: Workspace,
  signal?: AbortSignal,
): Promise<TableSummary[]> {
  const response = await catalogClient.listCatalog(
    create(ListCatalogRequestSchema, {
      kind: CatalogItemKind.TABLE,
      workspace,
    }),
    { signal },
  )

  return response.items.flatMap((item) => (item.item.case === 'table' ? [item.item.value] : []))
}

export async function fetchTableColumnsFromCoral(
  catalogClient: CatalogClient,
  workspace: Workspace,
  schemaName: string,
  tableName: string,
  signal?: AbortSignal,
): Promise<ColumnDef[]> {
  const firstPage = await listColumnsPage(
    catalogClient,
    workspace,
    schemaName,
    tableName,
    0,
    signal,
  )
  const columns = columnsFromResponse(firstPage)
  const pagination = firstPage.pagination
  if (!pagination?.hasMore) return columns

  const nextOffset = pagination.nextOffset || COLUMN_PAGE_LIMIT
  if (pagination.totalCount > nextOffset) {
    const remainingPages = await mapWithConcurrency(
      pageOffsets(nextOffset, pagination.totalCount, COLUMN_PAGE_LIMIT),
      COLUMN_PAGE_CONCURRENCY,
      (offset) => listColumnsPage(catalogClient, workspace, schemaName, tableName, offset, signal),
    )
    return columns.concat(remainingPages.flatMap(columnsFromResponse))
  }

  let offset = pagination.nextOffset
  while (offset > 0) {
    const page = await listColumnsPage(
      catalogClient,
      workspace,
      schemaName,
      tableName,
      offset,
      signal,
    )
    columns.push(...columnsFromResponse(page))
    if (!page.pagination?.hasMore) break
    offset = page.pagination.nextOffset
  }
  return columns
}

async function listColumnsPage(
  catalogClient: CatalogClient,
  workspace: Workspace,
  schemaName: string,
  tableName: string,
  offset: number,
  signal?: AbortSignal,
): Promise<ListColumnsResponse> {
  return catalogClient.listColumns(
    create(ListColumnsRequestSchema, {
      pagination: {
        limit: COLUMN_PAGE_LIMIT,
        offset,
      },
      schemaName,
      tableName,
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

function optional(value: string): string | undefined {
  const trimmed = value.trim()
  return trimmed ? trimmed : undefined
}

import { createClient, type Client } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

import { ensureCoralRuntime } from './coral-runtime'

let catalogClientPromise: Promise<Client<typeof CatalogService>> | null = null
let queryClientPromise: Promise<Client<typeof QueryService>> | null = null
let sourceClientPromise: Promise<Client<typeof SourceService>> | null = null

export function getCatalogClient(): Promise<Client<typeof CatalogService>> {
  catalogClientPromise ??= ensureCoralRuntime().then((runtime) =>
    createClient(CatalogService, createGrpcWebTransport({ baseUrl: runtime.url })),
  )
  return catalogClientPromise
}

export function getQueryClient(): Promise<Client<typeof QueryService>> {
  queryClientPromise ??= ensureCoralRuntime().then((runtime) =>
    createClient(QueryService, createGrpcWebTransport({ baseUrl: runtime.url })),
  )
  return queryClientPromise
}

export function getSourceClient(): Promise<Client<typeof SourceService>> {
  sourceClientPromise ??= ensureCoralRuntime().then((runtime) =>
    createClient(SourceService, createGrpcWebTransport({ baseUrl: runtime.url })),
  )
  return sourceClientPromise
}

export const catalogClient: Client<typeof CatalogService> = {
  async describeTable(request, options) {
    return (await getCatalogClient()).describeTable(request, options)
  },
  async listCatalog(request, options) {
    return (await getCatalogClient()).listCatalog(request, options)
  },
  async listColumns(request, options) {
    return (await getCatalogClient()).listColumns(request, options)
  },
  async searchCatalog(request, options) {
    return (await getCatalogClient()).searchCatalog(request, options)
  },
}

export const WORKSPACE = { name: 'default' } as const

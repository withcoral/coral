import { create } from '@bufbuild/protobuf'
import { createClient } from '@connectrpc/connect'
import { createGrpcTransport } from '@connectrpc/connect-node'

import { CatalogService, ListCatalogRequestSchema } from '@/generated/coral/v1/catalog_pb'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { resolveCoralEndpoint } from './coral-endpoint.server'

export async function assertCoralReady(request: Request): Promise<void> {
  const endpoint = resolveCoralEndpoint({ authenticated: true, request })
  const catalog = createClient(CatalogService, createGrpcTransport({ baseUrl: endpoint.baseUrl }))

  await catalog.listCatalog(
    create(ListCatalogRequestSchema, {
      workspace: create(WorkspaceSchema, { name: 'default' }),
    }),
    { signal: request.signal },
  )
}

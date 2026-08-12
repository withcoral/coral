import { create } from '@bufbuild/protobuf'
import { createClient } from '@connectrpc/connect'
import { createGrpcTransport } from '@connectrpc/connect-node'

import { CatalogService, ListCatalogRequestSchema } from '@/generated/coral/v1/catalog_pb'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import { resolveCoralEndpoint } from '@/lib/coral-endpoint.server'

import type { Route } from './+types/readyz'

/** Readiness proves Reef can complete a real native gRPC request to Coral. */
export async function loader({ request }: Route.LoaderArgs): Promise<Response> {
  const endpoint = resolveCoralEndpoint({ authenticated: true, request })
  const catalog = createClient(CatalogService, createGrpcTransport({ baseUrl: endpoint.baseUrl }))

  await catalog.listCatalog(
    create(ListCatalogRequestSchema, {
      workspace: create(WorkspaceSchema, { name: 'default' }),
    }),
    { signal: request.signal },
  )

  return Response.json({ coral: 'reachable', status: 'ok' })
}

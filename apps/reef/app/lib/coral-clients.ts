import { createClient, type Client } from '@connectrpc/connect'

import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

import { getCoralTransport } from './coral-runtime'

export function getSourceClient(): Promise<Client<typeof SourceService>> {
  return getCoralTransport().then((transport) => createClient(SourceService, transport))
}

export function getCatalogClient(): Promise<Client<typeof CatalogService>> {
  return getCoralTransport().then((transport) => createClient(CatalogService, transport))
}

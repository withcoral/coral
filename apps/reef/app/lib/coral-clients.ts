import { createClient, type Client } from '@connectrpc/connect'

import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

import { getCoralTransport } from './coral-runtime'

export function getSourceClient(): Promise<Client<typeof SourceService>> {
  return getCoralTransport().then((transport) => createClient(SourceService, transport))
}

export function getQueryClient(): Promise<Client<typeof QueryService>> {
  return getCoralTransport().then((transport) => createClient(QueryService, transport))
}

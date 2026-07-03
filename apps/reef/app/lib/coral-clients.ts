import { createClient, type Client } from '@connectrpc/connect'

import { SourceService } from '@/generated/coral/v1/sources_pb'

import { getCoralTransport } from './coral-runtime'

export function getSourceClient(): Promise<Client<typeof SourceService>> {
  return getCoralTransport().then((transport) => createClient(SourceService, transport))
}

export const WORKSPACE = { name: 'default' } as const

import { createClient, type Client } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

import { ensureCoralRuntime } from './coral-runtime'

let queryClientPromise: Promise<Client<typeof QueryService>> | null = null
let sourceClientPromise: Promise<Client<typeof SourceService>> | null = null

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

export const WORKSPACE = { name: 'default' } as const

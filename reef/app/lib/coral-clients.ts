import { createClient, type Client } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

import { ensureCoralRuntime } from './coral-runtime'

let queryClientPromise: Promise<Client<typeof QueryService>> | null = null
let sourceClientPromise: Promise<Client<typeof SourceService>> | null = null

export function getQueryClient(): Promise<Client<typeof QueryService>> {
  if (queryClientPromise) return queryClientPromise
  const promise = ensureCoralRuntime().then((runtime) =>
    createClient(QueryService, createGrpcWebTransport({ baseUrl: runtime.url })),
  )
  promise.catch(() => {
    if (queryClientPromise === promise) queryClientPromise = null
  })
  queryClientPromise = promise
  return promise
}

export function getSourceClient(): Promise<Client<typeof SourceService>> {
  if (sourceClientPromise) return sourceClientPromise
  const promise = ensureCoralRuntime().then((runtime) =>
    createClient(SourceService, createGrpcWebTransport({ baseUrl: runtime.url })),
  )
  promise.catch(() => {
    if (sourceClientPromise === promise) sourceClientPromise = null
  })
  sourceClientPromise = promise
  return promise
}

export const WORKSPACE = { name: 'default' } as const

import { createClient } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

function grpcWebBaseUrl(): string {
  return import.meta.env.VITE_CORAL_GRPC_WEB_URL ?? window.location.origin
}

const transport = createGrpcWebTransport({ baseUrl: grpcWebBaseUrl() })

export const queryClient = createClient(QueryService, transport)
export const sourceClient = createClient(SourceService, transport)

export const WORKSPACE = { name: 'default' } as const

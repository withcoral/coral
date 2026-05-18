import { create } from '@bufbuild/protobuf'
import { createClient } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import {
  GetTraceRequestSchema,
  ListTracesRequestSchema,
  TraceService,
  type GetTraceResponse,
  type ListTracesResponse,
} from '@/generated/coral/v1/traces_pb'

function grpcWebBaseUrl(): string {
  return import.meta.env.VITE_CORAL_GRPC_WEB_URL ?? window.location.origin
}

const transport = createGrpcWebTransport({
  baseUrl: grpcWebBaseUrl(),
})

const traces = createClient(TraceService, transport)

export async function listTraces(pageSize = 50, pageToken = ''): Promise<ListTracesResponse> {
  return traces.listTraces(create(ListTracesRequestSchema, { pageSize, pageToken }))
}

export async function getTrace(traceId: string): Promise<GetTraceResponse> {
  return traces.getTrace(create(GetTraceRequestSchema, { traceId }))
}

import { create } from '@bufbuild/protobuf'
import { createClient, type Client } from '@connectrpc/connect'

import {
  GetTraceRequestSchema,
  ListTracesRequestSchema,
  TraceService,
  type GetTraceResponse,
  type ListTracesResponse,
} from '@/generated/coral/v1/traces_pb'

import { getCoralTransport } from './coral-runtime'

const listTraceRequests = new Map<string, Promise<ListTracesResponse>>()
const getTraceRequests = new Map<string, Promise<GetTraceResponse>>()

function getTracesClient(): Promise<Client<typeof TraceService>> {
  return getCoralTransport().then((transport) => createClient(TraceService, transport))
}

export async function listTraces(pageSize = 50, pageToken = ''): Promise<ListTracesResponse> {
  const key = `${pageSize}:${pageToken}`
  const inFlight = listTraceRequests.get(key)
  if (inFlight) return inFlight

  const request = getTracesClient()
    .then((traces) => traces.listTraces(create(ListTracesRequestSchema, { pageSize, pageToken })))
    .finally(() => listTraceRequests.delete(key))
  listTraceRequests.set(key, request)
  return request
}

export async function getTrace(traceId: string): Promise<GetTraceResponse> {
  const inFlight = getTraceRequests.get(traceId)
  if (inFlight) return inFlight

  const request = getTracesClient()
    .then((traces) => traces.getTrace(create(GetTraceRequestSchema, { traceId })))
    .finally(() => getTraceRequests.delete(traceId))
  getTraceRequests.set(traceId, request)
  return request
}

import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/traces'

import { ListTracesRequestSchema } from '@/generated/coral/v1/traces_pb'
import { traceClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { formatTraceError, isQueryTrace, type TraceSummaryData } from '@/views/traces/trace-utils'

const MAX_QUERY_TRACES = 80
const TRACE_LIST_PAGE_SIZE = 100
const MAX_TRACE_LIST_PAGES = 2

export interface TracesRouteData {
  endpointLabel: string
  loadError: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
}

export type ListTracePage = (
  request: Request,
  pageSize: number,
  pageToken: string,
) => Promise<{ nextPageToken: string; traces: TraceSummaryData[] }>

export async function loader({ request }: Route.LoaderArgs): Promise<TracesRouteData> {
  return loadTracesRouteData(request)
}

export async function loadTracesRouteData(
  request: Request,
  listPage: ListTracePage = listTracePageForRequest,
): Promise<TracesRouteData> {
  const referenceTimeMs = Date.now()
  try {
    return {
      endpointLabel: traceEndpointLabel(request),
      loadError: null,
      referenceTimeMs,
      traces: await listQueryTraces(request, listPage),
    }
  } catch (error) {
    return {
      endpointLabel: traceEndpointLabel(request),
      loadError: formatTraceError(errorMessage(error)),
      referenceTimeMs,
      traces: [],
    }
  }
}

export async function listQueryTraces(
  request: Request,
  listPage: ListTracePage = listTracePageForRequest,
): Promise<TraceSummaryData[]> {
  const queryTraces: TraceSummaryData[] = []
  let pageToken = ''

  for (
    let page = 0;
    page < MAX_TRACE_LIST_PAGES && queryTraces.length < MAX_QUERY_TRACES;
    page += 1
  ) {
    const response = await listPage(request, TRACE_LIST_PAGE_SIZE, pageToken)
    queryTraces.push(...response.traces.filter(isQueryTrace))
    pageToken = response.nextPageToken
    if (!pageToken) break
  }

  return queryTraces.slice(0, MAX_QUERY_TRACES)
}

export function traceEndpointLabel(request: Request): string {
  try {
    return new URL(request.url).host || 'TraceService'
  } catch {
    return 'TraceService'
  }
}

async function listTracePageForRequest(
  request: Request,
  pageSize: number,
  pageToken: string,
): Promise<{ nextPageToken: string; traces: TraceSummaryData[] }> {
  return traceClientForRequest(request).listTraces(
    create(ListTracesRequestSchema, { pageSize, pageToken }),
  )
}

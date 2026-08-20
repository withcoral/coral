import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/traces'

import { requestAuthContext } from '@/auth/server-context'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { ListTracesRequestSchema, TraceView } from '@/generated/coral/v1/traces_pb'
import { traceClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { formatTraceError, type TraceSummaryData } from '@/views/traces/trace-utils'

const MAX_QUERY_TRACES = 80
const TRACE_LIST_PAGE_SIZE = 100

export interface TracesRouteData {
  endpointLabel: string
  loadError: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
}

export type ListTracePage = (
  request: Request,
  workspace: Workspace,
  pageSize: number,
  pageToken: string,
  view: TraceView,
  accessToken: string | null,
) => Promise<{ nextPageToken: string; traces: TraceSummaryData[] }>

export async function loader({
  context,
  params,
  request,
}: Route.LoaderArgs): Promise<TracesRouteData> {
  return loadTracesRouteData(
    request,
    workspaceFromParams(params),
    context.get(requestAuthContext).accessToken,
  )
}

export async function loadTracesRouteData(
  request: Request,
  workspace: Workspace,
  accessToken: string | null,
  listPage: ListTracePage = listTracePageForRequest,
): Promise<TracesRouteData> {
  const referenceTimeMs = Date.now()
  try {
    return {
      endpointLabel: traceEndpointLabel(request),
      loadError: null,
      referenceTimeMs,
      traces: await listQueryTraces(request, workspace, accessToken, listPage),
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
  workspace: Workspace,
  accessToken: string | null,
  listPage: ListTracePage = listTracePageForRequest,
): Promise<TraceSummaryData[]> {
  const response = await listPage(
    request,
    workspace,
    TRACE_LIST_PAGE_SIZE,
    '',
    TraceView.QUERY_STREAM,
    accessToken,
  )
  return response.traces.slice(0, MAX_QUERY_TRACES)
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
  workspace: Workspace,
  pageSize: number,
  pageToken: string,
  view: TraceView,
  accessToken: string | null,
): Promise<{ nextPageToken: string; traces: TraceSummaryData[] }> {
  return traceClientForRequest(request, accessToken).listTraces(
    create(ListTracesRequestSchema, { pageSize, pageToken, view, workspace }),
  )
}

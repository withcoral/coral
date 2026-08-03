import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/traces'

import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { ListTracesRequestSchema, TraceView } from '@/generated/coral/v1/traces_pb'
import { traceClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import {
  formatTraceError,
  hasTypedOperation,
  isLegacyQueryTrace,
  type TraceSummaryData,
} from '@/views/traces/trace-utils'

const MAX_QUERY_TRACES = 80
const TRACE_LIST_PAGE_SIZE = 100
const MAX_LEGACY_TRACE_LIST_PAGES = 2

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
) => Promise<{ nextPageToken: string; traces: TraceSummaryData[] }>

export async function loader({ params, request }: Route.LoaderArgs): Promise<TracesRouteData> {
  return loadTracesRouteData(request, workspaceFromParams(params))
}

export async function loadTracesRouteData(
  request: Request,
  workspace: Workspace,
  listPage: ListTracePage = listTracePageForRequest,
): Promise<TracesRouteData> {
  const referenceTimeMs = Date.now()
  try {
    return {
      endpointLabel: traceEndpointLabel(request),
      loadError: null,
      referenceTimeMs,
      traces: await listQueryTraces(request, workspace, listPage),
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
  listPage: ListTracePage = listTracePageForRequest,
): Promise<TraceSummaryData[]> {
  const firstPage = await listPage(
    request,
    workspace,
    TRACE_LIST_PAGE_SIZE,
    '',
    TraceView.QUERY_STREAM,
  )
  const typedOperations = firstPage.traces.filter(hasTypedOperation)

  // New servers apply Query Stream projection and pagination before returning data.
  // A non-empty response without operation metadata came from an older server that
  // ignored the unknown `view` field, so retain the bounded client-side fallback.
  if (firstPage.traces.length === 0 || typedOperations.length > 0) {
    return typedOperations.slice(0, MAX_QUERY_TRACES)
  }

  const queryTraces = firstPage.traces.filter(isLegacyQueryTrace)
  let pageToken = firstPage.nextPageToken

  for (
    let page = 1;
    page < MAX_LEGACY_TRACE_LIST_PAGES && pageToken && queryTraces.length < MAX_QUERY_TRACES;
    page += 1
  ) {
    const response = await listPage(
      request,
      workspace,
      TRACE_LIST_PAGE_SIZE,
      pageToken,
      TraceView.QUERY_STREAM,
    )
    queryTraces.push(...response.traces.filter(isLegacyQueryTrace))
    pageToken = response.nextPageToken
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
  workspace: Workspace,
  pageSize: number,
  pageToken: string,
  view: TraceView,
): Promise<{ nextPageToken: string; traces: TraceSummaryData[] }> {
  return traceClientForRequest(request).listTraces(
    create(ListTracesRequestSchema, { pageSize, pageToken, view, workspace }),
  )
}

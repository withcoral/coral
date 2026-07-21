import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/trace-detail'

import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { GetTraceRequestSchema } from '@/generated/coral/v1/traces_pb'
import { traceClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { rootSpanIdFromSearch } from '@/views/traces/trace-location'
import { formatTraceError, type TraceDetailData } from '@/views/traces/trace-utils'

export interface TraceDetailRouteData {
  detail: TraceDetailData | null
  loadError: string | null
}

export type GetTraceForRequest = (
  request: Request,
  traceId: string,
  rootSpanId: string,
  workspace: Workspace,
) => Promise<TraceDetailData>

export async function loader({ params, request }: Route.LoaderArgs): Promise<TraceDetailRouteData> {
  return loadTraceDetailRouteData(request, params.traceId, workspaceFromParams(params))
}

export async function loadTraceDetailRouteData(
  request: Request,
  traceId: string | undefined,
  workspace: Workspace,
  getTrace: GetTraceForRequest = getTraceForRequest,
): Promise<TraceDetailRouteData> {
  if (!traceId) return { detail: null, loadError: 'Missing trace ID' }

  try {
    return {
      detail: await getTrace(
        request,
        traceId,
        rootSpanIdFromSearch(new URL(request.url).search),
        workspace,
      ),
      loadError: null,
    }
  } catch (error) {
    return { detail: null, loadError: formatTraceError(errorMessage(error)) }
  }
}

async function getTraceForRequest(
  request: Request,
  traceId: string,
  rootSpanId: string,
  workspace: Workspace,
): Promise<TraceDetailData> {
  return traceClientForRequest(request).getTrace(
    create(GetTraceRequestSchema, { rootSpanId, traceId, workspace }),
  )
}

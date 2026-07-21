import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/trace-detail'

import { requestAuthContext } from '@/auth/server-context'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { GetTraceRequestSchema } from '@/generated/coral/v1/traces_pb'
import { traceClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { formatTraceError, type TraceDetailData } from '@/views/traces/trace-utils'

export interface TraceDetailRouteData {
  detail: TraceDetailData | null
  loadError: string | null
}

export type GetTraceForRequest = (
  request: Request,
  traceId: string,
  workspace: Workspace,
  accessToken: string | null,
) => Promise<TraceDetailData>

export async function loader({
  context,
  params,
  request,
}: Route.LoaderArgs): Promise<TraceDetailRouteData> {
  return loadTraceDetailRouteData(
    request,
    params.traceId,
    workspaceFromParams(params),
    context.get(requestAuthContext).accessToken,
  )
}

export async function loadTraceDetailRouteData(
  request: Request,
  traceId: string | undefined,
  workspace: Workspace,
  accessToken: string | null,
  getTrace: GetTraceForRequest = getTraceForRequest,
): Promise<TraceDetailRouteData> {
  if (!traceId) return { detail: null, loadError: 'Missing trace ID' }

  try {
    return { detail: await getTrace(request, traceId, workspace, accessToken), loadError: null }
  } catch (error) {
    return { detail: null, loadError: formatTraceError(errorMessage(error)) }
  }
}

async function getTraceForRequest(
  request: Request,
  traceId: string,
  workspace: Workspace,
  accessToken: string | null,
): Promise<TraceDetailData> {
  return traceClientForRequest(request, accessToken).getTrace(
    create(GetTraceRequestSchema, { traceId, workspace }),
  )
}

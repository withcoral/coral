import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-detail'
import { action as sourcesAction, type SourcesActionData } from './sources-action'

import { requestAuthContext } from '@/auth/server-context'
import {
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
  type Source,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { SourceDetailView } from '@/views/sources/source-detail'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import {
  originLabel,
  toCatalogSource,
  toCatalogSourceInputSpecs,
  type CatalogEntry,
} from '@/lib/sources'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { routePath } from '@/routing/routemap'

interface SourceDetailRouteData {
  entry: CatalogEntry
  loadError: string | null
}

export async function loader({
  context,
  params,
  request,
}: Route.LoaderArgs): Promise<SourceDetailRouteData> {
  const name = params.sourceName
  const workspace = workspaceFromParams(params)
  if (!name) {
    return {
      entry: sourceDetailEntry('', null, null),
      loadError: 'Missing source name',
    }
  }

  const sourceClient = sourceClientForRequest(request, context.get(requestAuthContext).accessToken)
  const [sourceResult, infoResult] = await Promise.allSettled([
    getInstalledSource(sourceClient, workspace, name),
    getSourceInfo(sourceClient, workspace, name),
  ])
  const info = infoResult.status === 'fulfilled' ? infoResult.value : null

  const source = sourceResult.status === 'fulfilled' ? sourceResult.value : null
  const loadError =
    !source && (!info || info.installed) && sourceResult.status === 'rejected'
      ? errorMessage(sourceResult.reason)
      : null

  return {
    entry: sourceDetailEntry(name, source, info),
    loadError,
  }
}

export async function action(args: Route.ActionArgs): Promise<SourcesActionData | Response> {
  return sourcesAction(args)
}

export default function SourceDetailRoute({
  actionData,
  loaderData,
  params,
}: Route.ComponentProps) {
  return (
    <SourceDetailView
      actionData={actionData}
      loaderData={loaderData}
      sourcesPath={routePath('workspaceSources', { workspaceId: params.workspaceId })}
      workspaceId={params.workspaceId}
    />
  )
}

async function getSourceInfo(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  name: string,
): Promise<SourceInfo> {
  const response = await sourceClient.getSourceInfo(
    create(GetSourceInfoRequestSchema, { name, workspace }),
  )
  if (!response.sourceInfo) throw new Error(`Source info for ${name} was not found`)
  return response.sourceInfo
}

async function getInstalledSource(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  workspace: Workspace,
  name: string,
): Promise<Source> {
  const response = await sourceClient.getSource(create(GetSourceRequestSchema, { name, workspace }))
  if (!response.source) throw new Error(`Source ${name} was not found`)
  return response.source
}

function sourceDetailEntry(name: string, source: Source | null, info: SourceInfo | null) {
  const origin = source ? originLabel(source.origin) : info ? originLabel(info.origin) : 'unknown'
  return {
    description:
      info?.description ?? (origin === 'imported' ? 'Imported source' : 'Configured source'),
    inputSpecs: info ? toCatalogSourceInputSpecs(info) : undefined,
    installed: source ? true : (info?.installed ?? false),
    name: source?.name || info?.name || name,
    origin,
    source: source ? toCatalogSource(source) : undefined,
    version: source?.version || info?.version || '',
  } satisfies CatalogEntry
}

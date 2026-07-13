import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/source-detail'
import { action as sourcesAction, type SourcesActionData } from './sources-action'

import {
  GetSourceInfoRequestSchema,
  GetSourceRequestSchema,
  type Source,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'
import { SourceDetailView } from '@/views/sources/source-detail'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import { WORKSPACE } from '@/lib/constants'
import {
  originLabel,
  toCatalogSource,
  toCatalogSourceInputSpecs,
  type CatalogEntry,
} from '@/lib/sources'
import { errorMessage } from '@/lib/utils'

interface SourceDetailRouteData {
  entry: CatalogEntry
  loadError: string | null
}

export async function loader({
  params,
  request,
}: Route.LoaderArgs): Promise<SourceDetailRouteData> {
  const name = params.sourceName
  if (!name) {
    return {
      entry: sourceDetailEntry('', null, null),
      loadError: 'Missing source name',
    }
  }

  const sourceClient = sourceClientForRequest(request)
  const [sourceResult, infoResult] = await Promise.allSettled([
    getInstalledSource(sourceClient, name),
    getSourceInfo(sourceClient, name),
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

export default function SourceDetailRoute({ actionData, loaderData }: Route.ComponentProps) {
  return <SourceDetailView actionData={actionData} loaderData={loaderData} />
}

async function getSourceInfo(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  name: string,
): Promise<SourceInfo> {
  const response = await sourceClient.getSourceInfo(
    create(GetSourceInfoRequestSchema, { name, workspace: WORKSPACE }),
  )
  if (!response.sourceInfo) throw new Error(`Source info for ${name} was not found`)
  return response.sourceInfo
}

async function getInstalledSource(
  sourceClient: ReturnType<typeof sourceClientForRequest>,
  name: string,
): Promise<Source> {
  const response = await sourceClient.getSource(
    create(GetSourceRequestSchema, { name, workspace: WORKSPACE }),
  )
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

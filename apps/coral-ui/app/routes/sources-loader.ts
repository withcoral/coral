import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/sources'

import { requestAuthContext } from '@/auth/server-context'
import {
  DiscoverSourcesRequestSchema,
  ListSourcesRequestSchema,
} from '@/generated/coral/v1/sources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import { catalogEntries, type CatalogEntry } from '@/lib/sources'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'

export interface SourcesRouteData {
  entries: CatalogEntry[]
  loadError: string | null
}

export async function loader({
  context,
  params,
  request,
}: Route.LoaderArgs): Promise<SourcesRouteData> {
  return loadSourcesRouteData(
    request,
    workspaceFromParams(params),
    context.get(requestAuthContext).accessToken,
  )
}

export async function loadSourcesRouteData(
  request: Request,
  workspace: Workspace,
  accessToken: string | null,
): Promise<SourcesRouteData> {
  try {
    return {
      entries: await listCatalogForRequest(request, workspace, accessToken),
      loadError: null,
    }
  } catch (error) {
    return { entries: [], loadError: errorMessage(error) }
  }
}

export async function listCatalogForRequest(
  request: Request,
  workspace: Workspace,
  accessToken: string | null,
): Promise<CatalogEntry[]> {
  const sourceClient = sourceClientForRequest(request, accessToken)
  const [discovered, installed] = await Promise.all([
    sourceClient.discoverSources(create(DiscoverSourcesRequestSchema, { workspace })),
    sourceClient.listSources(create(ListSourcesRequestSchema, { workspace })),
  ])
  return catalogEntries(discovered.sources, installed.sources)
}

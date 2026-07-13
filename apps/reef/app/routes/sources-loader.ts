import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/sources'

import {
  DiscoverSourcesRequestSchema,
  ListSourcesRequestSchema,
} from '@/generated/coral/v1/sources_pb'
import { WORKSPACE } from '@/lib/constants'
import { sourceClientForRequest } from '@/lib/coral-request.server'
import { catalogEntries, type CatalogEntry } from '@/lib/sources'
import { errorMessage } from '@/lib/utils'

export interface SourcesRouteData {
  entries: CatalogEntry[]
  loadError: string | null
}

export async function loader({ request }: Route.LoaderArgs): Promise<SourcesRouteData> {
  return loadSourcesRouteData(request)
}

export async function loadSourcesRouteData(request: Request): Promise<SourcesRouteData> {
  try {
    return { entries: await listCatalogForRequest(request), loadError: null }
  } catch (error) {
    return { entries: [], loadError: errorMessage(error) }
  }
}

export async function listCatalogForRequest(request: Request): Promise<CatalogEntry[]> {
  const sourceClient = sourceClientForRequest(request)
  const [discovered, installed] = await Promise.all([
    sourceClient.discoverSources(create(DiscoverSourcesRequestSchema, { workspace: WORKSPACE })),
    sourceClient.listSources(create(ListSourcesRequestSchema, { workspace: WORKSPACE })),
  ])
  return catalogEntries(discovered.sources, installed.sources)
}

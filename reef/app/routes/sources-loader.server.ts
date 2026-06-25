import type { Route } from './+types/sources'

import type { Source, SourceInfo } from '@/generated/coral/v1/sources_pb'
import type { CatalogEntry } from '@/lib/source-data'
import { type SourceServiceResource, sourceServiceForRequest } from '@/lib/source-service.server'

export interface SourcesLoaderData {
  entries: CatalogEntry[]
  loadError: string | null
  selected: SelectedSource | null
}

export type SelectedSource =
  | {
      error: string | null
      info: SourceInfo | null
      kind: 'install'
      name: string
    }
  | {
      error: string | null
      info: SourceInfo | null
      kind: 'detail'
      name: string
      source: Source | null
    }

export async function loader({ request }: Route.LoaderArgs): Promise<SourcesLoaderData> {
  const url = new URL(request.url)
  const installName = url.searchParams.get('install')
  const sourceName = url.searchParams.get('source')
  const sources = sourceServiceForRequest(request)

  let entries: CatalogEntry[] = []
  let loadError: string | null = null
  try {
    entries = await sources.listCatalog()
  } catch (error) {
    loadError = errorMessage(error)
  }

  const selected = installName
    ? await loadInstallSelection(sources, installName)
    : sourceName
      ? await loadDetailSelection(sources, sourceName)
      : null

  return { entries, loadError, selected }
}

async function loadInstallSelection(
  sources: SourceServiceResource,
  name: string,
): Promise<SelectedSource> {
  try {
    const resolved = await sources.getSourceInfo(name)
    return { error: null, info: resolved.info, kind: 'install', name }
  } catch (error) {
    return { error: errorMessage(error), info: null, kind: 'install', name }
  }
}

async function loadDetailSelection(
  sources: SourceServiceResource,
  name: string,
): Promise<SelectedSource> {
  const [sourceResult, infoResult] = await Promise.allSettled([
    sources.getInstalledSource(name),
    sources.getSourceInfo(name),
  ])
  if (sourceResult.status === 'rejected') {
    return {
      error: errorMessage(sourceResult.reason),
      info: null,
      kind: 'detail',
      name,
      source: null,
    }
  }
  return {
    error: null,
    info: infoResult.status === 'fulfilled' ? infoResult.value.info : null,
    kind: 'detail',
    name,
    source: sourceResult.value,
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

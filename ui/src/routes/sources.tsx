import { useRef } from 'react'
import { useLoaderData, useRevalidator } from 'react-router'

import { discoverBundled, type CatalogEntry } from '@/lib/sources'
import { SourcesIndex } from '@/views/sources/sources-index'

export interface SourcesRouteData {
  catalog: CatalogEntry[]
  error: string | null
}

export async function sourcesLoader(): Promise<SourcesRouteData> {
  try {
    return { catalog: await discoverBundled(), error: null }
  } catch (err) {
    return {
      catalog: [],
      error: err instanceof Error ? err.message : String(err),
    }
  }
}

export function SourcesRoute() {
  const data = useLoaderData() as SourcesRouteData
  const revalidator = useRevalidator()
  const lastGoodCatalog = useRef(data.catalog)
  if (!data.error) lastGoodCatalog.current = data.catalog
  const catalog = data.error ? lastGoodCatalog.current : data.catalog
  const refresh = () => void revalidator.revalidate()

  return (
    <SourcesIndex
      catalog={catalog}
      error={data.error}
      loading={revalidator.state !== 'idle' && catalog.length === 0 && !data.error}
      onCatalogChanged={refresh}
      onRetry={refresh}
    />
  )
}

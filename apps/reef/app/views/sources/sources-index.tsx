import { useEffect, useRef, useState } from 'react'
import { useRevalidator } from 'react-router'

import { SourceCatalogSurface } from '@/components/sources'
import type { CatalogEntry } from '@/lib/sources'
import { routePath } from '@/routing/routemap'

export function SourcesIndex({
  entries,
  loadError = null,
  workspaceId,
}: {
  entries: CatalogEntry[]
  loadError?: string | null
  workspaceId: string
}) {
  const [search, setSearch] = useState('')
  const searchInputRef = useRef<HTMLInputElement>(null)
  const revalidator = useRevalidator()

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'f') {
        const input = searchInputRef.current
        if (!input) return
        event.preventDefault()
        input.focus()
        input.select()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  const loading = revalidator.state === 'loading' && entries.length === 0 && !loadError

  return (
    <SourceCatalogSurface
      entries={entries}
      errorMessage={loadError}
      getEntryTo={(entry) => routePath('workspaceSource', { sourceName: entry.name, workspaceId })}
      loadState={loadError ? 'error' : loading ? 'loading' : 'idle'}
      onRetry={() => revalidator.revalidate()}
      onSearchChange={setSearch}
      search={search}
      searchInputRef={searchInputRef}
    />
  )
}

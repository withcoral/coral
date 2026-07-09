import { useEffect, useRef, useState } from 'react'
import { useRevalidator } from 'react-router'

import { SourceCatalogSurface } from '@/components/sources'
import type { CatalogEntry } from '@/lib/sources'

export function SourcesIndex({
  entries,
  loadError = null,
}: {
  entries: CatalogEntry[]
  loadError?: string | null
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
      getEntryTo={(entry) => `/sources/${encodeURIComponent(entry.name)}`}
      loadState={loadError ? 'error' : loading ? 'loading' : 'idle'}
      onRetry={() => revalidator.revalidate()}
      onSearchChange={setSearch}
      search={search}
      searchInputRef={searchInputRef}
    />
  )
}

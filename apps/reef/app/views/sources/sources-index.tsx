import { useCallback, useRef, useState } from 'react'
import { useRevalidator } from 'react-router'

import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'

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

  const onSearchShortcut = useCallback((event: KeyboardEvent) => {
    const input = searchInputRef.current
    if (!input) return

    event.preventDefault()
    input.focus()
    input.select()
  }, [])

  const loading = revalidator.state === 'loading' && entries.length === 0 && !loadError

  return (
    <>
      <KeyboardShortcut handler={onSearchShortcut} shortcut="$mod+f" />
      <SourceCatalogSurface
        entries={entries}
        errorMessage={loadError}
        getEntryTo={(entry) =>
          routePath('workspaceSource', { sourceName: entry.name, workspaceId })
        }
        loadState={loadError ? 'error' : loading ? 'loading' : 'idle'}
        onRetry={() => revalidator.revalidate()}
        onSearchChange={setSearch}
        search={search}
        searchInputRef={searchInputRef}
      />
    </>
  )
}

import { useCallback, useRef, useState } from 'react'
import { Link, useRevalidator } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
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
        headerAction={
          <ButtonContainer
            as={Link}
            size="36"
            to={routePath('workspaceSourceInstall', { workspaceId })}
            variant="primary"
          >
            <ButtonIcon name="Plus" />
            <ButtonText>Create source</ButtonText>
          </ButtonContainer>
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

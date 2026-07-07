import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { Button, ScrollArea } from '@/wax/components'
import { CardList, type CardItem } from '@/wax/components/card'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import { EmptyPage } from '@/components/empty-page'
import { ErrorBanner } from '@/components/error-banner'
import { SOURCE_CATEGORY_ORDER, getCategoryForSource } from '@/lib/source-categories'
import { discoverBundled, type CatalogEntry } from '@/lib/sources'

import { ProviderLogo } from './provider-logo'
import { SourceDetailDialog } from './source-detail'
import { SourceInstallDialog } from './source-install'
import * as styles from './sources-index.css'

type IndexEntry = CatalogEntry

export function SourcesIndex() {
  const [bundled, setBundled] = useState<CatalogEntry[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [installingName, setInstallingName] = useState<string | null>(null)
  const [detailName, setDetailName] = useState<string | null>(null)
  const searchInputRef = useRef<HTMLInputElement>(null)

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

  const refresh = useCallback(async () => {
    try {
      setBundled(await discoverBundled())
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  useEffect(() => {
    void refresh()
  }, [refresh])

  const loading = bundled === null && !error

  const allEntries = useMemo<IndexEntry[]>(
    () => (bundled ?? []).toSorted((a, b) => a.name.localeCompare(b.name)),
    [bundled],
  )

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return allEntries
    return allEntries.filter(
      (entry) =>
        entry.name.toLowerCase().includes(q) || entry.description.toLowerCase().includes(q),
    )
  }, [allEntries, search])

  const connected = useMemo(() => filtered.filter((entry) => entry.installed), [filtered])

  const sections = useMemo(() => {
    const grouped = new Map<string, IndexEntry[]>()
    for (const entry of filtered) {
      if (entry.installed) continue
      const category = getCategoryForSource(entry.name)
      const group = grouped.get(category)
      if (group) {
        group.push(entry)
      } else {
        grouped.set(category, [entry])
      }
    }

    const ordered = SOURCE_CATEGORY_ORDER.map((category) => ({
      ...category,
      entries: grouped.get(category.key) ?? [],
    })).filter((category) => category.entries.length > 0)
    const other = grouped.get('other')
    if (other?.length) ordered.push({ key: 'other', label: 'Other', entries: other })
    return ordered
  }, [filtered])

  const onPick = (entry: IndexEntry) => {
    if (entry.installed) {
      setDetailName(entry.name)
    } else {
      setInstallingName(entry.name)
    }
  }

  const onInstalled = useCallback(() => {
    setInstallingName(null)
    void refresh()
  }, [refresh])

  const onRemoved = useCallback(() => {
    setDetailName(null)
    void refresh()
  }, [refresh])

  return (
    <>
      <section className={styles.root} aria-label="Coral sources">
        <div className={styles.header}>
          <div className={styles.headerInner}>
            <div className={styles.headerText}>
              <Typography.HeadingLarge as="h1">Sources</Typography.HeadingLarge>
              <Typography.Body variant="secondary">
                Manage sources for this workspace
              </Typography.Body>
            </div>

            <div className={styles.searchBar}>
              <TextInput
                ref={searchInputRef}
                value={search}
                onChange={setSearch}
                placeholder="Search sources…"
                icon="Search"
              />
            </div>
          </div>
        </div>

        {error ? (
          <div className={styles.statusPanel}>
            <ErrorBanner
              title="Couldn't load sources"
              message={error}
              onRetry={() => window.location.reload()}
            />
          </div>
        ) : null}

        <ScrollArea.Container className={styles.resultsScroll} constrainWidth fillContent>
          <div className={styles.resultsContent}>
            {loading ? (
              <div className={styles.loadingState}>
                <Icon name="Loader" size="16" color="tertiary" className={styles.spinAnimation} />
                <Typography.BodySmall variant="tertiary">Loading sources…</Typography.BodySmall>
              </div>
            ) : null}

            {!loading && !error && allEntries.length === 0 ? (
              <EmptyPage
                description="Check the Coral build for a populated catalog."
                iconName="Plug"
                title="No sources available"
              />
            ) : null}

            {connected.length > 0 ? (
              <Section title="Configured" count={connected.length}>
                <SourceCardList entries={connected} onPick={onPick} />
              </Section>
            ) : null}

            {sections.map((section) => (
              <Section key={section.key} title={section.label} count={section.entries.length}>
                <SourceCardList entries={section.entries} onPick={onPick} />
              </Section>
            ))}

            {connected.length === 0 &&
            sections.length === 0 &&
            !loading &&
            !error &&
            allEntries.length > 0 ? (
              <EmptyPage
                action={
                  <Button.TextButton onClick={() => setSearch('')} variant="secondary">
                    Clear search
                  </Button.TextButton>
                }
                description="Try adjusting your search."
                iconName="Search"
                title="No matching sources"
              />
            ) : null}
          </div>
        </ScrollArea.Container>
      </section>

      <SourceInstallDialog
        name={installingName}
        open={installingName !== null}
        onOpenChange={(open) => {
          if (!open) setInstallingName(null)
        }}
        onInstalled={onInstalled}
      />

      <SourceDetailDialog
        name={detailName}
        open={detailName !== null}
        onOpenChange={(open) => {
          if (!open) setDetailName(null)
        }}
        onRemoved={onRemoved}
      />
    </>
  )
}

function Section({
  title,
  count,
  children,
}: {
  title: string
  count: number
  children: React.ReactNode
}) {
  return (
    <div className={styles.categorySection}>
      <div className={styles.sectionHead}>
        <Typography.HeadingXSmall as="h2">{title}</Typography.HeadingXSmall>
        <span className={styles.sectionCount}>{count}</span>
      </div>
      {children}
    </div>
  )
}

function SourceCardList({
  entries,
  onPick,
}: {
  entries: IndexEntry[]
  onPick: (entry: IndexEntry) => void
}) {
  const entryById = new Map(entries.map((entry) => [sourceCardId(entry), entry]))
  const items = entries.map(toCardItem)

  return (
    <CardList
      items={items}
      onSelect={(item) => {
        const entry = entryById.get(item.id)
        if (entry) onPick(entry)
      }}
    />
  )
}

function toCardItem(entry: IndexEntry): CardItem {
  return {
    description: entry.description,
    headerPill: sourceOriginPill(entry),
    icon: <ProviderLogo name={entry.name} size="small" />,
    id: sourceCardId(entry),
    title: entry.name,
  }
}

function sourceOriginPill(entry: IndexEntry): CardItem['headerPill'] {
  if (entry.origin === 'bundled') return { label: 'Core' }
  if (entry.origin === 'imported') return { label: 'Imported' }
  return undefined
}

function sourceCardId(entry: IndexEntry) {
  return `${entry.origin}:${entry.name}`
}

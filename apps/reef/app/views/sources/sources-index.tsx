import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useRevalidator } from 'react-router'

import { Button, Card, ScrollArea } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import { EmptyPage } from '@/components/empty-page'
import { ErrorBanner } from '@/components/error-banner'
import { SOURCE_CATEGORY_ORDER, getCategoryForSource } from '@/lib/source-categories'
import type { CatalogEntry } from '@/lib/sources'

import { ProviderLogo } from './provider-logo'
import * as styles from './sources-index.css'

type IndexEntry = CatalogEntry

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

  const allEntries = useMemo<IndexEntry[]>(
    () => entries.toSorted((a, b) => a.name.localeCompare(b.name)),
    [entries],
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

  return (
    <section className={styles.root} aria-label="Coral sources">
      <div className={styles.header}>
        <div className={styles.headerInner}>
          <div className={styles.headerText}>
            <Typography.HeadingLarge as="h1">Sources</Typography.HeadingLarge>
            <Typography.Body variant="secondary">
              Connect external systems to query their data from Coral. Click a source to install or
              inspect it.
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

      {loadError ? (
        <div className={styles.statusPanel}>
          <ErrorBanner
            title="Couldn't load sources"
            message={loadError}
            onRetry={() => revalidator.revalidate()}
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

          {!loading && !loadError && allEntries.length === 0 ? (
            <EmptyPage
              description="Check the Coral build for a populated catalog."
              iconName="Plug"
              title="No sources available"
            />
          ) : null}

          {connected.length > 0 ? (
            <Section title="Configured" count={connected.length}>
              <SourceCardList entries={connected} />
            </Section>
          ) : null}

          {sections.map((section) => (
            <Section key={section.key} title={section.label} count={section.entries.length}>
              <SourceCardList entries={section.entries} />
            </Section>
          ))}

          {connected.length === 0 &&
          sections.length === 0 &&
          !loading &&
          !loadError &&
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

function SourceCardList({ entries }: { entries: IndexEntry[] }) {
  return (
    <Card.List>
      {entries.map((entry) => (
        <Card.Item key={sourceCardId(entry)}>
          <Card.Card
            as={Link}
            description={entry.description}
            headerPill={sourceOriginPill(entry)}
            icon={<ProviderLogo name={entry.name} size="small" />}
            prefetch="intent"
            preventScrollReset
            title={entry.name}
            to={`/sources/${encodeURIComponent(entry.name)}`}
          />
        </Card.Item>
      ))}
    </Card.List>
  )
}

function sourceOriginPill(entry: IndexEntry): Card.CardHeaderPill | undefined {
  if (entry.origin === 'bundled') return { label: 'Core' }
  if (entry.origin === 'imported') return { label: 'Imported' }
  return undefined
}

function sourceCardId(entry: IndexEntry) {
  return `${entry.origin}:${entry.name}`
}

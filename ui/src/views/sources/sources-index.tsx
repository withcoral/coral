import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import { ErrorBanner } from '@/components/error-banner'
import { providerIcon } from '@/lib/provider-icons'
import { SOURCE_CATEGORY_ORDER, getCategoryForSource } from '@/lib/source-categories'
import {
  discoverBundled,
  listInstalledSources,
  type CatalogEntry,
  type InstalledSource,
} from '@/lib/sources'

import { SourceDetailDialog } from './source-detail'
import { SourceInstallDialog } from './source-install'
import * as styles from './sources-index.css'

interface IndexEntry extends CatalogEntry {
  installedVersion?: string
}

export function SourcesIndex() {
  const [bundled, setBundled] = useState<CatalogEntry[] | null>(null)
  const [installed, setInstalled] = useState<InstalledSource[] | null>(null)
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
    const [installedRes, bundledRes] = await Promise.allSettled([
      listInstalledSources(),
      discoverBundled(),
    ])
    if (installedRes.status === 'fulfilled') setInstalled(installedRes.value)
    if (bundledRes.status === 'fulfilled') setBundled(bundledRes.value)
    const failure =
      installedRes.status === 'rejected'
        ? installedRes.reason
        : bundledRes.status === 'rejected'
          ? bundledRes.reason
          : null
    setError(failure ? (failure instanceof Error ? failure.message : String(failure)) : null)
  }, [])

  useEffect(() => {
    void refresh()
  }, [refresh])

  const loading = installed === null && bundled === null && !error

  const allEntries = useMemo<IndexEntry[]>(() => {
    const bundledByName = new Map((bundled ?? []).map((entry) => [entry.name, entry]))
    const merged: IndexEntry[] = (bundled ?? []).map((entry) => ({ ...entry }))
    for (const installedSource of installed ?? []) {
      const bundledEntry = bundledByName.get(installedSource.name)
      if (bundledEntry) {
        const target = merged.find((entry) => entry.name === installedSource.name)
        if (target) {
          target.installed = true
          target.installedVersion = installedSource.version
        }
      } else {
        merged.push({
          name: installedSource.name,
          description: '',
          version: installedSource.version,
          installed: true,
          origin: installedSource.origin === 'bundled' ? 'bundled' : 'imported',
          installedVersion: installedSource.version,
        })
      }
    }
    merged.sort((a, b) => a.name.localeCompare(b.name))
    return merged
  }, [bundled, installed])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return allEntries
    return allEntries.filter(
      (entry) =>
        entry.name.toLowerCase().includes(q) || entry.description.toLowerCase().includes(q),
    )
  }, [allEntries, search])

  const sections = useMemo(() => {
    const grouped = new Map<string, IndexEntry[]>()
    for (const entry of filtered) {
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

  const onInstalled = useCallback(
    (name: string) => {
      setInstallingName(null)
      void refresh()
      setDetailName(name)
    },
    [refresh],
  )

  const onRemoved = useCallback(() => {
    setDetailName(null)
    void refresh()
  }, [refresh])

  return (
    <div className={styles.root}>
      <div className={styles.container}>
        <div className={styles.header}>
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

        {error ? (
          <ErrorBanner
            title="Couldn't load sources"
            message={error}
            onRetry={() => window.location.reload()}
          />
        ) : null}

        {loading ? (
          <div className={styles.loadingState}>
            <Icon name="Loader" size="16" color="tertiary" className={styles.spinAnimation} />
            <Typography.BodySmall variant="tertiary">Loading sources…</Typography.BodySmall>
          </div>
        ) : null}

        {!loading && !error && allEntries.length === 0 ? (
          <div className={styles.emptyState}>
            <Icon name="Plug" size="24" color="tertiary" />
            <Typography.Body variant="secondary">
              No sources available. Check the Coral build for a populated catalog.
            </Typography.Body>
          </div>
        ) : null}

        {sections.map((section) => (
          <Section key={section.key} title={section.label} count={section.entries.length}>
            <div className={styles.cardGrid}>
              {section.entries.map((entry) => (
                <SourceCard
                  key={`${entry.origin}:${entry.name}`}
                  entry={entry}
                  onClick={() => onPick(entry)}
                />
              ))}
            </div>
          </Section>
        ))}

        {sections.length === 0 && !loading && !error && allEntries.length > 0 ? (
          <Typography.BodySmall variant="tertiary">
            No sources match your search.
          </Typography.BodySmall>
        ) : null}
      </div>

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
    </div>
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

function SourceCard({ entry, onClick }: { entry: IndexEntry; onClick: () => void }) {
  const icon = providerIcon(entry.name)
  return (
    <button type="button" onClick={onClick} className={styles.card}>
      <div className={styles.cardHeader}>
        <div className={styles.cardLogo}>
          {icon ? (
            <img alt="" src={icon} className={styles.cardLogoImg} />
          ) : (
            <Icon name="Plug" size="18" color="tertiary" />
          )}
        </div>
        <Typography.BodyLargeStrong as="span" className={styles.cardTitle}>
          {entry.name}
        </Typography.BodyLargeStrong>
        {entry.origin === 'imported' ? (
          <span className={styles.originPill}>Imported</span>
        ) : entry.origin === 'bundled' ? (
          <span className={styles.originPill}>Core</span>
        ) : null}
      </div>
      {entry.description ? (
        <Typography.Body variant="tertiary" className={styles.cardDescription}>
          {entry.description}
        </Typography.Body>
      ) : null}
      <div className={styles.cardFooter}>
        <Typography.BodySmall variant="tertiary">
          v{entry.installedVersion ?? entry.version}
        </Typography.BodySmall>
        {entry.installed ? (
          <span className={styles.connectedPill}>
            <Icon name="CircleCheck" size="14" color="inherit" />
            Connected
          </span>
        ) : null}
      </div>
    </button>
  )
}

import classNames from 'classnames'
import { useMemo } from 'react'
import type { Ref } from 'react'

import { animations } from '@/wax/animations'
import { Button, ScrollArea } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import { EmptyPage } from '@/components/empty-page'
import { ErrorBanner } from '@/components/error-banner'

import { buildSourceCatalogView, type SourceCatalogEntry } from './source-catalog'
import { SourceCardList, type SourceCardListInteraction } from './source-card-list'
import * as styles from './source-catalog-surface.css'

export type SourceCatalogLoadState = 'error' | 'idle' | 'loading'
export type SourceCatalogSurfaceVariant = 'compact' | 'full'

interface SourceCatalogSurfaceBaseProps {
  entries: SourceCatalogEntry[]
  errorMessage?: string | null
  headerAction?: React.ReactNode
  loadState?: SourceCatalogLoadState
  onRetry?: () => void
  onSearchChange: (search: string) => void
  search: string
  searchInputRef?: Ref<HTMLInputElement>
  showHeader?: boolean
  showSearch?: boolean
  showTitle?: boolean
  variant?: SourceCatalogSurfaceVariant
}

export type SourceCatalogSurfaceProps = SourceCatalogSurfaceBaseProps & SourceCardListInteraction

export function SourceCatalogSurface(props: SourceCatalogSurfaceProps) {
  const {
    entries,
    errorMessage = null,
    headerAction = null,
    loadState = 'idle',
    onRetry,
    onSearchChange,
    search,
    searchInputRef,
    showHeader = true,
    showSearch = true,
    showTitle = true,
    variant = 'full',
  } = props
  const { allEntries, connected, sections } = useMemo(
    () => buildSourceCatalogView(entries, search),
    [entries, search],
  )
  const cardListInteraction: SourceCardListInteraction = props.getEntryTo
    ? { getEntryTo: props.getEntryTo }
    : { onPick: props.onPick }
  const loading = loadState === 'loading'
  const error = loadState === 'error'
  const shouldShowHeader = showHeader && (showTitle || showSearch || headerAction !== null)

  return (
    <section
      className={classNames(styles.root, styles.rootVariant[variant])}
      aria-label="Coral sources"
    >
      {shouldShowHeader ? (
        <div className={styles.header}>
          <div className={classNames(styles.headerInner, styles.headerInnerVariant[variant])}>
            {showTitle ? (
              <div className={styles.headerText}>
                <Typography.HeadingLarge as="h1">Sources</Typography.HeadingLarge>
                <Typography.Body variant="secondary">
                  Connect external systems to query their data from Coral. Click a source to install
                  or inspect it.
                </Typography.Body>
              </div>
            ) : null}

            {showSearch ? (
              <div className={classNames(styles.searchBar, styles.searchBarVariant[variant])}>
                <TextInput
                  ref={searchInputRef}
                  value={search}
                  onChange={onSearchChange}
                  placeholder="Search sources…"
                  icon="Search"
                />
              </div>
            ) : null}

            {headerAction ? <div className={styles.headerAction}>{headerAction}</div> : null}
          </div>
        </div>
      ) : null}

      {error ? (
        <div className={classNames(styles.statusPanel, styles.statusPanelVariant[variant])}>
          <ErrorBanner
            title="Couldn't load sources"
            message={errorMessage ?? 'Check the Coral connection and try again.'}
            onRetry={onRetry}
          />
        </div>
      ) : null}

      <ScrollArea.Container className={styles.resultsScroll} constrainWidth fillContent>
        <div className={classNames(styles.resultsContent, styles.resultsContentVariant[variant])}>
          {loading ? (
            <div className={styles.loadingState}>
              <Icon name="Loader" size="16" color="tertiary" className={animations.spinAnimation} />
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
            <SourceCatalogSection count={connected.length} title="Configured" variant={variant}>
              <SourceCardList entries={connected} {...cardListInteraction} />
            </SourceCatalogSection>
          ) : null}

          {sections.map((section) => (
            <SourceCatalogSection
              count={section.count}
              key={section.key}
              title={section.title}
              variant={variant}
            >
              <SourceCardList entries={section.entries} {...cardListInteraction} />
            </SourceCatalogSection>
          ))}

          {connected.length === 0 &&
          sections.length === 0 &&
          !loading &&
          !error &&
          allEntries.length > 0 ? (
            <EmptyPage
              action={
                <Button.TextButton onClick={() => onSearchChange('')} variant="secondary">
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

function SourceCatalogSection({
  children,
  count,
  title,
  variant,
}: {
  children: React.ReactNode
  count: number
  title: string
  variant: SourceCatalogSurfaceVariant
}) {
  return (
    <div className={classNames(styles.categorySection, styles.categorySectionVariant[variant])}>
      <div className={styles.sectionHead}>
        <Typography.HeadingXSmall as="h2">{title}</Typography.HeadingXSmall>
        <span className={styles.sectionCount}>{count}</span>
      </div>
      {children}
    </div>
  )
}

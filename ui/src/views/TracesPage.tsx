import { useEffect, useRef, useState } from 'react'

import * as Button from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { Typography } from '@/wax/components/typography'
import type { TraceSummaryView } from '@/lib/trace-view-models'

import * as s from './traces-page.css'
import { EmptyState } from './traces/empty-state'
import { PageHeader } from './traces/page-header'
import { StatusBar } from './traces/status-bar'
import { TraceList } from './traces/trace-list'

function HeaderActions({
  searchOpen,
  searchText,
  searchVisible,
  setSearchOpen,
  setSearchText,
}: {
  searchOpen: boolean
  searchText: string
  searchVisible: boolean
  setSearchOpen: (value: boolean) => void
  setSearchText: (value: string) => void
}) {
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (searchOpen) inputRef.current?.focus()
  }, [searchOpen])

  return (
    <div className={s.headerActions}>
      <KeyboardShortcut
        handler={(e) => {
          e.preventDefault()
          setSearchOpen(true)
          inputRef.current?.select()
        }}
        shortcut="$mod+f"
      />
      <div className={s.inlineSearch} data-searching={searchVisible ? 'true' : undefined}>
        <div className={s.searchTrigger}>
          <Button.IconButton
            name="Search"
            onClick={() => setSearchOpen(true)}
            size="32"
            tooltipText="Search"
            variant="bare"
          />
        </div>
        <div className={s.searchField}>
          <TextInput
            icon="Search"
            onBlur={() => setSearchOpen(false)}
            onChange={setSearchText}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                setSearchText('')
                setSearchOpen(false)
                inputRef.current?.blur()
              }
            }}
            placeholder="Search queries..."
            ref={inputRef}
            value={searchText}
          />
        </div>
      </div>
    </div>
  )
}

function DisconnectedBanner({ message }: { message: string }) {
  return (
    <div className={s.disconnectedBanner}>
      <Typography.Body as="span">{message}</Typography.Body>
    </div>
  )
}

export function TracesPage({
  error,
  loading,
  onSelectTrace,
  searchText,
  setSearchText,
  traces,
}: {
  error: string | null
  loading: boolean
  onSelectTrace: (traceId: string) => void
  searchText: string
  setSearchText: (value: string) => void
  traces: TraceSummaryView[]
}) {
  const [searchOpen, setSearchOpen] = useState(false)
  const [activeIndex, setActiveIndex] = useState<number | null>(null)
  const searchVisible = searchOpen || searchText.trim().length > 0

  const filtered = traces.filter((trace) => {
    const needle = searchText.trim().toLowerCase()
    if (!needle) return true
    return `${trace.query} ${trace.name} ${trace.traceId}`.toLowerCase().includes(needle)
  })

  useEffect(() => setActiveIndex(null), [searchText])

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      const target = event.target
      const inEditable =
        target instanceof HTMLElement &&
        (target.isContentEditable || target.matches('textarea, [role="textbox"]'))
      if (inEditable) return

      if (event.key === 'ArrowDown') {
        if (filtered.length === 0) return
        event.preventDefault()
        setActiveIndex((index) => (index === null ? 0 : Math.min(filtered.length - 1, index + 1)))
      } else if (event.key === 'ArrowUp') {
        if (filtered.length === 0) return
        event.preventDefault()
        setActiveIndex((index) => (index === null ? filtered.length - 1 : Math.max(0, index - 1)))
      } else if (event.key === 'Enter') {
        if (activeIndex === null || !filtered[activeIndex]) return
        if (
          target instanceof HTMLElement &&
          target.matches('button, a, [role="button"], [role="link"]')
        )
          return
        event.preventDefault()
        onSelectTrace(filtered[activeIndex].traceId)
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [activeIndex, filtered, onSelectTrace])

  useEffect(() => {
    if (activeIndex === null) return
    const trace = filtered[activeIndex]
    if (!trace) return
    const escaped = trace.traceId.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
    document.querySelector(`[data-trace-row-id="${escaped}"]`)?.scrollIntoView({ block: 'nearest' })
  }, [activeIndex, filtered])

  const connected = !error
  return (
    <section className={s.root} aria-label="Coral traces">
      <PageHeader title="Query stream" isSearching={searchVisible}>
        <HeaderActions
          searchOpen={searchOpen}
          searchText={searchText}
          searchVisible={searchVisible}
          setSearchOpen={setSearchOpen}
          setSearchText={setSearchText}
        />
      </PageHeader>
      {error && <DisconnectedBanner message={error} />}
      {loading && traces.length === 0 ? (
        <div className={s.loadingState}>
          <Icon name="Loader" className={s.spinner} color="tertiary" />
          <Typography.Body>Loading traces…</Typography.Body>
        </div>
      ) : filtered.length === 0 ? (
        searchText.trim() ? (
          <EmptyState
            title={`No results for “${searchText.trim()}”`}
            details="Try a different search or clear the search field to show all traces."
          />
        ) : (
          <EmptyState error={error && traces.length === 0 ? error : null} />
        )
      ) : (
        <div className={s.queryScroll}>
          <TraceList
            activeTraceId={activeIndex !== null ? filtered[activeIndex]?.traceId : null}
            traces={filtered}
            onSelect={onSelectTrace}
          />
        </div>
      )}
      <StatusBar connected={connected} count={filtered.length} totalCount={traces.length} />
    </section>
  )
}

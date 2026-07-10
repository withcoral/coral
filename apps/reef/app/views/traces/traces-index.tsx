import { useEffect, useRef, useState } from 'react'
import { Outlet, useLocation, useNavigate, useRevalidator } from 'react-router'

import { Button } from '@/wax/components'
import { TextInput } from '@/wax/components/inputs/text'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { Typography } from '@/wax/components/typography'
import { EmptyPage } from '@/components/empty-page'

import * as s from './traces.css'
import { PageHeader } from './page-header'
import { StatusBar } from './status-bar'
import { TraceList } from './trace-list'
import type { TraceSummaryData } from './trace-utils'

const TRACE_LIST_REFRESH_MS = 30_000

export interface TracesOutletContext {
  traces: TraceSummaryData[]
}

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

export function TracesIndex({
  endpointLabel,
  loadError,
  referenceTimeMs: loadedReferenceTimeMs,
  traces: loadedTraces,
}: {
  endpointLabel: string
  loadError: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
}) {
  const location = useLocation()
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const revalidationInFlight = useRef(false)
  const lastSuccessfulList = useRef({
    referenceTimeMs: loadedReferenceTimeMs,
    traces: loadedTraces,
  })
  if (!loadError) {
    lastSuccessfulList.current = {
      referenceTimeMs: loadedReferenceTimeMs,
      traces: loadedTraces,
    }
  }
  const { referenceTimeMs, traces } = loadError
    ? lastSuccessfulList.current
    : { referenceTimeMs: loadedReferenceTimeMs, traces: loadedTraces }
  const [searchText, setSearchText] = useState('')
  const [searchOpen, setSearchOpen] = useState(false)
  const [activeIndex, setActiveIndex] = useState<number | null>(null)
  const searchVisible = searchOpen || searchText.trim().length > 0
  const listIsActive = location.pathname === '/traces'

  const filtered = traces.filter((trace) => {
    const needle = searchText.trim().toLowerCase()
    if (!needle) return true
    return `${trace.query} ${trace.name} ${trace.traceId}`.toLowerCase().includes(needle)
  })

  useEffect(() => setActiveIndex(null), [searchText])

  useEffect(() => {
    if (!listIsActive) return
    const handler = (event: KeyboardEvent) => {
      const target = event.target
      const inEditable =
        target instanceof HTMLElement &&
        (target.isContentEditable || target.matches('input, textarea, select, [role="textbox"]'))
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
        navigate({
          pathname: `/traces/${encodeURIComponent(filtered[activeIndex].traceId)}`,
          search: location.search,
        })
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [activeIndex, filtered, listIsActive, location.search, navigate])

  useEffect(() => {
    if (activeIndex === null) return
    const trace = filtered[activeIndex]
    if (!trace) return
    const escaped = trace.traceId.replace(/\\/g, '\\\\').replace(/"/g, '\\"')
    document.querySelector(`[data-trace-row-id="${escaped}"]`)?.scrollIntoView({ block: 'nearest' })
  }, [activeIndex, filtered])

  useEffect(() => {
    if (!listIsActive) return
    const interval = window.setInterval(() => {
      if (revalidator.state !== 'idle' || revalidationInFlight.current) return
      revalidationInFlight.current = true
      void revalidator.revalidate().finally(() => {
        revalidationInFlight.current = false
      })
    }, TRACE_LIST_REFRESH_MS)
    return () => window.clearInterval(interval)
  }, [listIsActive, revalidator])

  const connected = !loadError
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
      {loadError && <DisconnectedBanner message={loadError} />}
      {filtered.length === 0 ? (
        searchText.trim() ? (
          <EmptyPage
            action={
              <Button.TextButton onClick={() => setSearchText('')} variant="secondary">
                Clear search
              </Button.TextButton>
            }
            description="Try a different search or clear the search field to show all traces."
            iconName="Search"
            title={`No results for “${searchText.trim()}”`}
          />
        ) : (
          <EmptyPage
            description={
              loadError && traces.length === 0
                ? loadError
                : 'Make sure tracing is enabled, then run a SQL query to see it here in real-time.'
            }
            iconName={loadError && traces.length === 0 ? 'CircleAlert' : 'Activity'}
            title={loadError && traces.length === 0 ? 'Tracing unavailable' : 'No queries yet'}
          />
        )
      ) : (
        <div className={s.queryScroll}>
          <TraceList
            activeTraceId={activeIndex !== null ? filtered[activeIndex]?.traceId : null}
            referenceTimeMs={referenceTimeMs}
            traces={filtered}
          />
        </div>
      )}
      <StatusBar
        connected={connected}
        count={filtered.length}
        endpointLabel={endpointLabel}
        totalCount={traces.length}
      />
      <Outlet context={{ traces: filtered } satisfies TracesOutletContext} />
    </section>
  )
}

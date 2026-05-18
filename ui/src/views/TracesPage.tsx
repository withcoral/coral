import { useCallback, useEffect, useRef, useState } from 'react'

import * as Button from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'
import { listTraces } from '@/lib/coral-traces-client'
import type { TraceSummary } from '@/generated/coral/v1/traces_pb'

import * as s from './traces-page.css'
import { EmptyState } from './traces/empty-state'
import { PageHeader } from './traces/page-header'
import { StatusBar } from './traces/status-bar'
import { TraceDetail } from './traces/trace-detail'
import { TraceList } from './traces/trace-list'
import { formatTraceError, isQueryTrace } from './traces/trace-utils'

const MAX_QUERY_TRACES = 80
const TRACE_LIST_PAGE_SIZE = 100
const MAX_TRACE_LIST_PAGES = 2
const TRACE_LIST_REFRESH_MS = 30_000

function useTraceList(enabled: boolean) {
  const [traces, setTraces] = useState<TraceSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const refreshInFlight = useRef(false)

  const refresh = useCallback(async (quiet = false) => {
    if (refreshInFlight.current) return
    refreshInFlight.current = true
    if (!quiet) setLoading(true)
    try {
      const queryTraces: TraceSummary[] = []
      let pageToken = ''

      for (let page = 0; page < MAX_TRACE_LIST_PAGES && queryTraces.length < MAX_QUERY_TRACES; page += 1) {
        const response = await listTraces(TRACE_LIST_PAGE_SIZE, pageToken)
        queryTraces.push(...response.traces.filter(isQueryTrace))
        pageToken = response.nextPageToken
        if (!pageToken) break
      }

      setError(null)
      setTraces(queryTraces.slice(0, MAX_QUERY_TRACES))
    } catch (err) {
      setError(formatTraceError(err instanceof Error ? err.message : String(err)))
    } finally {
      refreshInFlight.current = false
      if (!quiet) setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (!enabled) return
    void refresh()
    const interval = window.setInterval(() => void refresh(true), TRACE_LIST_REFRESH_MS)
    return () => window.clearInterval(interval)
  }, [enabled, refresh])

  return { error, loading, traces }
}

function HeaderActions({ onClearSearch, searchText, setSearchText, setShowSearch, showSearch }: {
  onClearSearch: () => void
  searchText: string
  setSearchText: (value: string) => void
  setShowSearch: (value: boolean) => void
  showSearch: boolean
}) {
  return (
    <div className={s.headerActions}>
      {showSearch ? (
        <div className={s.inlineSearch}>
          <div className={s.inlineSearchField}>
            <TextInput
              autoFocus
              icon="Search"
              onChange={setSearchText}
              placeholder="Search queries..."
              type="search"
              value={searchText}
            />
          </div>
          <Button.IconButton name="X" onClick={onClearSearch} size="32" tooltipText="Close search" variant="bare" />
        </div>
      ) : (
        <Button.IconButton name="Search" onClick={() => setShowSearch(true)} size="32" tooltipText="Search queries" variant="bare" />
      )}
    </div>
  )
}

function DisconnectedBanner({ message }: { message: string }) {
  return <div className={s.disconnectedBanner}><Typography.Body as="span">{message}</Typography.Body></div>
}

export function TracesPage() {
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null)
  const { error, loading, traces } = useTraceList(selectedTraceId === null)
  const [searchText, setSearchText] = useState('')
  const [showSearch, setShowSearch] = useState(false)

  const filtered = traces.filter((trace) => {
    const needle = searchText.trim().toLowerCase()
    if (!needle) return true
    return `${trace.query} ${trace.name} ${trace.traceId}`.toLowerCase().includes(needle)
  })

  if (selectedTraceId) {
    const selectedIndex = filtered.findIndex((trace) => trace.traceId === selectedTraceId)
    const newerTraceId = selectedIndex > 0 ? filtered[selectedIndex - 1].traceId : null
    const olderTraceId = selectedIndex >= 0 && selectedIndex < filtered.length - 1 ? filtered[selectedIndex + 1].traceId : null

    return (
      <TraceDetail
        newerTraceId={newerTraceId}
        olderTraceId={olderTraceId}
        onClose={() => setSelectedTraceId(null)}
        onSelectTrace={setSelectedTraceId}
        traceId={selectedTraceId}
      />
    )
  }

  const connected = !error
  return (
    <section className={s.root} aria-label="Coral traces">
      <PageHeader title="Query Stream">
        <HeaderActions
          onClearSearch={() => { setShowSearch(false); setSearchText('') }}
          searchText={searchText}
          setSearchText={setSearchText}
          setShowSearch={setShowSearch}
          showSearch={showSearch}
        />
      </PageHeader>
      {error && <DisconnectedBanner message={error} />}
      {loading && traces.length === 0 ? (
        <div className={s.loadingState}><Icon name="Loader" className={s.spinner} color="tertiary" /><Typography.Body>Loading traces…</Typography.Body></div>
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
        <div className={s.queryScroll}><TraceList traces={filtered} onSelect={setSelectedTraceId} /></div>
      )}
      <StatusBar connected={connected} count={filtered.length} totalCount={traces.length} />
    </section>
  )
}

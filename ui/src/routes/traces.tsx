import { useCallback, useEffect, useMemo, useRef } from 'react'
import {
  useLoaderData,
  useLocation,
  useNavigate,
  useRevalidator,
  useSearchParams,
  type LoaderFunctionArgs,
} from 'react-router'

import { getTrace, listTraces } from '@/lib/coral-traces-client'
import {
  mapTraceDetail,
  mapTraceSummary,
  type TraceDetailView,
  type TraceSummaryView,
} from '@/lib/trace-view-models'
import { TracesPage } from '@/views/TracesPage'
import { TraceDetail } from '@/views/traces/trace-detail'
import { formatTraceError, isQueryTrace } from '@/views/traces/trace-utils'

const MAX_QUERY_TRACES = 80
const TRACE_LIST_PAGE_SIZE = 100
const MAX_TRACE_LIST_PAGES = 2
const TRACE_LIST_REFRESH_MS = 30_000

export interface TraceListRouteData {
  error: string | null
  traces: TraceSummaryView[]
}

export interface TraceDetailRouteData extends TraceListRouteData {
  detail: TraceDetailView | null
  detailError: string | null
  traceId: string
}

async function loadTraceSummaries(): Promise<TraceSummaryView[]> {
  const queryTraces: TraceSummaryView[] = []
  let pageToken = ''

  for (
    let page = 0;
    page < MAX_TRACE_LIST_PAGES && queryTraces.length < MAX_QUERY_TRACES;
    page += 1
  ) {
    const response = await listTraces(TRACE_LIST_PAGE_SIZE, pageToken)
    queryTraces.push(...response.traces.map(mapTraceSummary).filter(isQueryTrace))
    pageToken = response.nextPageToken
    if (!pageToken) break
  }

  return queryTraces.slice(0, MAX_QUERY_TRACES)
}

export async function tracesLoader(): Promise<TraceListRouteData> {
  try {
    return { error: null, traces: await loadTraceSummaries() }
  } catch (err) {
    return {
      error: formatTraceError(err instanceof Error ? err.message : String(err)),
      traces: [],
    }
  }
}

export async function traceDetailLoader({
  params,
}: LoaderFunctionArgs): Promise<TraceDetailRouteData> {
  const traceId = params.traceId
  if (!traceId) throw new Response('Missing trace id', { status: 400 })

  const [listData, detailResult] = await Promise.allSettled([tracesLoader(), getTrace(traceId)])
  const resolvedListData =
    listData.status === 'fulfilled'
      ? listData.value
      : {
          error: formatTraceError(
            listData.reason instanceof Error ? listData.reason.message : String(listData.reason),
          ),
          traces: [],
        }

  if (detailResult.status === 'rejected') {
    return {
      ...resolvedListData,
      detail: null,
      detailError: formatTraceError(
        detailResult.reason instanceof Error
          ? detailResult.reason.message
          : String(detailResult.reason),
      ),
      traceId,
    }
  }

  return {
    ...resolvedListData,
    detail: mapTraceDetail(detailResult.value),
    detailError: null,
    traceId,
  }
}

function traceDetailPath(traceId: string): string {
  return `/traces/${encodeURIComponent(traceId)}`
}

function searchMatchesTrace(trace: TraceSummaryView, searchText: string): boolean {
  const needle = searchText.trim().toLowerCase()
  if (!needle) return true
  return `${trace.query} ${trace.name} ${trace.traceId}`.toLowerCase().includes(needle)
}

function useLastGoodTraceData(data: TraceListRouteData) {
  const lastGoodTraces = useRef(data.traces)
  if (!data.error) lastGoodTraces.current = data.traces
  return data.error ? lastGoodTraces.current : data.traces
}

function withSearch(path: string, search: string): string {
  return search ? `${path}${search}` : path
}

export function TracesRoute() {
  const data = useLoaderData() as TraceListRouteData
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const [searchParams, setSearchParams] = useSearchParams()
  const traces = useLastGoodTraceData(data)
  const loading = revalidator.state !== 'idle' && traces.length === 0 && !data.error
  const searchText = searchParams.get('q') ?? ''
  const routeSearch = searchParams.toString()
  const routeSearchSuffix = routeSearch ? `?${routeSearch}` : ''
  const refresh = useCallback(() => {
    if (revalidator.state === 'idle') void revalidator.revalidate()
  }, [revalidator])
  const setSearchText = useCallback(
    (next: string) => {
      setSearchParams(
        (current) => {
          const params = new URLSearchParams(current)
          if (next.trim()) params.set('q', next)
          else params.delete('q')
          return params
        },
        { replace: true },
      )
    },
    [setSearchParams],
  )

  useEffect(() => {
    const interval = window.setInterval(refresh, TRACE_LIST_REFRESH_MS)
    return () => window.clearInterval(interval)
  }, [refresh])

  return (
    <TracesPage
      error={data.error}
      loading={loading}
      onSelectTrace={(traceId) => navigate(withSearch(traceDetailPath(traceId), routeSearchSuffix))}
      searchText={searchText}
      setSearchText={setSearchText}
      traces={traces}
    />
  )
}

export function TraceDetailRoute() {
  const data = useLoaderData() as TraceDetailRouteData
  const location = useLocation()
  const navigate = useNavigate()
  const searchText = new URLSearchParams(location.search).get('q') ?? ''
  const filteredTraces = useMemo(
    () => data.traces.filter((trace) => searchMatchesTrace(trace, searchText)),
    [data.traces, searchText],
  )
  const selectedIndex = filteredTraces.findIndex((trace) => trace.traceId === data.traceId)
  const newerTraceId = selectedIndex > 0 ? filteredTraces[selectedIndex - 1]?.traceId : null
  const olderTraceId =
    selectedIndex >= 0 && selectedIndex < filteredTraces.length - 1
      ? filteredTraces[selectedIndex + 1]?.traceId
      : null
  const initialSummary = useMemo(
    () =>
      data.detail?.summary ??
      (selectedIndex >= 0
        ? filteredTraces[selectedIndex]
        : data.traces.find((trace) => trace.traceId === data.traceId)),
    [data.detail?.summary, data.traceId, data.traces, filteredTraces, selectedIndex],
  )

  return (
    <TraceDetail
      detail={data.detail}
      error={data.detailError}
      initialSummary={initialSummary}
      loading={false}
      newerTraceId={newerTraceId}
      olderTraceId={olderTraceId}
      onClose={() => navigate(withSearch('/', location.search))}
      onSelectTrace={(traceId) => navigate(withSearch(traceDetailPath(traceId), location.search))}
      traceId={data.traceId}
    />
  )
}

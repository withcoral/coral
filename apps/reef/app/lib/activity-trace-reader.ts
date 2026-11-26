import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useFetcher } from 'react-router'
import type { ActivityPage } from '@/views/activity/types'
import { activityPageFetchKey, fetchedActivityPage } from './activity-page-fetch'
import {
  absorbPage,
  coveredBy,
  markStale,
  nextTraceRequest,
  openTrace,
  receiveTrace,
  revalidateTrace,
  traceProgress,
  traceQuery,
  type ActivityTraceStore,
  type TracePreference,
  type TraceRequest,
} from './activity-trace'

/**
 * How long the trace waits after it marked what can change before it marks again, in
 * milliseconds. Live snapshots can arrive many times a second; one mark then covers them,
 * and the reads a mark starts are one for each changeable window of a parent.
 */
export const TRACE_REFRESH_INTERVAL_MS = 2000

/**
 * How long a record that changes no more stays on screen before later evidence makes the
 * trace read it once more, in milliseconds. That read costs as much as the first one, so
 * it is rare; it is how the trace learns that Coral no longer returns a record. A read of
 * everything that takes longer than this ends before the next one starts.
 */
export const TRACE_REVALIDATE_INTERVAL_MS = 60_000

/** The Activity loader's path for the list `page` belongs to. */
export function activityLoaderPath(page: ActivityPage) {
  // Every Workspace list reads through the Activity route; an Agent's through its own.
  return page.agent ? page.listPath : `${page.listPath.replace(/\/activity$/, '')}/activity`
}

export interface ActivityTraceReader {
  store: ActivityTraceStore
  /** Starts reading again after a failed read. */
  retry: () => void
  /**
   * Reads these children before the rest: each names a Task or Run and the ordinal of the
   * child you need, such as the one beside the rows on screen.
   */
  prefer: (wanted: TracePreference[]) => void
  /**
   * The page was concealed, and the trace has not read back what it held before. The
   * rows you read are on their way, so a reading position waits for them.
   */
  restoring?: boolean
}

interface Held {
  /**
   * One for each trace the reader starts: for a root you open, and again after the page
   * was concealed. An answer joins only the trace that asked for it. A number, counted in
   * state: the compiler keeps one object for every `{}` a render makes.
   */
  epoch: number
  store: ActivityTraceStore
  /** What the store last joined of a page. It joins once, so it never undoes a later read. */
  page: Pick<ActivityPage, 'rows' | 'branches' | 'selectedRow'>
}
const samePage = (a: Held['page'], b: Held['page']) =>
  a.rows === b.rows && a.branches === b.branches && a.selectedRow === b.selectedRow

/**
 * Reads everything under the top-level record `root` in the background, through the
 * Activity loader `page` came from, and keeps it current. `evidence` changes whenever
 * Coral reports a change, such as a live snapshot: the trace then reads again only what
 * can still change, and at most once a minute everything, so a record Coral no longer
 * returns goes. With a null root, it reads nothing. While `page` is concealed, as after observation
 * lost its authority, it shows, reads and keeps nothing.
 */
export function useActivityTrace(
  page: ActivityPage,
  root: string | null,
  evidence: unknown,
): ActivityTraceReader | null {
  const concealed = page.listUnavailable
  const { state: fetchState, data: fetchData, load } = useFetcher({ key: 'activity-trace' })
  const [held, setHeld] = useState<Held | null>(null)
  const [started, setStarted] = useState(0)
  let current = held
  // Whatever hid the page can have ended your access: what the trace held goes, and the
  // page that shows again reads its root from the start.
  if (!root || concealed) current = null
  // Whatever the list itself holds of the root joins at once, without a read.
  else if (held?.store.root !== root) {
    current = { epoch: started + 1, store: absorbPage(openTrace(root), page), page }
    setStarted(started + 1)
  } else if (!samePage(held.page, page))
    current = { ...held, store: absorbPage(held.store, page), page }
  if (current !== held) setHeld(current)
  const store = current?.store ?? null
  const [restoring, setRestoring] = useState(false)
  const settled = store && (traceProgress(store).complete || store.failed || store.unavailable)
  if (concealed ? !restoring : restoring && (!root || settled)) setRestoring(concealed)

  const prefer = useRef<TracePreference[]>([])
  // The read in flight, with the fetcher's data from before it. A quick load can start and
  // end between two renders, so only new data, not a state change, says it has answered.
  // `listed`: what the list held when it left. `marked`: later evidence came while it was
  // on its way.
  const flight = useRef<{
    epoch: number
    request: TraceRequest
    before: unknown
    listed: Held['page']
    marked: boolean
  } | null>(null)
  // An answer joined the trace in this commit, so this render's store still lacks it.
  const joining = useRef(false)
  const path = activityLoaderPath(page)
  const epoch = current?.epoch ?? null
  const latest = useRef({ store, page, epoch })
  useEffect(() => {
    latest.current = { store, page, epoch }
    joining.current = false
  })
  // When this trace started to read. What it holds is no older than that.
  const opened = useRef<{ epoch: number | null; at: number }>({ epoch: null, at: 0 })
  useEffect(() => {
    opened.current = { epoch, at: Date.now() }
  }, [epoch])

  // Marks what can still change, at most once per interval: for later evidence, and after
  // an answer that can be older than what the trace held. `all`: such an answer has replaced
  // what the list's page holds, so that is marked too. `news`: later evidence came.
  const refreshed = useRef({ evidence, at: 0 })
  const owed = useRef<{
    timer: ReturnType<typeof setTimeout> | null
    all: boolean
    news: boolean
  }>({ timer: null, all: false, news: false })
  const markSoon = useCallback((all: boolean) => {
    const mark = () => {
      const { all: everything, news } = owed.current
      owed.current = { timer: null, all: false, news: false }
      refreshed.current.at = Date.now()
      const { store: shown, page: listed, epoch: reading } = latest.current
      if (!shown) return
      // From when the trace started to read, or last asked to read everything again: the
      // first evidence after a quiet minute reads everything again, not the one after that.
      // While the read before still goes on, the trace declines, and the minute starts over.
      const again =
        opened.current.epoch === reading &&
        Date.now() - opened.current.at >= TRACE_REVALIDATE_INTERVAL_MS
      if (again) opened.current.at = Date.now()
      // Only later evidence makes the read in flight a late one. A mark that repeats a late
      // answer brings nothing new: the read in flight left after that answer, so what it
      // returns is no older. Else each such mark would make the next, and the reads would
      // never end.
      if (news && flight.current?.epoch === reading) flight.current.marked = true
      setHeld((value) => {
        if (value?.epoch !== reading) return value
        const stale = markStale(
          value.store,
          everything ? undefined : coveredBy(value.store, listed),
        )
        const marked = again ? revalidateTrace(stale) : stale
        return marked === value.store ? value : { ...value, store: marked }
      })
    }
    if (all) owed.current.all = true
    else owed.current.news = true
    if (owed.current.timer) return
    const wait = refreshed.current.at + TRACE_REFRESH_INTERVAL_MS - Date.now()
    if (wait <= 0) mark()
    else owed.current.timer = setTimeout(mark, wait)
  }, [])
  useEffect(
    () => () => {
      if (owed.current.timer) clearTimeout(owed.current.timer)
    },
    [],
  )
  useEffect(() => {
    if (refreshed.current.evidence === evidence) return
    refreshed.current.evidence = evidence
    markSoon(false)
  }, [evidence, markSoon])

  // The answer to the read in flight joins the trace that asked for it. An answer for a
  // root you left, or from before the page was concealed, is dropped: the trace that shows
  // now starts its own read at once.
  useEffect(() => {
    const asked = flight.current
    if (!asked || fetchState !== 'idle' || fetchData === asked.before) return
    flight.current = null
    if (latest.current.epoch !== asked.epoch) return
    // The next read waits for the store this answer makes.
    joining.current = true
    const answer = fetchedActivityPage(fetchData)
    setHeld((value) => {
      if (value?.epoch !== asked.epoch) return value
      // A list page that changed while the read was on its way is later than its answer.
      const newer = samePage(asked.listed, value.page) ? undefined : value.page
      return { ...value, store: receiveTrace(value.store, asked.request, answer, newer) }
    })
    // Coral can have given this answer before the evidence, or the list's snapshot, that
    // came while it was on its way. Nothing says which of them is the later one, and the
    // answer has replaced what the snapshot held. So the trace asks Coral once more for
    // everything that can change, also what the list holds, and that answer decides. A Run
    // the answer withholds and the later snapshot shows is asked for at once, in the store. It
    // asks at the rate of the marks, so a list that never stops changing costs no more,
    // and it asks once: when the evidence stops, so do the reads.
    if (asked.marked || !samePage(asked.listed, latest.current.page)) markSoon(true)
  }, [fetchState, fetchData, markSoon])

  // One read at a time, until the trace asks for nothing more.
  useEffect(() => {
    if (!store || !epoch || concealed || flight.current || joining.current) return
    if (fetchState !== 'idle') return
    const request = nextTraceRequest(store, prefer.current)
    if (!request) return
    flight.current = {
      epoch,
      request,
      before: fetchData,
      listed: latest.current.page,
      marked: false,
    }
    const query = traceQuery(store, request)
    query.set(activityPageFetchKey, 'page')
    void load(`${path}?${query}`)
  }, [store, epoch, concealed, load, fetchState, fetchData, path])

  const retry = useCallback(
    () =>
      setHeld((value) =>
        value?.store.failed ? { ...value, store: { ...value.store, failed: false } } : value,
      ),
    [],
  )
  const setPrefer = useCallback((wanted: TracePreference[]) => {
    prefer.current = wanted
  }, [])
  const reader = useMemo(
    () => (store ? { store, retry, prefer: setPrefer, restoring } : null),
    [store, retry, setPrefer, restoring],
  )
  return concealed ? null : reader
}

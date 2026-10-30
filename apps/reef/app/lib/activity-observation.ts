import { useEffect, useRef, useState } from 'react'
import { atom, useAtomValue, useStore } from 'jotai'
import { useFetcher, useLocation, useNavigate, useNavigation, useRevalidator } from 'react-router'
import {
  activityNavigation,
  activitySelection,
  branchCoordinate,
  selectionKeys,
  shownRecordOf,
} from '@/views/activity/navigation'
import type { ActivityPage } from '@/views/activity/types'
import { readingWindow, type ActivityViewport } from './activity-live-types'
import {
  failPage,
  liveRoots,
  loadsAutomatically,
  mergeActivityPage,
  openActivityList,
  pageQuery,
  receivePage,
  startPage,
  type ActivityList,
  type ActivityListDirection,
} from './activity-list'
import { activityPageFetchKey, fetchedActivityPage } from './activity-page-fetch'
import { subscribeActivityStream } from './activity-stream'
import { activitySearchKey } from './activity-search'

const idle = () => {}
// Selection, expansion and reading position never change which list is loaded.
const windowIndependentKeys = new Set<string>([
  ...selectionKeys,
  'pendingAfter',
  'expand',
  'collapse',
  'tree',
  'anchor',
  'before',
  'after',
  'root',
  'rootMode',
])

export interface LiveActivityState {
  /** The last authorized page. Its rows and branches give way to `list`. */
  page: ActivityPage
  list: ActivityList
  /** Counts the lists this reader opened. A navigation that joins the list keeps it. */
  generation: number
  /** This page joined the list the reader had loaded, rather than opening a new one. */
  joined: boolean
  connected: boolean
  sessionExpired: boolean
  canFollowRoot: boolean
  followRoot: boolean
  viewports: Record<string, ActivityViewport>
  /** The records the live window watches. */
  roots: string[]
  /** The top-level records on screen, first to last. */
  visible: string[]
}
export type ActivityReadingChange =
  | { kind: 'root'; atStart: boolean }
  | { kind: 'viewport'; parent: string; viewport: ActivityViewport }
  | { kind: 'visible'; records: string[] }
  | { kind: 'load'; direction: ActivityListDirection }
  /** A null page is a load that failed. */
  | { kind: 'loaded'; direction: ActivityListDirection; page: ActivityPage | null }
function conceal(page: ActivityPage): ActivityPage {
  return {
    ...page,
    rows: [],
    pending: page.pending
      ? { actions: [], actionCount: '0', next: '', unavailable: true }
      : undefined,
    branches: {},
    hasBefore: false,
    hasAfter: false,
    detail: null,
    selectedRow: null,
    unavailable: Boolean(page.selected),
    listUnavailable: true,
  }
}
/** The page the view renders: the authorized page around the loaded list. */
export function activityView({ page, list }: LiveActivityState): ActivityPage {
  // A concealed page hides the loaded list too, until observation is authorized again.
  if (page.listUnavailable) return page
  return {
    ...page,
    rows: list.rows,
    branches: list.branches,
    hasBefore: list.hasNewer,
    hasAfter: list.hasOlder,
    anchorUnavailable: list.anchorUnavailable,
  }
}
// A list that opened unavailable cannot follow; a snapshot that conceals it pauses only.
const canFollow = (list: ActivityList, initial: ActivityPage) =>
  !list.hasNewer && !list.anchorUnavailable && !initial.listUnavailable
const sameNames = (a: string[], b: string[]) =>
  a.length === b.length && a.every((value, index) => value === b[index])

/**
 * One mounted reader, one cancelable observation. A navigation inside the same list
 * passes the previous state, so the loaded records stay and the fresh page joins them.
 */
export function activityObservation(
  initial: ActivityPage,
  path: string,
  search: string,
  previous?: LiveActivityState,
) {
  const query = activitySelection(initial, search)
  const window = readingWindow(initial)
  const joined = previous && mergeActivityPage(previous.list, initial, { retained: false })
  // Whether the anchor resolved is the fresh page's answer, not the earlier list's.
  const list = joined
    ? { ...joined, anchorUnavailable: initial.anchorUnavailable }
    : openActivityList(initial)
  const canFollowRoot = canFollow(list, initial)
  window.followRoot = canFollowRoot && (joined ? previous.followRoot : true)
  const visible = joined ? previous.visible : list.rows.slice(0, 10).map((row) => row.name)
  window.roots = liveRoots(list, visible, initial.expanded)
  for (const [parent, viewport] of Object.entries(window.branches)) {
    if (!query.has(`offset.${parent}`)) continue
    viewport.anchor = query.get(`anchor.${parent}`) ?? ''
    viewport.preceding = Number(query.get(`preceding.${parent}`) ?? 0)
  }
  const state = atom<LiveActivityState>({
    page: initial,
    list,
    generation: (previous?.generation ?? 0) + (joined ? 0 : 1),
    joined: Boolean(joined),
    connected: true,
    sessionExpired: false,
    canFollowRoot,
    followRoot: window.followRoot,
    viewports: { ...window.branches },
    roots: [...window.roots],
    visible,
  })
  for (const [parent, anchor] of Object.entries(initial.anchors ?? {}))
    if (!query.has(`anchor.${parent}`)) query.set(`anchor.${parent}`, anchor)
  // The live connection reads the window when it connects. Its snapshot is the newest
  // page while it follows the newest records, and only the watched records otherwise.
  let snapshotFollows = window.followRoot
  // Only a server event renews the authorization lease. A connection replaced before
  // its first snapshot renews nothing, so a window change waits for that snapshot.
  let answered = false
  let deferred = false
  // Child windows the reader moved after the open connection read its window. That
  // connection's snapshots still report the offsets it was opened with.
  const moved = new Set<string>()
  const input = () => {
    snapshotFollows = window.followRoot
    answered = false
    moved.clear()
    return { path, search: query.toString(), window }
  }
  let wake = idle
  /** Recomputes the watched records after the list or the viewport changes. */
  const watch = (current: LiveActivityState): LiveActivityState => {
    const followable = canFollow(current.list, initial)
    const roots = liveRoots(current.list, current.visible, initial.expanded)
    let changed = false
    if (!sameNames(roots, window.roots)) {
      window.roots = roots
      // A following window reads the newest page, whatever records it names.
      changed = !window.followRoot
    }
    if (!followable && window.followRoot) {
      window.followRoot = false
      changed = true
    }
    if (changed) wake()
    return {
      ...current,
      canFollowRoot: followable,
      followRoot: window.followRoot,
      roots: [...roots],
    }
  }
  const reader = atom(
    (get) => get(state),
    (get, set, change: ActivityReadingChange) => {
      const current = get(state)
      if (change.kind === 'root') {
        const follow = current.canFollowRoot && change.atStart
        if (window.followRoot === follow) return
        window.followRoot = follow
        set(state, { ...current, followRoot: follow })
        wake()
      } else if (change.kind === 'viewport') {
        if (!initial.expanded.includes(change.parent)) return
        const old = window.branches[change.parent]
        const next = change.viewport
        if (
          old?.offset === next.offset &&
          old.anchor === next.anchor &&
          old.preceding === next.preceding
        )
          return
        window.branches[change.parent] = next
        moved.add(change.parent)
        set(state, { ...current, viewports: { ...window.branches } })
        wake()
      } else if (change.kind === 'visible') {
        if (sameNames(change.records, current.visible)) return
        set(state, watch({ ...current, visible: change.records }))
      } else if (change.kind === 'load') {
        set(state, { ...current, list: startPage(current.list, change.direction) })
      } else {
        const next = change.page
          ? receivePage(current.list, change.direction, change.page)
          : failPage(current.list, change.direction)
        if (next !== current.list) set(state, watch({ ...current, list: next }))
      }
    },
  )
  state.onMount = (set) => {
    const stream = subscribeActivityStream('/activity/observe', {
      input,
      onValue: ({ page }) => {
        answered = true
        if (deferred) {
          deferred = false
          wake()
        }
        for (const [parent, branch] of Object.entries(page.branches)) {
          if (moved.has(parent)) continue
          const previousViewport = window.branches[parent] ?? {
            anchor: '',
            preceding: 0,
            offset: 0,
          }
          window.branches[parent] = { ...previousViewport, offset: branch.offset ?? 0 }
        }
        set((current) =>
          watch({
            ...current,
            page,
            // A snapshot never drops or reorders loaded records.
            list:
              mergeActivityPage(current.list, page, { retained: !snapshotFollows }) ?? current.list,
            connected: true,
            viewports: { ...window.branches },
          }),
        )
      },
      onUnavailable: () =>
        set((current) => ({ ...current, page: conceal(current.page), connected: false })),
      onSessionExpired: () =>
        set((current) => ({
          ...current,
          page: conceal(current.page),
          connected: false,
          sessionExpired: true,
        })),
    })
    // Coalesce scrolling. The authorization lease is independent of this delay.
    wake = () => {
      if (answered) stream.reconnect(120)
      else deferred = true
    }
    return () => {
      wake = idle
      stream.close()
    }
  }
  return reader
}

/** The Activity loader's address for the next page of the list `page` belongs to. */
function pageHref(page: ActivityPage, search: string, query: Record<string, string>) {
  const current = new URLSearchParams(search)
  const next = new URLSearchParams({ tree: '1', ...query, [activityPageFetchKey]: 'page' })
  const expanded = new Set(current.getAll('expand'))
  for (const name of expanded) next.append('expand', name)
  for (const key of [activitySearchKey, 'collapse'])
    for (const value of current.getAll(key)) next.append(key, value)
  // Records that the page brings in open their child windows where the URL keeps them.
  for (const [key, value] of current) {
    const parent = branchCoordinate(key)
    if (parent && expanded.has(parent)) next.append(key, value)
  }
  // Every Workspace list reads through the Activity route; an Agent's through its own.
  const path = page.agent ? page.listPath : `${page.listPath.replace(/\/activity$/, '')}/activity`
  return `${path}?${next}`
}

/** One feature-specific reader owns observation, navigation, and identity-only continuity.
 * A loader replacement inside the same list joins the loaded records; any other
 * replacement opens a new list from its freshly authorized page.
 */
export function useActivityReader(initial: ActivityPage) {
  const location = useLocation()
  const navigate = useNavigate()
  const transition = useNavigation()
  const store = useStore()
  const revalidator = useRevalidator()
  const fetcher = useFetcher()
  useEffect(() => {
    const query = new URLSearchParams(location.search)
    if (
      ((!initial.initialRequest || query.has('request')) &&
        (!initial.initialRecord || query.get('record') === initial.initialRecord)) ||
      transition.state !== 'idle' ||
      revalidator.state !== 'idle'
    )
      return
    const selected = activitySelection(initial, location.search)
    // This only names the exact selection already returned by the loader. Normal
    // navigation, mutations and observation still perform their authorized reads.
    void navigate(`${location.pathname}?${selected}${location.hash}`, {
      replace: true,
      preventScrollReset: true,
      defaultShouldRevalidate: false,
    })
  }, [
    initial,
    location.pathname,
    location.search,
    location.hash,
    navigate,
    transition.state,
    revalidator.state,
  ])

  const query = new URLSearchParams(location.search)
  const rootQuery = new URLSearchParams(query)
  const keys = [...rootQuery.keys()]
  for (const key of keys) {
    if (windowIndependentKeys.has(key) || branchCoordinate(key)) rootQuery.delete(key)
  }
  rootQuery.sort()
  const listKey = `${initial.listPath}?${rootQuery}`
  const [reader, setReader] = useState(() => ({
    initial,
    listKey,
    atom: activityObservation(initial, location.pathname, location.search),
  }))
  let current = reader
  if (reader.initial !== initial) {
    const previous = reader.listKey === listKey ? store.get(reader.atom) : undefined
    current = {
      initial,
      listKey,
      atom: activityObservation(initial, location.pathname, location.search, previous),
    }
    setReader(current)
  }
  const state = useAtomValue(current.atom)
  const page = activityView(state)
  const firstVisible = state.visible.find((name) =>
    state.list.rows.some((row) => row.name === name),
  )
  const navigation = activityNavigation(page, location.pathname, location.search, {
    anchor: state.followRoot ? null : (firstVisible ?? null),
    branches: state.viewports,
  })
  // Every list is one Workspace's, so losing the list means losing that Workspace.
  const lostContext = state.connected && state.page.listUnavailable && !initial.listUnavailable
  const revalidated = useRef<ActivityPage>(undefined)
  useEffect(() => {
    // Surrounding management data does not lend its authority to exact detail. An expired
    // session revalidates too, so the protected route sends the reader to sign in.
    if ((lostContext || state.sessionExpired) && revalidated.current !== initial) {
      revalidated.current = initial
      void revalidator.revalidate()
    }
  }, [lostContext, state.sessionExpired, initial, revalidator])

  // One page loads at a time per list. A revalidated fetcher repeats an old page, so
  // only the load this list asked for joins it; a newer list's load replaces it.
  const requested = useRef<{ direction: ActivityListDirection; atom: typeof current.atom }>(
    undefined,
  )
  useEffect(() => {
    if (fetcher.state !== 'idle' || requested.current?.atom !== current.atom) return
    const { direction } = requested.current
    requested.current = undefined
    store.set(current.atom, { kind: 'loaded', direction, page: fetchedActivityPage(fetcher.data) })
  }, [fetcher.state, fetcher.data, store, current.atom])
  const load = (direction: ActivityListDirection) => {
    const latest = store.get(current.atom)
    const next = pageQuery(latest.list, direction)
    if (!next || requested.current?.atom === current.atom) return
    requested.current = { direction, atom: current.atom }
    store.set(current.atom, { kind: 'load', direction })
    void fetcher.load(pageHref(latest.page, location.search, next))
  }
  const control = (direction: ActivityListDirection, available: boolean) =>
    available
      ? {
          load: () => load(direction),
          loading: state.list.loading?.direction === direction,
          failed: state.list.failed === direction,
          automatic: loadsAutomatically(state.list),
        }
      : null

  const initialAnchor = Object.entries(initial.anchors ?? {})
    .toSorted(([a], [b]) => Number(b.includes('/runs/')) - Number(a.includes('/runs/')))
    .map(
      ([parent, anchor]) =>
        initial.branches[parent]?.rows.find(
          (row) => row.name === anchor || (row.kind === 'run' && row.submission?.name === anchor),
        )?.name,
    )
    .find(Boolean)
  const initialOffset = Object.entries(initial.branches).find(
    ([parent, branch]) =>
      query.has(`offset.${parent}`) &&
      Number(query.get(`offset.${parent}`)) > 0 &&
      branch.rows.length,
  )?.[1]
  // A Run's final value selects its Run, whose detail shows it. The table reveals
  // the final value's own row, which the rail and the cursor show.
  const shownRecord = shownRecordOf(initial.selected, query.get('record')) ?? undefined
  // A new list reveals what its URL names. Inside a list, a new selection is revealed
  // only when it is off screen: the table does not move a record that is in view.
  // A new list without the final value's row (its Run is collapsed, or its ending Event
  // stands for it) reveals the Run.
  const newListRecord =
    initial.selected &&
    shownRecord !== initial.selected &&
    !initial.branches[initial.selected]?.rows.some((row) => row.name === shownRecord)
      ? initial.selected
      : shownRecord
  const revealRecord = state.joined
    ? shownRecord
    : newListRecord && (Object.keys(initial.anchors ?? {}).length || query.has('anchor'))
      ? newListRecord
      : (initialAnchor ??
        initialOffset?.rows.find((row) => row.position === initialOffset.offset)?.name)
  return {
    navigation,
    page,
    windowKey: `${listKey}#${state.generation}`,
    revealRecord,
    list: {
      count: state.list.rows.length,
      newer: control('newer', state.list.hasNewer),
      older: control('older', state.list.hasOlder),
    },
    live: {
      connected: state.connected,
      followRoot: state.followRoot,
      atStart: (atStart: boolean) => store.set(current.atom, { kind: 'root', atStart }),
      viewport: (parent: string, viewport: ActivityViewport) =>
        store.set(current.atom, { kind: 'viewport', parent, viewport }),
      visible: (records: string[]) => store.set(current.atom, { kind: 'visible', records }),
    },
  }
}

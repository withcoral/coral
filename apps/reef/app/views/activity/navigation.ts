import { createContext, useContext } from 'react'
import { useLocation, useSearchParams, type Location } from 'react-router'
import { rootOf } from '@/lib/activity-root'
import type { ActivityPage, ActivityRow, RequestDetail } from './types'

export const ActivityNavigationContext = createContext<ReturnType<
  typeof activityNavigation
> | null>(null)

/** Query keys that only select a record and its review, never the root or the listed window. */
export const selectionKeys = ['record', 'request', 'actionsAfter'] as const
const clearSelection = Object.fromEntries(selectionKeys.map((key) => [key, null]))
/** Query keys that position the list or name the open root. */
const listKeys = new Set<string>([...selectionKeys, 'root', 'anchor', 'before', 'after'])

/** Whether the query selects a record or positions the list, which only a complete list can show. */
export function readsActivity(query: URLSearchParams) {
  return [...query.keys()].some((key) => listKeys.has(key))
}

/** The record on its Activity page: its root open, the record selected, and its request, if any. */
export function revealHref(row: ActivityRow, options: { request?: string } = {}) {
  const next = new URLSearchParams({ root: rootOf(row), record: row.name })
  if (options.request) next.set('request', options.request)
  const path = row.agent ? `/${row.agent}` : `/${row.workspace}/activity`
  return `${path}?${next}`
}

/**
 * Whether a navigation leaves the review of `request`. Moving between an approval's Run and
 * its call keeps the review, and so does the address gaining the request the loader resolved.
 */
export const leavesActivityReview =
  (request: RequestDetail | null | undefined) => (current: Location, next: Location) => {
    const from = new URLSearchParams(current.search)
    const to = new URLSearchParams(next.search)
    const record = from.get('record') ?? current.pathname.slice(1)
    // The approval's Run and its call, in either order.
    const pair = request?.kind === 'approval' ? [request.run, request.invocation] : []
    const sameReview =
      pair.includes(record) && pair.includes(to.get('record') ?? '') && record !== to.get('record')
    return (
      current.pathname !== next.pathname ||
      (from.get('record') !== to.get('record') && !sameReview) ||
      (from.get('request') ?? request?.name ?? null) !== to.get('request')
    )
  }

/**
 * The address with what the loader resolved pinned into it: the request a review opened, the
 * record an exact approval selected, and the root the page opened. A live update then reads
 * the same root, never a newer one.
 */
export function activitySelection(page: ActivityPage, search: string): URLSearchParams {
  const query = new URLSearchParams(search)
  if (!query.has('request') && page.initialRequest) query.set('request', page.initialRequest)
  if (page.initialRecord) query.set('record', page.initialRecord)
  if (!query.has('root') && page.trace) query.set('root', page.trace.root.name)
  return query
}

/** Where the reader is in the list: the first top-level record on screen, or null at the newest. */
export interface ActivityReading {
  anchor: string | null
}

/** Links within the Activity page. They carry identity only: the loader reauthorizes each. */
export function activityNavigation(
  page: ActivityPage,
  pathname: string,
  search: string,
  reading: ActivityReading = { anchor: new URLSearchParams(search).get('anchor') },
) {
  const context = activitySelection(page, search)
  // Pages are no longer links. The reading anchor names the position instead.
  for (const key of ['before', 'after']) context.delete(key)
  const href = (changes: Record<string, string | string[] | null>, path = page.listPath) => {
    const next = new URLSearchParams(context)
    // A link keeps the reading position, so a reload opens the list there.
    if (!('anchor' in changes) && path === page.listPath) {
      if (reading.anchor) next.set('anchor', reading.anchor)
      else next.delete('anchor')
    }
    // A stable Run route carries its selection in the path, not a competing query value.
    if (pathname !== page.listPath && page.selected) next.set('record', page.selected)
    for (const [key, value] of Object.entries(changes)) {
      next.delete(key)
      for (const item of Array.isArray(value) ? value : value ? [value] : []) next.append(key, item)
    }
    return `${path}?${next}`
  }
  /** Selects `name` in the open root. */
  const recordHref = (name: string) => href({ ...clearSelection, record: name })
  /** Opens another root, with nothing selected. */
  const rootHref = (name: string) => href({ ...clearSelection, root: name })
  const backHref = href(clearSelection)
  return { href, recordHref, rootHref, revealHref, backHref }
}

export function useActivityNavigation(page: ActivityPage) {
  const reader = useContext(ActivityNavigationContext)
  const [query] = useSearchParams()
  const { pathname } = useLocation()
  return reader ?? activityNavigation(page, pathname, query.toString())
}

/** A Run's final value, which its Run's detail shows. */
export const finalValue = (run: string) => `${run}/final`

/** The record a selection shows: its Run's final value when the URL's `record` names it. */
export const shownRecordOf = (selected: string | null, record: string | null) =>
  selected && record === finalValue(selected) ? record : selected

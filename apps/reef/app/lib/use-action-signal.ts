import { createContext, useContext, useEffect, useState } from 'react'
import { useRevalidator } from 'react-router'
import { atom, useAtomValue } from 'jotai'
import type { ActionCount, ActionCounts, ActionCountKind } from './activity-live-types'
import { subscribeActivityStream } from './activity-stream'

const digits = (value: unknown) => typeof value === 'string' && /^\d+$/.test(value)

function isCount(value: unknown): value is ActionCount {
  if (!value || typeof value !== 'object') return false
  const { total, approvals, connections, revisions } = value as Partial<ActionCount>
  return (
    digits(total) &&
    digits(approvals) &&
    digits(connections) &&
    typeof revisions?.approvals === 'string' &&
    typeof revisions.connections === 'string'
  )
}

function actionCounts(value: unknown): ActionCounts {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid counts')
  for (const count of Object.values(value))
    if (count !== null && !isCount(count)) throw new Error('Invalid count')
  return value as ActionCounts
}

/** The last counts stay this long after the stream reports a drop, then clear. A silent
 * stall reports only when its 15 s lease ends, so a count can stay about 45 s after the
 * last sign of life. */
const STALE_MS = 30_000

/** The one required-actions signal. `counts` keep their last value through a short drop;
 * `connected` turns false at once, for a page that must not present stale rows as current. */
export interface ActionSignal {
  counts: ActionCounts | null
  connected: boolean
}

/** The app shell's signal. A view outside the shell has no stream and counts as connected. */
export const ActionSignalContext = createContext<ActionSignal>({ counts: null, connected: true })

/** One shell-owned subscription for every Workspace's count. A short drop keeps the last
 * counts until the next snapshot replaces them all. A long drop or an expired session
 * conceals them (null). Reconnect never starts from a cached authority grant or a replay
 * cursor: the next snapshot alone sets the counts.
 */
export function useActionSignal(initial: ActionCounts | null) {
  const [signal] = useState(() => {
    const value = atom<ActionSignal>({ counts: initial, connected: true })
    value.onMount = (set) => {
      let stale: ReturnType<typeof setTimeout> | undefined
      const fresh = () => {
        clearTimeout(stale)
        stale = undefined
      }
      const stream = subscribeActivityStream('/activity/count', {
        onValue: (event) => {
          set({ counts: actionCounts(event.counts), connected: true })
          fresh()
        },
        onUnavailable: () => {
          set((current) => ({ ...current, connected: false }))
          stale ??= setTimeout(() => set({ counts: null, connected: false }), STALE_MS)
        },
        onSessionExpired: () => {
          fresh()
          set({ counts: null, connected: false })
        },
      })
      return () => {
        fresh()
        stream.close()
      }
    }
    return value
  })
  return useAtomValue(signal)
}

/** Keeps a page's loader data on one kind's revision in `workspace` (its path), and returns
 * whether the page is current. `loaded` is the revision the loader read before its rows, so
 * a change between that read and this mount still refreshes. An in-place refresh keeps
 * component state, and a newer one replaces any still in flight.
 */
export function useFollowRequiredActions(
  workspace: string,
  kind: ActionCountKind,
  loaded: string | undefined,
) {
  const { counts, connected } = useContext(ActionSignalContext)
  const { revalidate } = useRevalidator()
  const count = counts?.[workspace]
  const revision = count?.revisions[kind]
  useEffect(() => {
    if (revision !== undefined && revision !== loaded) void revalidate()
  }, [revision, loaded, revalidate])
  // A Workspace the stream no longer lists has no signal either, as when it was archived.
  return connected && (counts === null || Boolean(count))
}

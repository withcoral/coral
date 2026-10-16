import { useState } from 'react'
import { atom, useAtomValue } from 'jotai'
import type { ActionCounts } from './activity-live-types'
import { subscribeActivityStream } from './activity-stream'

function actionCounts(value: unknown): ActionCounts {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid counts')
  for (const count of Object.values(value))
    if (count !== null && (typeof count !== 'string' || !/^\d+$/.test(count)))
      throw new Error('Invalid count')
  return value as ActionCounts
}

/** The last counts stay this long after the stream reports a drop, then clear. A silent
 * stall reports only when its 15 s lease ends, so a count can stay about 45 s after the
 * last sign of life. */
const STALE_MS = 30_000

/** One shell-owned subscription for every Workspace's count. A short drop keeps the last
 * counts until the next snapshot replaces them all. A long drop or an expired session
 * conceals them (null). Reconnect never starts from a cached authority grant or a replay
 * cursor: the next snapshot alone sets the counts.
 */
export function useActionCounts(initial: ActionCounts | null) {
  const [counts] = useState(() => {
    const value = atom<ActionCounts | null>(initial)
    value.onMount = (set) => {
      let stale: ReturnType<typeof setTimeout> | undefined
      const fresh = () => {
        clearTimeout(stale)
        stale = undefined
      }
      const stream = subscribeActivityStream('/activity/count', {
        onValue: (event) => {
          set(actionCounts(event.counts))
          fresh()
        },
        onUnavailable: () => {
          stale ??= setTimeout(() => set(null), STALE_MS)
        },
        onSessionExpired: () => {
          fresh()
          set(null)
        },
      })
      return () => {
        fresh()
        stream.close()
      }
    }
    return value
  })
  return useAtomValue(counts)
}

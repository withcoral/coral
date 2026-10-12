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

/** One shell-owned subscription for every Workspace's count. Failures conceal the counts
 * (null); reconnect starts from fresh counts, never from a cached authority grant or a
 * replay cursor.
 */
export function useActionCounts(initial: ActionCounts | null) {
  const [counts] = useState(() => {
    const value = atom<ActionCounts | null>(initial)
    value.onMount = (set) => {
      const stream = subscribeActivityStream('/activity/count', {
        onValue: (event) => set(actionCounts(event.counts)),
        onUnavailable: () => set(null),
        onSessionExpired: () => set(null),
      })
      return () => stream.close()
    }
    return value
  })
  return useAtomValue(counts)
}

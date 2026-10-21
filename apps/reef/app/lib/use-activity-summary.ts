import { useState } from 'react'
import { atom, useAtomValue } from 'jotai'
import type { ActivitySummaryData, SummaryDays } from './activity-summary'
import { subscribeActivityStream } from './activity-stream'

interface ShownSummary {
  days: SummaryDays
  summary: ActivitySummaryData
}

/** The loader's summary, replaced by each newer one the stream pushes. A drop or an
 * expired session keeps the last numbers: they stay true for their window. A new period
 * keeps the last numbers, with their own period, until the stream's first value for it.
 */
export function useActivitySummary(
  initial: ShownSummary,
  workspace: string,
  days: SummaryDays,
): ShownSummary {
  const key = `${workspace}/${days}`
  const [live, setLive] = useState(() => liveSummary(workspace, days))
  if (live.key !== key) setLive(liveSummary(workspace, days))
  const pushed = useAtomValue(live.value)
  const [shown, setShown] = useState(initial)
  const current = pushed ?? (initial.days === days ? initial.summary : undefined)
  if (current && (current !== shown.summary || shown.days !== days))
    setShown({ days, summary: current })
  return current ? { days, summary: current } : shown
}

function liveSummary(workspace: string, days: SummaryDays) {
  const value = atom<ActivitySummaryData | null>(null)
  value.onMount = (set) => {
    const stream = subscribeActivityStream('/activity/summary', {
      input: () => ({ workspace, days }),
      onValue: (event) => set(event.summary),
      onUnavailable: () => {},
      onSessionExpired: () => {},
    })
    return () => stream.close()
  }
  return { key: `${workspace}/${days}`, value }
}

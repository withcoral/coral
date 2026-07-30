import { useEffect, useState } from 'react'

import { coralDesktopApi, desktopErrorMessage, type DesktopUpdateState } from '@/lib/coral-desktop'

const IDLE_UPDATE_STATE = { status: 'idle' } as const satisfies DesktopUpdateState

export function useDesktopUpdateState(enabled: boolean): DesktopUpdateState {
  const [state, setState] = useState<DesktopUpdateState>(IDLE_UPDATE_STATE)

  useEffect(() => {
    if (!enabled) return

    const desktop = coralDesktopApi()
    if (!desktop) return

    let cancelled = false
    let receivedStateChange = false
    const unsubscribe = desktop.onUpdateStateChange((nextState) => {
      receivedStateChange = true
      if (!cancelled) setState(nextState)
    })

    void desktop
      .getUpdateState()
      .then((initialState) => {
        // Subscription happens first so an update cannot slip through between
        // the snapshot and listener setup. If an event wins that race, retain
        // the newer event instead of replacing it with the older snapshot.
        if (!cancelled && !receivedStateChange) setState(initialState)
      })
      .catch((reason: unknown) => {
        if (!cancelled) {
          console.error('Failed to read desktop update state:', desktopErrorMessage(reason))
        }
      })

    return () => {
      cancelled = true
      unsubscribe()
    }
  }, [enabled])

  return state
}

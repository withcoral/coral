import { atom } from 'jotai'

import { coralDesktopApi, desktopErrorMessage, type DesktopUpdateState } from '@/lib/coral-desktop'

const IDLE_UPDATE_STATE = { status: 'idle' } as const satisfies DesktopUpdateState

// The main process owns the update state and pushes it over IPC, which no route
// loader can express, so the atom holds the last published state and opens the
// bridge only while something reads it — every reader shares one subscription.
// Acting on that state is a request and belongs in a route action instead; see
// `routes/desktop-update-action.ts`. Web builds and the server have no bridge
// and stay idle, which the sidebar hides.
export const desktopUpdateStateAtom = atom<DesktopUpdateState>(IDLE_UPDATE_STATE)

desktopUpdateStateAtom.onMount = (set) => {
  const desktop = coralDesktopApi()
  if (!desktop) return

  // Subscription happens first so an update cannot slip through between the
  // snapshot and listener setup. If an event wins that race, retain the newer
  // event instead of replacing it with the older snapshot.
  let receivedStateChange = false
  const unsubscribe = desktop.onUpdateStateChange((nextState) => {
    receivedStateChange = true
    set(nextState)
  })

  void desktop
    .getUpdateState()
    .then((initialState) => {
      if (!receivedStateChange) set(initialState)
    })
    .catch((reason: unknown) => {
      console.error('Failed to read desktop update state:', desktopErrorMessage(reason))
    })

  return unsubscribe
}

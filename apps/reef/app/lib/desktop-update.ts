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

  // The snapshot is only useful until something newer arrives: an event on this
  // mount, or anything the next mount reads after this one ends. The store keeps
  // its value between mounts, so a late snapshot can still overwrite it.
  let snapshotIsCurrent = true

  // Subscription happens first so an update cannot slip through between the
  // snapshot and listener setup.
  const unsubscribe = desktop.onUpdateStateChange((nextState) => {
    snapshotIsCurrent = false
    set(nextState)
  })

  void desktop
    .getUpdateState()
    .then((initialState) => {
      if (snapshotIsCurrent) set(initialState)
    })
    .catch((reason: unknown) => {
      console.error('Failed to read desktop update state:', desktopErrorMessage(reason))
    })

  return () => {
    snapshotIsCurrent = false
    unsubscribe()
  }
}

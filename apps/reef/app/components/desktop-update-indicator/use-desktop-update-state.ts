import { useCallback, useEffect, useRef, useState } from 'react'

import { coralDesktopApi, desktopErrorMessage, type DesktopUpdateState } from '@/lib/coral-desktop'
import { addToast } from '@/wax/components/toast'

import type { DesktopUpdateIndicatorProps } from './desktop-update-indicator'

const IDLE_UPDATE_STATE = { status: 'idle' } as const satisfies DesktopUpdateState

type DesktopUpdateStateResult = Pick<
  DesktopUpdateIndicatorProps,
  'isPending' | 'onDownload' | 'onInstall'
> & { state: DesktopUpdateState }

export function useDesktopUpdateState(enabled: boolean): DesktopUpdateStateResult {
  const [state, setState] = useState<DesktopUpdateState>(IDLE_UPDATE_STATE)
  const [isPending, setIsPending] = useState(false)
  // The main process publishes `downloading` a round trip later, so the button
  // needs its own record of an in-flight request to stay disabled meanwhile.
  const pendingRef = useRef(false)

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

  const request = useCallback(
    (
      operation: (desktop: NonNullable<ReturnType<typeof coralDesktopApi>>) => Promise<void>,
      errorTitle: string,
      { resetOnSuccess = true }: { resetOnSuccess?: boolean } = {},
    ) => {
      const desktop = coralDesktopApi()
      if (!desktop || pendingRef.current) return

      pendingRef.current = true
      setIsPending(true)
      operation(desktop)
        .then<boolean, boolean>(
          () => resetOnSuccess,
          (reason: unknown) => {
            addToast('error', { description: desktopErrorMessage(reason), title: errorTitle })
            return true
          },
        )
        .then((reset) => {
          if (!reset) return
          pendingRef.current = false
          setIsPending(false)
        })
    },
    [],
  )

  const onDownload = useCallback(() => {
    request((desktop) => desktop.downloadUpdate(), 'Couldn’t download the update')
  }, [request])

  // Installing quits Coral, so the toast only ever reaches the user when the
  // hand-off refuses to start. A resolved request means the quit was accepted
  // and teardown is under way, which the window outlives — stay disabled rather
  // than inviting a second click at a button that is about to disappear.
  const onInstall = useCallback(() => {
    request((desktop) => desktop.installUpdate(), 'Couldn’t install the update', {
      resetOnSuccess: false,
    })
  }, [request])

  return { isPending, onDownload, onInstall, state }
}

export interface BeforeQuitEventLike {
  preventDefault: () => void
}

export interface ShutdownCoordinatorDeps {
  stopServices: () => Promise<void>
  installReadyUpdate: () => boolean
  quit: () => void
}

export interface ShutdownCoordinator {
  allowQuit: () => void
  beforeQuit: (event: BeforeQuitEventLike) => void
  isShuttingDown: () => boolean
  quitAfterUpdateFailure: () => void
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

export function createShutdownCoordinator(
  deps: ShutdownCoordinatorDeps,
): ShutdownCoordinator {
  let shutdownStarted = false
  let quitAllowed = false

  function quitNormally(): void {
    quitAllowed = true
    deps.quit()
  }

  function finishShutdown(): void {
    try {
      if (deps.installReadyUpdate()) return
    } catch (error) {
      console.error(`[coral-desktop] update hand-off failed: ${errorMessage(error)}`)
    }
    quitNormally()
  }

  function beforeQuit(event: BeforeQuitEventLike): void {
    if (quitAllowed) return

    event.preventDefault()
    if (shutdownStarted) return
    shutdownStarted = true

    void deps.stopServices().then(finishShutdown, (error: unknown) => {
      console.error(`[coral-desktop] service shutdown failed: ${errorMessage(error)}`)
      quitNormally()
    })
  }

  return {
    // Keep manual quits blocked while Squirrel fetches from the in-process
    // proxy. Electron calls this immediately before its update-driven quit.
    allowQuit: () => {
      quitAllowed = true
    },
    beforeQuit,
    isShuttingDown: () => shutdownStarted,
    quitAfterUpdateFailure: quitNormally,
  }
}

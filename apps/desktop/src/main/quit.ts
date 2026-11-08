// When Quit, Restart and an update stop Coral and start it again, apart from
// Electron so tests can drive it. Desktop supervises Coral (ADR 0012), and
// Coral outlives its last window (ADR 0014), so only a Quit that must stop
// Coral does.

import type { PersistedListener } from './ports'

export type StartOptions = {
  /** Start on a new port for this listener instead of the kept one. */
  replacePort?: PersistedListener
}

export type QuitLifecycle = {
  /** Quit Completely, Quit in the failure dialogs, SIGINT/SIGTERM. Always stops Coral. */
  quitCompletely(): void
  /** Wired to app 'before-quit'. */
  beforeQuit(event: { preventDefault(): void }): void
  /** powerMonitor 'shutdown': macOS is logging out, restarting or shutting down. */
  sessionEnding(): void
  /** Stop Coral, then start it again unless Quit began meanwhile. */
  restart(): void
  /** Start Coral (optionally on a new port); does nothing once Quit began. */
  start(options?: StartOptions): void
  /** True once Quit began, until an update hand-off fails. */
  quitting(): boolean
  /** The updater's stopCoral: stop before the installer runs. */
  stopForUpdate(): Promise<void>
  /** The updater's resumeCoral: the hand-off failed, so start again. */
  resumeAfterUpdate(): void
}

export function createQuitLifecycle(deps: {
  platform: NodeJS.Platform
  /** Stops both processes. */
  stopCoral: () => Promise<void>
  startCoral: (options?: StartOptions) => Promise<void>
  /** Quit in the Coral menu on macOS: closes the windows, and Coral keeps running. */
  closeWindows: () => void
  /** Electron's app.quit, which emits before-quit again. */
  exit: () => void
  onStartFailure: (error: unknown) => void
  log: (message: string) => void
}): QuitLifecycle {
  // Set once Quit begins. Nothing starts Coral after that, so Quit's stop
  // leaves no process behind.
  let quitting = false
  // Set by every Quit that Coral offers and that must stop it. On macOS, a quit
  // that arrives without it came from the Dock or the app switcher.
  let quitRequested = false
  // Set when macOS logs the person out, restarts or shuts down. This relies
  // on macOS posting that notice before it asks the apps to quit.
  let sessionEnding = false
  // Quit waits for both processes to stop, so none outlives the app.
  let stopped = false

  const startCoral = (options?: StartOptions) =>
    void deps.startCoral(options).catch(deps.onStartFailure)

  return {
    quitCompletely() {
      quitRequested = true
      deps.exit()
    },
    beforeQuit(event) {
      if (stopped) return
      event.preventDefault()
      if (quitting) return
      // On macOS, any other quit only closes the windows, so Coral keeps
      // serving the person's agents.
      if (deps.platform === 'darwin' && !quitRequested && !sessionEnding) return deps.closeWindows()
      quitting = true
      // The app quits even when a process does not stop cleanly.
      void deps
        .stopCoral()
        .catch((error: unknown) => deps.log(`quit: ${String(error)}`))
        .then(() => {
          stopped = true
          deps.exit()
        })
    },
    sessionEnding() {
      sessionEnding = true
    },
    restart() {
      void deps
        .stopCoral()
        .then(() => (quitting ? undefined : deps.startCoral()))
        .catch(deps.onStartFailure)
    },
    start(options) {
      if (quitting) return
      startCoral(options)
    },
    quitting: () => quitting,
    // The installer replaces the app only after both processes stop. Its own
    // quit then passes before-quit.
    async stopForUpdate() {
      quitting = true
      await deps.stopCoral()
      stopped = true
    },
    // If the hand-off fails, Coral starts again. This is the only way out of
    // Quit.
    resumeAfterUpdate() {
      stopped = false
      quitting = false
      startCoral()
    },
  }
}

// When Quit, Restart and an update stop Coral and start it again, apart from
// Electron so tests can drive it. Desktop supervises Coral (ADR 0012), and
// Coral outlives its last window (ADR 0014), so only a Quit that must stop
// Coral does.

import type { StartOptions } from './supervisor'

export type { StartOptions }

export type QuitLifecycle = {
  /** Quit Completely, Quit in the failure dialogs, SIGINT/SIGTERM. Always stops Coral. */
  quitCompletely(): void
  /** Wired to app 'before-quit'. */
  beforeQuit(event: { preventDefault(): void }): void
  /** powerMonitor 'shutdown': macOS is logging out, restarting or shutting down. */
  sessionEnding(): void
  /** Stop Coral, then start it again unless Quit began meanwhile. */
  restart(): void
  /** Start Coral (optionally on a new port); does nothing once Quit or an update began. */
  start(options?: StartOptions): void
  /** True once Quit or an update began, until a failed update starts Coral again. */
  quitting(): boolean
  /** The updater's stopCoral: stop before the installer runs. */
  stopForUpdate(): Promise<void>
  /** The updater's resumeCoral: the hand-off failed. Quit if Quit was asked meanwhile, else start again. */
  resumeAfterUpdate(): void
}

/**
 * Where Quit and an update stand. Only `moveTo` changes it.
 *
 * - `running`: Coral may run. Only here does anything start Coral.
 * - `quitting`: Quit stops Coral, then ends the app.
 * - `updating`: the updater stops Coral before its installer runs.
 * - `handingOff`: Coral is stopped, and the installer's quit may pass.
 * - `stopped`: Coral is stopped, and the app ends.
 *
 * `quitPending` keeps a Quit that arrives during an update, so a failed
 * hand-off ends the app instead of starting Coral again.
 */
type Phase =
  | { name: 'running' }
  | { name: 'quitting' }
  | { name: 'updating'; quitPending: boolean }
  | { name: 'handingOff'; quitPending: boolean }
  | { name: 'stopped' }

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
  let phase: Phase = { name: 'running' }
  const moveTo = (next: Phase) => {
    phase = next
  }
  // Set when macOS logs the person out, restarts or shuts down. This relies
  // on macOS posting that notice before it asks the apps to quit.
  let sessionEnding = false

  const startCoral = (options?: StartOptions) =>
    void deps.startCoral(options).catch(deps.onStartFailure)

  /** A Quit that must stop Coral. */
  function quit() {
    switch (phase.name) {
      case 'running':
        moveTo({ name: 'quitting' })
        // The app ends even when a process does not stop cleanly.
        void deps
          .stopCoral()
          .catch((error: unknown) => deps.log(`quit: ${String(error)}`))
          .then(() => {
            moveTo({ name: 'stopped' })
            deps.exit()
          })
        return
      case 'updating':
        // The update's stop is under way. The hand-off's end decides.
        return moveTo({ name: 'updating', quitPending: true })
      case 'handingOff':
      case 'stopped':
        // Coral is stopped, so the app can end now.
        return deps.exit()
      case 'quitting':
        return
    }
  }

  /** Leaves an update that did not hand off. */
  function resumeAfterUpdate() {
    if (phase.name !== 'handingOff') return
    if (phase.quitPending) {
      moveTo({ name: 'stopped' })
      return deps.exit()
    }
    moveTo({ name: 'running' })
    startCoral()
  }

  /** The update's stop ended; a Quit asked during it carries over. */
  function stopEnded() {
    moveTo({ name: 'handingOff', quitPending: phase.name === 'updating' && phase.quitPending })
  }

  return {
    quitCompletely: quit,
    beforeQuit(event) {
      // Once Coral is stopped, the app's own quit and the installer's pass.
      if (phase.name === 'handingOff' || phase.name === 'stopped') return
      event.preventDefault()
      // On macOS, a quit from the Dock or Command+Q only closes the windows,
      // so Coral keeps serving the person's agents.
      if (deps.platform === 'darwin' && !sessionEnding) return deps.closeWindows()
      quit()
    },
    sessionEnding() {
      sessionEnding = true
    },
    restart() {
      if (phase.name !== 'running') return
      void deps
        .stopCoral()
        .then(() => (phase.name === 'running' ? deps.startCoral() : undefined))
        .catch(deps.onStartFailure)
    },
    start(options) {
      if (phase.name === 'running') startCoral(options)
    },
    quitting: () => phase.name !== 'running',
    // The installer replaces the app only after both processes stop. Its own
    // quit then passes before-quit.
    async stopForUpdate() {
      if (phase.name !== 'running') return
      moveTo({ name: 'updating', quitPending: false })
      try {
        await deps.stopCoral()
      } catch (error) {
        // The installer does not run, so leave the update and let Quit work again.
        stopEnded()
        resumeAfterUpdate()
        throw error
      }
      stopEnded()
    },
    resumeAfterUpdate,
  }
}

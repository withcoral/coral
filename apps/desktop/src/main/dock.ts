// Whether Coral is in the macOS Dock and app switcher, apart from Electron so
// tests can drive it. docs/adr/0014-desktop-serves-in-the-background.md
// records why a Coral with no window leaves them.

/** macOS drops a Dock call that follows another within a second. */
export const DOCK_SETTLE_MS = 1100

/** Electron's `app.dock`, which only macOS has. */
export interface Dock {
  show(): Promise<void>
  hide(): void
}

/**
 * Returns the function that puts Coral in the Dock, or takes it out, to match
 * `wanted`. Call it whenever the answer may have changed. A change that comes
 * too soon after the last one waits, then follows what `wanted` says by then.
 */
export function createDockPresence(deps: {
  /** Absent where there is no Dock. */
  dock: Dock | undefined
  wanted: () => boolean
  log: (message: string) => void
}): () => void {
  const { dock } = deps
  // An app starts in the Dock.
  let shown = true
  let changedAt = -Infinity
  let waiting = false

  const sync = () => {
    if (!dock || waiting) return
    const wanted = deps.wanted()
    if (wanted === shown) return
    const wait = changedAt + DOCK_SETTLE_MS - Date.now()
    if (wait > 0) {
      waiting = true
      setTimeout(() => {
        waiting = false
        sync()
      }, wait)
      return
    }
    shown = wanted
    changedAt = Date.now()
    if (!wanted) return dock.hide()
    void dock.show().catch((error: unknown) => {
      // The next call tries again.
      shown = false
      deps.log(`dock: ${String(error)}`)
    })
  }
  return sync
}

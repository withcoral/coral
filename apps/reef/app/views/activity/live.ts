import { createContext, useContext } from 'react'

/** Whether the reader still holds an authorized live view. Decisions and local clocks
 * pause while it reconnects. Views outside a live reader count as connected.
 */
export const ActivityConnectedContext = createContext(true)

export function useActivityConnected() {
  return useContext(ActivityConnectedContext)
}

/** Tells the reader that you opened (true) or closed (false) the expanded viewer of a
 * payload or of the source. Live mode takes no step while one is open, so the record you
 * inspect stays. Views outside a reader have none.
 */
export const ActivityInspectContext = createContext<((expanded: boolean) => void) | undefined>(
  undefined,
)

export function useActivityInspect() {
  return useContext(ActivityInspectContext)
}

/** Your role in an archived Workspace, or null while it is active. Coral refuses
 * decisions and connections there, so views tell you to restore it instead.
 */
export const ActivityArchivedContext = createContext<'owner' | 'member' | null>(null)

export function useActivityArchived() {
  return useContext(ActivityArchivedContext)
}

/** Tells you to restore the archived Workspace before you `then`. */
export function restoreFirst(role: 'owner' | 'member', then: string) {
  return role === 'owner'
    ? `Restore this workspace to ${then}.`
    : `Ask a workspace owner to restore this workspace. Then ${then}.`
}

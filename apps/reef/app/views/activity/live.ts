import { createContext, useContext } from 'react'

/** Whether the reader still holds an authorized live view. Decisions and local clocks
 * pause while it reconnects. Views outside a live reader count as connected.
 */
export const ActivityConnectedContext = createContext(true)

export function useActivityConnected() {
  return useContext(ActivityConnectedContext)
}

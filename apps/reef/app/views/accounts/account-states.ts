import type { PillColor } from '@/wax/components/pill/pill'
import type { PersonalAccountState } from './types'

/** How each connection state reads, wherever an account's state shows. */
export const accountStates: Record<PersonalAccountState, { label: string; color: PillColor }> = {
  connected: { label: 'Connected', color: 'green' },
  not_connected: { label: 'Not connected', color: 'gray' },
  needs_reconnect: { label: 'Needs reconnect', color: 'amber' },
}

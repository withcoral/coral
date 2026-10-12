import { Pill } from '@/wax/components/pill'
import type { WorkspaceView } from './types'

export function StatePill({ workspace: w }: { workspace: Pick<WorkspaceView, 'state'> }) {
  return w.state === 'active' ? (
    <Pill color="green">Active</Pill>
  ) : (
    <Pill color="gray">Archived</Pill>
  )
}

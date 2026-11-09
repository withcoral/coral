import type { SendBlock, UsageDataView } from './types'

/** What a report never holds. "What Coral sends" below shows the rest. */
export const contents =
  'Coral sends anonymous usage counts. It never sends your programs, tool arguments, tool results, credentials or the names you give workspaces, providers, agents or functions. Coral sends nothing while sharing is off.'

export function place(kind: UsageDataView['kind']): string {
  return kind === 'desktop' ? 'your computer' : 'this server'
}

/** Why Coral sends nothing whatever the switch shows. */
export function blockReason(block: SendBlock, kind: UsageDataView['kind']): string {
  const cause =
    block.kind === 'build'
      ? 'This is a source build.'
      : block.variable === 'CORAL_TELEMETRY'
        ? 'CORAL_TELEMETRY is off.'
        : `${block.variable} is set.`
  return `${cause} Coral sends no usage data from ${place(kind)}, even when sharing is on.`
}

import type { SendBlock, UsageDataView } from './types'

/** What a report never holds. "Today so far" below shows the rest. */
export const contents =
  'Coral sends anonymous usage data. It never sends your programs, tool arguments, tool results, credentials or the names you give things.'

/** What a report holds and how to turn sharing off. */
export const docsUrl = 'https://withcoral.com/docs/reference/configuration#usage-data'

export function place(kind: UsageDataView['kind']): string {
  return kind === 'desktop' ? 'your computer' : 'this server'
}

/** Why sharing is off and the switch takes no change. */
export function blockReason(block: SendBlock, kind: UsageDataView['kind']): string {
  switch (block.kind) {
    case 'build':
      return 'Sharing is off in a source build.'
    case 'unknown':
      return `Sharing is off on ${place(kind)}.`
    case 'environment':
      return `Sharing is off while ${block.variable} is set on ${place(kind)}.`
  }
}

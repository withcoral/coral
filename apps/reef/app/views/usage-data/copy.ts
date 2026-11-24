import type { SendBlock, UsageDataView } from './types'

/** What a report never holds. "Today so far" below shows the rest. */
export const contents =
  'Coral sends anonymous usage data. It never sends your programs, tool arguments, tool results, credentials or the names you give things.'

/** What a report holds and how to turn sharing off. */
export const docsUrl = 'https://withcoral.com/docs/reference/configuration#usage-data'

export function place(kind: UsageDataView['kind']): string {
  return kind === 'desktop' ? 'your computer' : 'this server'
}

/** Why the switch shows off and takes no change. */
export function blockReason(block: SendBlock, kind: UsageDataView['kind']): string {
  switch (block.kind) {
    case 'build':
      return 'You cannot turn on sharing in a source build.'
    case 'unknown':
      return `You cannot turn on sharing for ${place(kind)}.`
    case 'environment':
      return `You cannot turn on sharing while ${block.variable} is set on ${place(kind)}.`
  }
}

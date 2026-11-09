import type { SendBlock } from './types'

/** What a report never holds. "What we send" below shows the rest word for word. */
export const contents =
  'Coral sends anonymous usage counts once a day. It never sends your programs, tool arguments, tool results, credentials or the names you give things.'

/** The same UTC form as a function's save time, on the server and in the browser. */
export function utc(at: string): string {
  return `${at.slice(0, 10)} ${at.slice(11, 16)} UTC`
}

export function blockReason(block: SendBlock): string {
  if (block.kind === 'build')
    return 'This is a source build. It shows the report but never sends it.'
  return block.variable === 'CORAL_TELEMETRY'
    ? 'CORAL_TELEMETRY is off, so this process sends nothing.'
    : `${block.variable} is set, so this process sends nothing.`
}

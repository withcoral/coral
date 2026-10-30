import type { Run } from './highlight'

/**
 * JavaScript's line terminators: CRLF ends one line; a lone CR, LF, U+2028 and U+2029
 * each end one. U+2028 is the only character in category Zl, and U+2029 in Zp.
 */
export const LINE_TERMINATOR = /\r\n|[\r\n\p{Zl}\p{Zp}]/gu

/** The lines of `code` as JavaScript counts them, without their terminators. */
export function sourceLines(code: string): string[] {
  return code.split(LINE_TERMINATOR)
}

/**
 * Groups highlighted runs into the lines `sourceLines` counts. A run that holds a
 * terminator is cut there and each part keeps its class; the terminators themselves
 * are not rendered. Lezer reports each LF as its own run, so a CRLF can arrive as a
 * run ending in CR followed by that LF run; it still ends one line.
 */
export function splitLines(runs: Run[]): Run[][] {
  const lines: Run[][] = [[]]
  let endedByCR = false
  for (const run of runs) {
    const text: string = endedByCR && run.text.startsWith('\n') ? run.text.slice(1) : run.text
    endedByCR = false
    let start = 0
    for (const terminator of text.matchAll(LINE_TERMINATOR)) {
      if (terminator.index > start)
        lines.at(-1)!.push({ ...run, text: text.slice(start, terminator.index) })
      lines.push([])
      start = terminator.index + terminator[0].length
      endedByCR = terminator[0] === '\r' && start === text.length
    }
    if (start < text.length) lines.at(-1)!.push({ ...run, text: text.slice(start) })
  }
  return lines
}

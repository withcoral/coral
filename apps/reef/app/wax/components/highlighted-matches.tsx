import classNames from 'classnames'
import type { ReactNode } from 'react'

import type { FilterQuery } from '@/utils/filter-query'

import * as styles from './highlighted-matches.css'

/** A found part of a text: its range, and whether it belongs to the current match. */
export type Highlight = { range: [start: number, end: number]; current?: boolean }

/**
 * `text` with each part that a filter query matched in bold. A find passes the parts it
 * found as `highlights` instead, in order: each is tinted as well, and the current match's
 * parts are outlined and carry `data-current-match`, so the find can scroll to them.
 */
export function HighlightedMatches({
  text,
  ...matches
}: { text: string } & ({ query: FilterQuery } | { highlights: Highlight[] })) {
  const found = 'highlights' in matches
  const parts: ReactNode[] = []
  let from = 0
  const ranges = found
    ? matches.highlights
    : matches.query.ranges(text).map((range): Highlight => ({ range }))
  for (const { range, current } of ranges) {
    const [start, end] = range
    if (start > from) parts.push(text.slice(from, start))
    parts.push(
      found ? (
        <mark
          key={start}
          className={classNames(styles.match, styles.found, current && styles.current)}
          data-current-match={current ? '' : undefined}
        >
          {text.slice(start, end)}
        </mark>
      ) : (
        <strong key={start} className={styles.match}>
          {text.slice(start, end)}
        </strong>
      ),
    )
    from = end
  }
  if (from < text.length) parts.push(text.slice(from))
  return parts
}

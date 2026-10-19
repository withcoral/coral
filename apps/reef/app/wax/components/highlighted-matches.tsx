import type { ReactNode } from 'react'

import type { FilterQuery } from '@/utils/filter-query'

import * as styles from './highlighted-matches.css'

/** `text` with each part that a filter query matched in bold. */
export function HighlightedMatches({ text, query }: { text: string; query: FilterQuery }) {
  const parts: ReactNode[] = []
  let from = 0
  for (const [start, end] of query.ranges(text)) {
    if (start > from) parts.push(text.slice(from, start))
    parts.push(
      <strong key={start} className={styles.match}>
        {text.slice(start, end)}
      </strong>,
    )
    from = end
  }
  if (from < text.length) parts.push(text.slice(from))
  return parts
}

import { useMemo } from 'react'

import { countLabel } from '@/utils/count-label'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'

import { diffLines, foldUnchanged } from './diff-lines'
import * as styles from './diff-view.css'

const markers = { same: ' ', removed: '−', added: '+' } as const
const spoken = { removed: 'Removed: ', added: 'Added: ' } as const

/**
 * What changed between two texts, line by line: each removed line, then each
 * added one, with `context` unchanged lines around them and the rest folded.
 * A sign marks each changed line, so its colour is not the only cue. Lines do
 * not wrap; the diff scrolls sideways. With `maxLines` it grows to that many
 * lines, then scrolls inside itself. Two equal texts show `unchanged`.
 */
export function DiffView({
  before,
  after,
  label,
  context = 3,
  maxLines,
  unchanged = 'No changes.',
}: {
  before: string
  after: string
  /** The accessible name of the diff, such as "Source changes". */
  label: string
  context?: number
  maxLines?: number
  unchanged?: string
}) {
  const rows = useMemo(() => {
    const lines = diffLines(before, after)
    return lines.every((line) => line.kind === 'same') ? null : foldUnchanged(lines, context)
  }, [before, after, context])
  if (!rows)
    return (
      <Typography.BodySmall as="p" variant="secondary">
        {unchanged}
      </Typography.BodySmall>
    )
  return (
    <ScrollArea
      className={styles.frame}
      maxHeight={
        maxLines === undefined ? 'none' : `calc(${maxLines}lh + ${styles.FRAME_PADDING * 2}px)`
      }
      fade={maxLines === undefined ? 'none' : 'bottom'}
      horizontal
    >
      <div role="group" aria-label={label} className={styles.rows}>
        {rows.map((row, index) =>
          'hidden' in row ? (
            <div key={index} className={styles.fold}>
              {countLabel(row.hidden, 'unchanged line')}
            </div>
          ) : (
            <div key={index} className={styles.line[row.kind]} data-change={row.kind}>
              <span className={styles.number} aria-hidden>
                {row.before}
              </span>
              <span className={styles.number} aria-hidden>
                {row.after}
              </span>
              <span className={styles.marker[row.kind]} aria-hidden>
                {markers[row.kind]}
              </span>
              <code className={styles.text}>
                {row.kind !== 'same' && <span className={visuallyHidden}>{spoken[row.kind]}</span>}
                {row.text}
              </code>
            </div>
          ),
        )}
      </div>
    </ScrollArea>
  )
}

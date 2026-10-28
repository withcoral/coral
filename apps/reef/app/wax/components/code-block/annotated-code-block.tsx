import classNames from 'classnames'
import { useEffect, useMemo, useRef } from 'react'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Typography } from '@/wax/components/typography'
import { highlight as highlightSource, type Language } from './highlight'
import { splitLines } from './lines'
import * as styles from './annotated-code-block.css'

export interface AnnotatedCodeHighlight {
  startLine: number
  endLine: number
}

/**
 * Read-only source with line numbers, syntax highlighting, and an optional
 * highlighted line range. It keeps its scroll position across a re-render;
 * only a change to `revealKey` attempts a new reveal, and the reveal itself
 * moves the viewport only when the range is not already fully visible, so a
 * caller can select the same range again without re-scrolling a reader's
 * manual position.
 */
export function AnnotatedCodeBlock({
  code,
  language,
  highlight: range,
  followHighlight,
  revealKey,
}: {
  code: string
  language: Language
  highlight: AnnotatedCodeHighlight | null
  /** Auto-scroll the highlighted range into view when `revealKey` changes. */
  followHighlight: boolean
  revealKey: string
}) {
  const lines = useMemo(() => splitLines(highlightSource(code, language)), [code, language])
  const viewportRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (!followHighlight || !range) return
    const viewport = viewportRef.current
    const start = viewport?.querySelector<HTMLElement>(`[data-line="${range.startLine}"]`)
    const end = viewport?.querySelector<HTMLElement>(`[data-line="${range.endLine}"]`)
    if (!viewport || !start || !end) return
    const bounds = viewport.getBoundingClientRect()
    const startBounds = start.getBoundingClientRect()
    const endBounds = end.getBoundingClientRect()
    const alreadyVisible = startBounds.top >= bounds.top && endBounds.bottom <= bounds.bottom
    if (alreadyVisible) return
    const padding = 24
    viewport.scrollTo({
      top: Math.max(0, viewport.scrollTop + (startBounds.top - bounds.top) - padding),
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [revealKey, followHighlight])
  return (
    <ScrollArea viewportRef={viewportRef} viewportClassName={styles.viewport} fade="both">
      <Typography.Code as="pre" className={styles.code}>
        {lines.map((runs, index) => {
          const lineNumber = index + 1
          const inRange = !!range && lineNumber >= range.startLine && lineNumber <= range.endLine
          return (
            <div
              key={lineNumber}
              data-line={lineNumber}
              data-highlighted={inRange || undefined}
              className={classNames(styles.line, inRange && styles.lineHighlighted)}
            >
              <span className={styles.gutter} aria-hidden>
                {lineNumber}
              </span>
              <span className={styles.lineText}>
                {runs.map(({ text, className }, i) =>
                  className ? (
                    <span key={i} className={className}>
                      {text}
                    </span>
                  ) : (
                    text
                  ),
                )}
              </span>
            </div>
          )
        })}
      </Typography.Code>
    </ScrollArea>
  )
}

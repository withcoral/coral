import classNames from 'classnames'
import { Fragment, useEffect, useMemo, useRef } from 'react'
import { HighlightedMatches, type Highlight } from '@/wax/components/highlighted-matches'
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
 * manual position. A Find passes its `marks`: they show inside the syntax colouring,
 * and the current match scrolls into view whenever it changes.
 */
export function AnnotatedCodeBlock({
  code,
  language,
  highlight: range,
  followHighlight,
  revealKey,
  marks,
}: {
  code: string
  language: Language
  highlight: AnnotatedCodeHighlight | null
  /** Auto-scroll the highlighted range into view when `revealKey` changes. */
  followHighlight: boolean
  revealKey: string
  /** By line number, the parts of its text a Find found, in order. Each new map reveals
   * its current match, so keep it the same between renders until Find moves. */
  marks?: ReadonlyMap<number, Highlight[]>
}) {
  const lines = useMemo(() => splitLines(highlightSource(code, language)), [code, language])
  // Where each run starts in its line, so a mark finds the runs it falls in.
  const offsets = useMemo(
    () =>
      lines.map((runs) => {
        let at = 0
        return runs.map(({ text }) => (at += text.length) - text.length)
      }),
    [lines],
  )
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
  // The current match, once it has rendered.
  useEffect(() => {
    viewportRef.current
      ?.querySelector('[aria-current="true"]')
      ?.scrollIntoView?.({ block: 'nearest' })
  }, [marks])
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
                {runs.map(({ text, className }, i) => {
                  const shown = markedRun(text, offsets[index]![i]!, marks?.get(lineNumber))
                  return className ? (
                    <span key={i} className={className}>
                      {shown}
                    </span>
                  ) : (
                    <Fragment key={i}>{shown}</Fragment>
                  )
                })}
              </span>
            </div>
          )
        })}
      </Typography.Code>
    </ScrollArea>
  )
}

/** A run's `text`, from `start` in its line, with the parts of `marks` that fall in it. */
function markedRun(text: string, start: number, marks: Highlight[] | undefined) {
  if (!marks?.length) return text
  const inRun = marks.flatMap(({ range: [from, to], current }): Highlight[] => {
    const range: Highlight['range'] = [
      Math.max(from, start) - start,
      Math.min(to, start + text.length) - start,
    ]
    return range[0] < range[1] ? [{ range, current }] : []
  })
  return inRun.length ? <HighlightedMatches text={text} highlights={inRun} /> : text
}

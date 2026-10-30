import classNames from 'classnames'
import { Fragment, useEffect, useMemo, useRef, type ReactNode } from 'react'
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
  // By line, each run's text with the parts of the marks that fall in it.
  const shown = useMemo(
    () => lines.map((runs, index) => markedRuns(runs, marks?.get(index + 1))),
    [lines, marks],
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
                {runs.map(({ className }, i) =>
                  className ? (
                    <span key={i} className={className}>
                      {shown[index]![i]}
                    </span>
                  ) : (
                    <Fragment key={i}>{shown[index]![i]}</Fragment>
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

/**
 * Each run's text with the parts of `marks` that fall in it. Find's marks come in order
 * and never overlap, so one pass over the runs visits each mark only where it falls.
 */
function markedRuns(runs: { text: string }[], marks: Highlight[] | undefined): ReactNode[] {
  if (!marks?.length) return runs.map(({ text }) => text)
  let next = 0
  let start = 0
  return runs.map(({ text }, index) => {
    const end = start + text.length
    // A mark that ended before this run is behind every later run too.
    while (next < marks.length && marks[next]!.range[1] <= start) next++
    const inRun: Highlight[] = []
    for (let at = next; at < marks.length && marks[at]!.range[0] < end; at++) {
      const {
        range: [from, to],
        current,
      } = marks[at]!
      if (from < to)
        inRun.push({ range: [Math.max(from, start) - start, Math.min(to, end) - start], current })
    }
    start = end
    return inRun.length ? <HighlightedMatches key={index} text={text} highlights={inRun} /> : text
  })
}

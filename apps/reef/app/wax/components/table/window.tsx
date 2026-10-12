import { Fragment, useContext, useId, useLayoutEffect, useRef, type ReactNode } from 'react'
import { fallbackVar } from '@/wax/css'
import { ReadingContext } from './reading-position'
import * as styles from './table.css'

export interface WindowViewport {
  offset: number
  anchor: string
  preceding: number
}
export interface WindowProps {
  label: string
  /** Total logical rows, not just the materialized window. */
  total: number
  items: { key: string; index: number; content: ReactNode }[]
  /** Bounded query capacity; the caller loads at most this many ordinary rows. */
  size: number
  onViewportChange?: (viewport: WindowViewport) => void
}

/** Sparse sections share their table's one measurement and reading controller. */
export function Window(props: WindowProps) {
  const { label, total, items } = props
  const section = useRef<HTMLDivElement>(null)
  const reading = useContext(ReadingContext)
  const id = useId()
  useLayoutEffect(() => reading?.registerWindow(section.current!, props))
  const ordered = items.toSorted((a, b) => a.index - b.index)
  const end = (ordered.at(-1)?.index ?? -1) + 1
  const gap = (from: number, count: number) =>
    count > 0 && (
      <div
        key={`gap-${from}`}
        data-window-from={from}
        data-window-count={count}
        aria-hidden="true"
        className={styles.windowGap}
        style={{
          height: `calc(${count} * ${fallbackVar(styles.windowRowHeight, `${Math.min(40, 500_000 / Math.max(1, total))}px`)})`,
        }}
      />
    )
  return (
    <div ref={section} data-window={id} className={styles.group} role="rowgroup" aria-label={label}>
      {ordered.map((item, index) => (
        <Fragment key={item.key}>
          {gap(
            index ? ordered[index - 1]!.index + 1 : 0,
            item.index - (index ? ordered[index - 1]!.index + 1 : 0),
          )}
          <div className={styles.group} data-window-index={item.index} data-window-key={item.key}>
            {item.content}
          </div>
        </Fragment>
      ))}
      {gap(end, total - end)}
    </div>
  )
}

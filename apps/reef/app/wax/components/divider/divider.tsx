import classNames from 'classnames'
import { useRef } from 'react'
import { Icon } from '@/wax/components/icon'
import * as styles from './divider.css'

/**
 * A horizontal split divider: drag with the pointer or press ↑/↓ (Shift for a
 * bigger step) while focused. It only reports a pixel delta; the caller owns
 * the split size and its clamping, so a resize never remounts either pane.
 */
export function Divider({
  className,
  label,
  onResize,
  percent,
}: {
  className?: string
  label: string
  onResize: (deltaY: number) => void
  /** The pane above the divider's share of the split, for the separator's ARIA value. */
  percent: number
}) {
  const dragStartY = useRef<number | null>(null)
  return (
    <div
      role="separator"
      aria-orientation="horizontal"
      aria-label={label}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(percent)}
      tabIndex={0}
      className={classNames(styles.divider, className)}
      onPointerDown={(event) => {
        event.currentTarget.setPointerCapture(event.pointerId)
        dragStartY.current = event.clientY
      }}
      onPointerMove={(event) => {
        if (dragStartY.current === null) return
        const deltaY = event.clientY - dragStartY.current
        dragStartY.current = event.clientY
        if (deltaY) onResize(deltaY)
      }}
      onPointerUp={(event) => {
        dragStartY.current = null
        event.currentTarget.releasePointerCapture(event.pointerId)
      }}
      onPointerCancel={() => {
        dragStartY.current = null
      }}
      onKeyDown={(event) => {
        const step = event.shiftKey ? 48 : 16
        if (event.key === 'ArrowUp') {
          event.preventDefault()
          onResize(-step)
        } else if (event.key === 'ArrowDown') {
          event.preventDefault()
          onResize(step)
        }
      }}
    >
      <span className={styles.grip} aria-hidden>
        <Icon name="GripHorizontal" size="14" />
      </span>
    </div>
  )
}

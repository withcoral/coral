import { useContext, type ReactNode } from 'react'
import * as Button from '@/wax/components/button'
import { Icon, type IconName } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import * as styles from './record.css'
import { RowStatusContext } from './row'

// A colored edge alone does not tell an error from a warning to a color-blind reader.
const statusIcons = { error: 'CircleX', warning: 'CircleAlert', success: 'CircleCheck' } as const

/** A summary label with hierarchy controls. Exact evidence never belongs in this cell. */
export function Record({
  depth = 0,
  expanded,
  onToggle,
  disabled = false,
  children,
  marker,
  metadata,
  icon,
  tabbable = true,
  everyStatus = false,
  stacked = false,
  wrap = false,
}: {
  depth?: number
  expanded?: boolean
  onToggle?: () => void
  disabled?: boolean
  children: ReactNode
  marker?: string
  /** Short, secondary attribution kept inline with the truncated record label. */
  metadata?: ReactNode
  /** Accessible record kind without a repeated text label. */
  icon?: { name: IconName; label: string }
  /**
   * False takes the toggle out of the Tab order, for a table that is one Tab stop
   * itself and toggles from its own keys. A click still toggles.
   */
  tabbable?: boolean
  /**
   * Repeat a success as an icon too, not only an error or a warning. Use it where each row is
   * one outcome to scan, such as a list of runs; on a dense tree it adds a mark to every row.
   */
  everyStatus?: boolean
  /** Put the metadata on its own line under the label, for a list to scan by name. */
  stacked?: boolean
  /** Wrap the label to two lines instead of cutting it, for names that state an intent. */
  wrap?: boolean
}) {
  const status = useContext(RowStatusContext)
  const shape =
    status?.tone === 'error' ||
    status?.tone === 'warning' ||
    (everyStatus && status?.tone === 'success')
      ? status.tone
      : undefined
  return (
    <div className={styles.record} style={{ paddingInlineStart: depth * 20 }}>
      <span className={styles.control}>
        {onToggle && (
          <Button.Container
            variant="bare"
            size="22"
            ariaLabel={expanded ? 'Collapse record' : 'Expand record'}
            aria-expanded={expanded}
            disabled={disabled}
            tabIndex={tabbable ? undefined : -1}
            onClick={onToggle}
          >
            <Button.Icon name={expanded ? 'ChevronDown' : 'ChevronRight'} />
          </Button.Container>
        )}
      </span>
      {icon && (
        <Pill color="graySubtle" role="img" aria-label={icon.label} title={icon.label}>
          <Icon name={icon.name} size="14" />
        </Pill>
      )}
      {shape && (
        // The row already describes its status to assistive technology.
        <span className={styles.status} aria-hidden>
          <Icon name={statusIcons[shape]} size="14" color={shape} />
        </span>
      )}
      <div className={stacked ? styles.stackedContent : styles.content}>
        <div className={wrap ? styles.wrappedLabel : stacked ? styles.stackedLabel : styles.label}>
          {children}
        </div>
        {marker && <span className={styles.marker}>{marker}</span>}
        {metadata && <div className={styles.metadata}>{metadata}</div>}
      </div>
    </div>
  )
}

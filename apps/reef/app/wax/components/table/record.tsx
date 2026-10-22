import type { ReactNode } from 'react'
import * as Button from '@/wax/components/button'
import { Icon, type IconName } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import * as styles from './record.css'

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
}) {
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
      <div className={styles.content}>
        <div className={styles.label}>{children}</div>
        {marker && <span className={styles.marker}>{marker}</span>}
        {metadata && <div className={styles.metadata}>{metadata}</div>}
      </div>
    </div>
  )
}

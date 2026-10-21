import classNames from 'classnames'
import * as styles from './meter.css'

export interface MeterProps {
  value: number
  max: number
  size?: 'medium' | 'small'
  className?: string
}

/** A bar filled to `value / max`. It only draws the share, so put the number in text beside it. */
export function Meter({ value, max, size = 'medium', className }: MeterProps) {
  return (
    <span className={classNames(styles.size[size], className)} aria-hidden>
      {value > 0 && max > 0 && (
        <span className={styles.fill} style={{ width: `${Math.min(value / max, 1) * 100}%` }} />
      )}
    </span>
  )
}

import classNames from 'classnames'
import { useId } from 'react'
import { Icon, type IconName } from '../icon'
import { NumberPopIn } from '../number-pop-in'
import { Typography } from '../typography'
import { visuallyHidden } from '../visually-hidden.css'
import * as styles from './stat.css'

export interface StatProps {
  label: string
  icon: IconName
  value: string
  /** Spoken in place of the value when the value alone is ambiguous. */
  valueLabel?: string
  detail?: string
  variant?: 'success' | 'error' | 'tertiary'
  /** A medium stat stands alone, as on a tile. A small one sits in a row of stats. */
  size?: 'medium' | 'small'
  className?: string
}

/** One number with its label, and an optional line under it. It reads as a group named by its label. */
export function Stat({
  label,
  icon,
  value,
  valueLabel,
  detail,
  variant,
  size = 'medium',
  className,
}: StatProps) {
  const Value = size === 'medium' ? Typography.HeadingMedium : Typography.HeadingSmall
  const labelId = useId()
  return (
    <div role="group" aria-labelledby={labelId} className={classNames(styles.stat, className)}>
      <span className={styles.label}>
        <Icon name={icon} size={size === 'medium' ? '16' : '14'} color="tertiary" />
        <Typography.BodySmall as="span" variant="tertiary" id={labelId}>
          {label}
        </Typography.BodySmall>
      </span>
      <Value as="span" className={styles.value} variant={variant}>
        <span aria-hidden={valueLabel ? true : undefined}>
          <NumberPopIn value={value} />
        </span>
        {valueLabel && <span className={visuallyHidden}>{valueLabel}</span>}
      </Value>
      {detail && (
        <Typography.BodySmall as="span" variant="tertiary">
          {detail}
        </Typography.BodySmall>
      )}
    </div>
  )
}

import classNames from 'classnames'

import * as styles from './combobox.css'

export interface ChipLabelProps extends React.HTMLAttributes<HTMLSpanElement> {
  children: React.ReactNode
  className?: string
  /** The label clips its text, so a truncation tooltip measures this element. */
  ref?: React.Ref<HTMLSpanElement>
}

export function ChipLabel({ children, className, ...props }: ChipLabelProps) {
  return (
    <span {...props} className={classNames(styles.chipLabel, className)}>
      {children}
    </span>
  )
}

import classNames from 'classnames'

import * as styles from './combobox.css'

export interface ChipLabelProps {
  children: React.ReactNode
  className?: string
}

export function ChipLabel({ children, className }: ChipLabelProps) {
  return <span className={classNames(styles.chipLabel, className)}>{children}</span>
}

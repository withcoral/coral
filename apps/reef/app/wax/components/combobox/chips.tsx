import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'

import * as styles from './combobox.css'

export interface ChipsProps {
  children: React.ReactNode
  className?: string
}

export function Chips({ children, className }: ChipsProps) {
  return (
    <BaseCombobox.Chips className={classNames(styles.chips, className)}>
      {children}
    </BaseCombobox.Chips>
  )
}

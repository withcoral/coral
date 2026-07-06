import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'

import * as styles from './combobox.css'

export interface ChipProps {
  children: React.ReactNode
  className?: string
}

export function Chip({ children, className }: ChipProps) {
  return (
    <BaseCombobox.Chip className={classNames(styles.chip, className)}>{children}</BaseCombobox.Chip>
  )
}

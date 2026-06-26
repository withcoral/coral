import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'

import { Icon } from '@/wax/components/icon'

import * as styles from './combobox.css'

export interface ChipRemoveProps {
  'aria-label'?: string
  children?: React.ReactNode
  className?: string
}

export function ChipRemove({ 'aria-label': ariaLabel, children, className }: ChipRemoveProps) {
  return (
    <BaseCombobox.ChipRemove
      aria-label={ariaLabel}
      className={classNames(styles.chipRemove, className)}
    >
      {children ?? <Icon color="inherit" name="X" size="14" />}
    </BaseCombobox.ChipRemove>
  )
}

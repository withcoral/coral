import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'

import { Icon } from '@/wax/components/icon'

import * as styles from './combobox.css'

export interface ItemProps {
  children: React.ReactNode
  className?: string
  disabled?: boolean
  value: string
}

export function Item({ children, className, disabled, value }: ItemProps) {
  return (
    <BaseCombobox.Item
      className={classNames(styles.item, className)}
      disabled={disabled}
      value={value}
    >
      <span className={styles.itemLabel}>{children}</span>
      <BaseCombobox.ItemIndicator className={styles.itemIndicator}>
        <Icon color="secondary" name="Check" size="16" />
      </BaseCombobox.ItemIndicator>
    </BaseCombobox.Item>
  )
}

import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'

import * as styles from './combobox.css'

export interface ListProps<Item = string> {
  children: ((item: Item, index: number) => React.ReactNode) | React.ReactNode
  className?: string
}

export function List<Item = string>({ children, className }: ListProps<Item>) {
  return (
    <BaseCombobox.List className={classNames(styles.list, className)}>{children}</BaseCombobox.List>
  )
}

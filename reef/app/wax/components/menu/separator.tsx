import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'

import * as styles from './menu.css'

export interface SeparatorProps {
  className?: string
  fullWidth?: boolean
}

export function Separator({ className, fullWidth }: SeparatorProps) {
  return (
    <BaseMenu.Separator
      className={classNames(styles.separator, className, {
        [styles.separatorFullWidth]: fullWidth,
      })}
    />
  )
}

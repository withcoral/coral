import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'

import { Typography } from '@/wax/components/typography'

import * as styles from './menu.css'

export const Group = BaseMenu.Group

export interface GroupLabelProps {
  children: React.ReactNode
  className?: string
}

export function GroupLabel({ children, className }: GroupLabelProps) {
  return (
    <BaseMenu.GroupLabel
      className={classNames(styles.groupLabel, className)}
      render={Typography.BodySmall}
    >
      {children}
    </BaseMenu.GroupLabel>
  )
}

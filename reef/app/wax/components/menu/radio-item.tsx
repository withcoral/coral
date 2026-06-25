import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'

import { Icon } from '@/wax/components/icon'
import type { IconColor, IconName } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'

import * as styles from './menu.css'

export interface RadioItemProps {
  children: React.ReactNode
  className?: string
  closeOnClick?: boolean
  disabled?: boolean
  iconColor?: IconColor
  iconName?: IconName
  value: string
}

export function RadioItem({
  children,
  className,
  closeOnClick = true,
  disabled = false,
  iconColor = 'tertiary',
  iconName,
  value,
}: RadioItemProps) {
  return (
    <BaseMenu.RadioItem
      className={classNames(styles.item, className)}
      closeOnClick={closeOnClick}
      disabled={disabled}
      value={value}
    >
      {iconName && <Icon color={iconColor} name={iconName} size="18" />}
      <div className={styles.itemContent}>
        <Tooltip content={children} showOnlyWhenTruncated>
          <span className={styles.itemLabel}>{children}</span>
        </Tooltip>
      </div>
      <BaseMenu.RadioItemIndicator className={styles.radioIndicator}>
        <Icon color="secondary" name="Check" size="16" />
      </BaseMenu.RadioItemIndicator>
    </BaseMenu.RadioItem>
  )
}

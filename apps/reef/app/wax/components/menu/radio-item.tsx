import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'
import React, { type ElementType } from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconColor, IconName } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'

import * as styles from './menu.css'

interface RadioItemBaseProps {
  children: React.ReactNode
  className?: string
  closeOnClick?: boolean
  disabled?: boolean
  iconColor?: IconColor
  iconName?: IconName
  value: string
}

export type RadioItemProps<T extends ElementType = 'div'> = RadioItemBaseProps &
  Omit<React.ComponentPropsWithoutRef<T>, 'as' | keyof RadioItemBaseProps> & {
    as?: T
  }

export function RadioItem<T extends ElementType = 'div'>(props: RadioItemProps<T>) {
  const {
    as,
    children,
    className,
    closeOnClick = true,
    disabled = false,
    iconColor = 'tertiary',
    iconName,
    value,
    ...rest
  } = props
  const render = as ? React.createElement(as, rest) : undefined
  const content = (
    <>
      {iconName && <Icon color={iconColor} name={iconName} size="18" />}
      <div className={styles.itemContent}>
        <Tooltip content={children} showOnlyWhenTruncated>
          <span className={styles.itemLabel}>{children}</span>
        </Tooltip>
      </div>
      <BaseMenu.RadioItemIndicator className={styles.radioIndicator}>
        <Icon color="secondary" name="Check" size="16" />
      </BaseMenu.RadioItemIndicator>
    </>
  )

  return (
    <BaseMenu.RadioItem
      className={classNames(styles.item, className)}
      closeOnClick={closeOnClick}
      disabled={disabled}
      render={render}
      value={value}
    >
      {content}
    </BaseMenu.RadioItem>
  )
}

import classNames from 'classnames'
import React, { ElementType } from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'

import {
  activeClass,
  disabledClass,
  iconStyles,
  sidebarButton,
  textStyles,
} from './sidebar-button.css'

export interface SidebarButtonProps<T extends ElementType = 'button'> {
  as?: T
  children: React.ReactNode
  className?: string
  disabled?: boolean
  icon: IconName
  isActive?: boolean
  isMinimized?: boolean
  variant?: SidebarButtonVariant
}

export type SidebarButtonVariant = 'accent' | 'default'

type PolymorphicProps<T extends ElementType> = Omit<
  React.ComponentPropsWithoutRef<T>,
  keyof SidebarButtonProps<T>
> &
  SidebarButtonProps<T>

export function SidebarButton<T extends ElementType = 'button'>(
  props: PolymorphicProps<T> & { ref?: React.Ref<HTMLElement> },
) {
  const {
    as,
    children,
    className,
    disabled = false,
    icon,
    isActive = false,
    isMinimized = false,
    ref,
    variant = 'default',
    ...rest
  } = props

  const Component = (as ?? 'button') as ElementType
  const type = 'type' in props ? props.type! : 'button'

  const componentProps = {
    className: classNames(
      sidebarButton({ disabled, isActive, isMinimized, variant }),
      { [activeClass]: isActive, [disabledClass]: disabled },
      className,
    ),
    ref,
    ...rest,
    // Only add button-specific props when rendering as button
    ...(Component === 'button' && { disabled, type }),
    // For non-button elements, add aria-disabled and tabIndex for accessibility
    ...(Component !== 'button' &&
      disabled && { 'aria-disabled': true, href: undefined, tabIndex: -1 }),
  }

  return (
    <Component {...componentProps}>
      <Icon className={iconStyles({ variant })} color="inherit" name={icon} size="18" />
      {children && <span className={textStyles({ variant })}>{children}</span>}
    </Component>
  )
}

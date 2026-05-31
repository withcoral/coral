import classNames from 'classnames'
import React from 'react'
import type { ElementType, MouseEvent } from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'

import {
  activeClass,
  disabledClass,
  iconStyles,
  sidebarButton,
  textStyles,
} from './sidebar-button.css'

function handleDisabledClick(event: MouseEvent<HTMLElement>) {
  event.preventDefault()
  event.stopPropagation()
}

function resolveAriaLabel(
  children: React.ReactNode,
  explicitLabel: string | undefined,
  isMinimized: boolean,
) {
  if (explicitLabel !== undefined) return explicitLabel
  if (!isMinimized || typeof children !== 'string') return undefined

  return children
}

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
    'aria-label': ariaLabel,
    ...rest
  } = props

  const Component = (as ?? 'button') as ElementType
  const isNativeButton = Component === 'button'
  const type = 'type' in props ? props.type! : 'button'
  const resolvedAriaLabel = resolveAriaLabel(children, ariaLabel, isMinimized)

  const componentProps = {
    className: classNames(
      sidebarButton({ disabled, isActive, isMinimized, variant }),
      { [activeClass]: isActive, [disabledClass]: disabled },
      className,
    ),
    ref,
    ...rest,
    ...(resolvedAriaLabel !== undefined && { 'aria-label': resolvedAriaLabel }),
    onClick: !isNativeButton && disabled ? handleDisabledClick : rest.onClick,
    ...(isNativeButton && { disabled, type }),
    ...(!isNativeButton && disabled && { 'aria-disabled': true, href: undefined, tabIndex: -1 }),
  }

  return (
    <Component {...componentProps}>
      <Icon className={iconStyles({ variant })} color="inherit" name={icon} size="18" />
      {!isMinimized && children && <span className={textStyles({ variant })}>{children}</span>}
    </Component>
  )
}

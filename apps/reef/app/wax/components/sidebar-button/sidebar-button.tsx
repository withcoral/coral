import classNames from 'classnames'
import React, { ElementType, useState } from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'

import {
  activeClass,
  badgeEnter,
  badgeExit,
  badgeStyles,
  disabledClass,
  iconStyles,
  sidebarButton,
  textStyles,
} from './sidebar-button.css'

export interface SidebarButtonProps<T extends ElementType = 'button'> {
  as?: T
  /** Supplementary status; include its meaning in the link's accessible name. */
  badge?: string
  badgeTone?: BadgeTone
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
    badge,
    badgeTone = 'attention',
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
      {!isMinimized && children && <span className={textStyles({ variant })}>{children}</span>}
      <SidebarBadge badge={badge} tone={badgeTone} isMinimized={isMinimized} />
    </Component>
  )
}

type BadgeTone = 'attention' | 'neutral'

/** A status badge for sidebar controls. Minimized, it is a dot in the positioned
 * parent's top corner. Include its meaning in the control's accessible name.
 * Render it whether or not there is a badge: one that arrives after the first
 * render pops in, and one that leaves keeps its last value while it pops out. */
export function SidebarBadge({
  badge,
  tone = 'attention',
  isMinimized = false,
}: {
  badge?: string
  tone?: BadgeTone
  isMinimized?: boolean
}) {
  const open = Boolean(badge)
  const [shown, setShown] = useState<{
    last?: { badge: string; tone: BadgeTone }
    open: boolean
    arrived: boolean
  }>({ last: badge ? { badge, tone } : undefined, open, arrived: false })
  if (open !== shown.open || (badge && (badge !== shown.last?.badge || tone !== shown.last.tone)))
    setShown({
      last: badge ? { badge, tone } : shown.last,
      open,
      arrived: open ? shown.arrived || !shown.open : shown.arrived,
    })
  if (!shown.last) return null
  return (
    <span
      aria-hidden="true"
      className={classNames(
        badgeStyles({ tone: shown.last.tone, isMinimized }),
        shown.open ? shown.arrived && badgeEnter : badgeExit,
      )}
      onAnimationEnd={() =>
        setShown((current) => (current.open ? current : { ...current, last: undefined }))
      }
    >
      {!isMinimized && shown.last.badge}
    </span>
  )
}

import React from 'react'
import { Link } from 'react-router'
import { To } from 'react-router'

import { Button } from '@/wax/components'
import type { ButtonVariant } from './button.css'
import type { IconName } from '@/wax/components/icon'

interface ButtonLinkProps {
  children: React.ReactNode
  className?: string
  disabled?: boolean
  /** A leading icon, for links that read as a direction such as Back. */
  iconName?: IconName
  size?: 'default' | 'small' | 'x-small'
  to: To
  preventScrollReset?: boolean
  wrap?: boolean
  variant?: ButtonVariant
  'aria-current'?: React.AriaAttributes['aria-current']
  'aria-label'?: string
}

/**
 * A styled link component using reactrouter Link for client side navigation.
 */
export function InternalLink(props: ButtonLinkProps) {
  const {
    children,
    className,
    disabled,
    iconName,
    size,
    to,
    preventScrollReset,
    variant = 'linkSubtle',
    wrap = false,
    'aria-current': ariaCurrent,
    'aria-label': ariaLabel,
  } = props

  const buttonSize = (() => {
    switch (size) {
      case 'small':
        return '32'
      case 'x-small':
        return '22'
      default:
        return '36'
    }
  })()

  return (
    <Button.Container
      as={Link}
      className={className}
      disabled={disabled}
      onClick={disabled ? (e) => e.preventDefault() : undefined}
      size={buttonSize}
      to={to}
      preventScrollReset={preventScrollReset}
      variant={variant}
      aria-current={ariaCurrent}
      ariaLabel={ariaLabel}
    >
      {iconName && <Button.Icon name={iconName} />}
      <Button.Text wrap={wrap}>{children}</Button.Text>
    </Button.Container>
  )
}

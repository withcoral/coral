import React from 'react'
import { Link } from 'react-router'
import { To } from 'react-router'

import { Button } from '@/wax/components'

interface ButtonLinkProps {
  children: React.ReactNode
  className?: string
  disabled?: boolean
  size?: 'default' | 'small' | 'x-small'
  to: To
}

/**
 * A styled link component using reactrouter Link for client side navigation.
 */
export function InternalLink(props: ButtonLinkProps) {
  const { children, className, disabled, size, to } = props

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
      variant="linkSubtle"
    >
      <Button.Text>{children}</Button.Text>
    </Button.Container>
  )
}

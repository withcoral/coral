import React from 'react'

import { Button } from '..'
import type { ButtonSize, ButtonVariant } from './button.css'
import { iconContainer } from './icon.css'
import { Icon, IconSize } from '../icon'

interface ExternalLinkProps {
  children: React.ReactNode
  className?: string
  disabled?: boolean
  href: string
  size?: ExternalLinkSize
  /** `secondary` looks like a secondary button, for a link that is an action. */
  variant?: 'primary' | 'subtle' | 'secondary'
}

type ExternalLinkSize = 'default' | 'large' | 'small'

/**
 * A styled external link component with ExternalLink icon.
 * Always opens in a new tab with rel="noopener noreferrer".
 */
export function ExternalLink({
  children,
  className,
  disabled,
  href,
  size = 'default',
  variant = 'subtle',
}: ExternalLinkProps) {
  const buttonVariant: ButtonVariant =
    variant === 'subtle' ? 'linkSubtle' : variant === 'secondary' ? 'secondary' : 'link'

  return (
    <Button.Container
      as="a"
      className={className}
      disabled={disabled}
      href={href}
      rel="noopener noreferrer"
      size={getButtonSize(size)}
      target="_blank"
      variant={buttonVariant}
    >
      <Button.Text>{children}</Button.Text>
      <Icon
        className={iconContainer({ buttonVariant, size: getButtonSize(size) })}
        color="inherit"
        name="ExternalLink"
        size={getIconSize(size)}
      />
    </Button.Container>
  )
}

function getButtonSize(size: ExternalLinkSize): ButtonSize {
  switch (size) {
    case 'default':
      return '32'

    case 'large':
      return '36'

    case 'small':
      return '22'
  }
}

function getIconSize(size: ExternalLinkSize): IconSize {
  switch (size) {
    case 'default':
      return '14'

    case 'large':
      return '16'

    case 'small':
      return '14'
  }
}

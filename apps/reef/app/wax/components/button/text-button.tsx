import React from 'react'

import { visuallyHidden } from '@/wax/components/visually-hidden.css'

import { ButtonProps, Container } from './container'
import type { IconName } from '@/wax/components/icon'

import { Icon, SpinningButtonIcon } from './icon'
import { Text } from './text'

export interface TextButtonProps extends Omit<ButtonProps, 'children'> {
  /** Shows a spinner beside the stable label and ignores clicks until the work ends. */
  busy?: boolean
  children: React.ReactNode
  /** An icon before the label, such as one whose shape shows a toggle's state. */
  icon?: IconName
  ref?: React.Ref<HTMLButtonElement>
}

/**
 * Convenience component for a button containing only text.
 */
export function TextButton({
  busy,
  children,
  disabled,
  icon,
  onClick,
  ref,
  ...rest
}: TextButtonProps) {
  return (
    <>
      <Container
        ref={ref}
        {...rest}
        aria-busy={busy ? true : rest['aria-busy']}
        disabled={disabled || busy}
        focusableWhenDisabled={busy || rest.focusableWhenDisabled}
        onClick={busy ? undefined : onClick}
      >
        {busy ? <SpinningButtonIcon name="Loader" /> : icon && <Icon name={icon} />}
        <Text>{children}</Text>
      </Container>
      {/* A screen reader announces a live region's changes, not its first
          content, so a button that can be busy keeps its region mounted. */}
      {busy !== undefined && (
        <span className={visuallyHidden} role="status">
          {busy ? 'Loading' : ''}
        </span>
      )}
    </>
  )
}

import React from 'react'

import { visuallyHidden } from '@/wax/components/visually-hidden.css'

import { ButtonProps, Container } from './container'
import { SpinningButtonIcon } from './icon'
import { Text } from './text'

export interface TextButtonProps extends Omit<ButtonProps, 'children'> {
  /** Keeps the label stable while announcing this progress status. */
  busyStatus?: string
  children: React.ReactNode
  ref?: React.Ref<HTMLButtonElement>
}

/**
 * Convenience component for a button containing only text.
 */
export function TextButton({ busyStatus, children, disabled, ref, ...rest }: TextButtonProps) {
  const busy = busyStatus !== undefined
  return (
    <>
      <Container
        ref={ref}
        {...rest}
        aria-busy={busy || undefined}
        disabled={disabled || busy}
        focusableWhenDisabled={busy || rest.focusableWhenDisabled}
      >
        {busy && <SpinningButtonIcon name="Loader" />}
        <Text>{children}</Text>
      </Container>
      {busy && (
        <span className={visuallyHidden} role="status">
          {busyStatus}
        </span>
      )}
    </>
  )
}

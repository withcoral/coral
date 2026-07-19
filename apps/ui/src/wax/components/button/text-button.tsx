import React from 'react'

import type { ButtonProps } from './container'
import { Container } from './container'
import { Text } from './text'

interface TextButtonProps extends Omit<ButtonProps, 'children'> {
  children: React.ReactNode
  ref?: React.Ref<HTMLButtonElement>
}

/**
 * Convenience component for a button containing only text.
 */
export function TextButton({ children, ref, ...rest }: TextButtonProps) {
  return (
    <Container ref={ref} {...rest}>
      <Text>{children}</Text>
    </Container>
  )
}

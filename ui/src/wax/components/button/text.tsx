import React from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'

import { text } from './text.css'

interface ImplementationProps extends React.PropsWithChildren {
  buttonVariant: ButtonVariant
  size: ButtonSize
}

export function Text(_props: React.PropsWithChildren) {
  return null
}

export function TextImplementation({ buttonVariant, children, size }: ImplementationProps) {
  return <div className={text({ buttonVariant, size })}>{children}</div>
}

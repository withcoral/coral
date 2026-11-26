import classNames from 'classnames'
import React from 'react'

import type { ButtonSize, ButtonVariant } from '@/wax/components/button/button.css'
import { Tooltip } from '@/wax/components/tooltip'

import { text } from './text.css'

interface TextProps extends React.PropsWithChildren {
  wrap?: boolean
  /** Shown on hover only while the text is cut off, such as a long name in a table cell. */
  tooltip?: React.ReactNode
  /** Lets the layout around the button style the label, such as hiding it in a narrow toolbar. */
  className?: string
}

interface ImplementationProps extends TextProps {
  buttonVariant: ButtonVariant
  size: ButtonSize
}

export function Text(_props: TextProps) {
  return null
}

export function TextImplementation({
  buttonVariant,
  children,
  size,
  wrap = false,
  tooltip,
  className,
}: ImplementationProps) {
  const label = (
    <span className={classNames(text({ buttonVariant, size, wrap }), className)}>{children}</span>
  )
  return tooltip === undefined ? (
    label
  ) : (
    <Tooltip content={tooltip} showOnlyWhenTruncated maxWidth={520}>
      {label}
    </Tooltip>
  )
}

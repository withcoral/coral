import classNames from 'classnames'
import React, { Children, ElementType, isValidElement } from 'react'

import * as styles from './pill.css'

export type PillColor =
  | 'amber'
  | 'blue'
  | 'gray'
  | 'graySubtle'
  | 'green'
  | 'mention'
  | 'orange'
  | 'purple'
  | 'red'

export type PillProps<T extends ElementType = 'div'> = PillBaseProps &
  (T extends 'button'
    ? Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'as' | keyof PillBaseProps>
    : Omit<React.HTMLAttributes<HTMLDivElement>, 'as' | keyof PillBaseProps>) & {
    as?: T
  }

export type PillSize = 'default' | 'large'

interface PillBaseProps {
  children: React.ReactNode
  className?: string
  color?: PillColor
  isActive?: boolean
  size?: PillSize
  title?: string
}

export function Pill<T extends ElementType = 'div'>({
  as,
  children,
  className,
  color = 'gray',
  isActive,
  size = 'default',
  title,
  ...rest
}: PillProps<T>) {
  const icons: React.ReactNode[] = []
  const textParts: React.ReactNode[] = []

  Children.forEach(children, (child) => {
    if (isValidElement(child)) {
      icons.push(child)
    } else {
      textParts.push(child)
    }
  })

  const Component = (as ?? 'div') as ElementType
  const isInteractive = as === 'button'

  return (
    <Component
      className={classNames(
        styles.basePill,
        styles.sizeVariants[size],
        styles.colorVariants[color],
        { [styles.active]: isActive, [styles.interactive]: isInteractive },
        className,
      )}
      title={title}
      {...(Component === 'button' && { type: 'button' })}
      {...rest}
    >
      {icons}
      <span className={styles.textContent}>{textParts}</span>
    </Component>
  )
}

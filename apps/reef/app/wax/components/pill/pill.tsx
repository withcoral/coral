import classNames from 'classnames'
import React, { Children, isValidElement } from 'react'
import type { ElementType } from 'react'

import { Icon } from '@/wax/components/icon'

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
    : Omit<React.HTMLAttributes<HTMLDivElement>, 'as' | keyof PillBaseProps>) &
  // The remove button needs a host that is not interactive itself, such as a button or link.
  (T extends 'div' | 'span'
    ? {
        /** Adds a button after the text that removes the pill. */
        remove?: PillRemove
      }
    : { remove?: never }) & {
    as?: T
  }

export interface PillRemove {
  label: string
  onClick: () => void
}

export type PillSize = 'small' | 'default' | 'large'

export type PillVariant = 'filled' | 'outline'

interface PillBaseProps {
  children: React.ReactNode
  className?: string
  color?: PillColor
  isActive?: boolean
  size?: PillSize
  title?: string
  variant?: PillVariant
}

export function Pill<T extends ElementType = 'div'>({
  as,
  children,
  className,
  color = 'gray',
  isActive,
  remove,
  size = 'default',
  title,
  variant = 'filled',
  ...rest
}: PillProps<T>) {
  // Separate icons from text content for proper truncation
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
        variant === 'outline' && styles.outlineVariants[color],
        { [styles.active]: isActive, [styles.interactive]: isInteractive },
        className,
      )}
      title={title}
      {...(Component === 'button' && { type: 'button' })}
      {...rest}
    >
      {icons}
      {textParts.length > 0 && <span className={styles.textContent}>{textParts}</span>}
      {remove && (
        <button
          aria-label={remove.label}
          className={styles.removeButton}
          onClick={(event) => {
            // The pill may have its own onClick; removing it should not run that too.
            event.stopPropagation()
            remove.onClick()
          }}
          type="button"
        >
          <Icon color="inherit" name="X" size="14" />
        </button>
      )}
    </Component>
  )
}

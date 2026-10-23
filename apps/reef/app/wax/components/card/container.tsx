import classNames from 'classnames'
import type { ComponentPropsWithoutRef, ElementType, Ref } from 'react'

import * as styles from './card.css'

export type ContainerProps<T extends ElementType = 'div'> = ComponentPropsWithoutRef<T> & {
  as?: T
  /** Less padding, for a card that is one row in a dense list. */
  compact?: boolean
  /** No padding, and content clipped to the corners, for content that runs to the card's edges. */
  flush?: boolean
  interactive?: boolean
  ref?: Ref<HTMLElement>
}

/** Card framing for custom content; Card supplies the standard summary content. */
export function Container<T extends ElementType = 'div'>({
  as,
  className,
  compact = false,
  flush = false,
  interactive = false,
  ...props
}: ContainerProps<T>) {
  const Component = as ?? 'div'
  return (
    <Component
      {...props}
      className={classNames(
        styles.card,
        {
          [styles.cardCompact]: compact,
          [styles.cardFlush]: flush,
          [styles.cardButton]: interactive,
        },
        className,
      )}
    />
  )
}

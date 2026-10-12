import classNames from 'classnames'
import React, { ElementType, ReactNode } from 'react'

import { Pill, type PillProps } from '@/wax/components/pill'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './card.css'
import { Container } from './container'

export type CardHeaderPill = Omit<PillProps, 'as' | 'children' | 'size'> & {
  label: string
  /** At the card's far edge, or right after the title; the far edge when unset. */
  placement?: 'end' | 'title'
}

interface CardBaseProps {
  /** Content under the description, such as the card's actions. */
  children?: ReactNode
  className?: string
  /** Less padding, for a card that is one row in a dense list. */
  compact?: boolean
  /** A line under the title; omit it when the title says everything. */
  description?: string
  /** Lines of description shown before it is cut off; five when unset. */
  descriptionLines?: number
  headerPill?: CardHeaderPill
  icon?: ReactNode
  interactive?: boolean
  title: string
}

/**
 * Only a plain card has the `action` slot. A card rendered `as` another element
 * is often interactive, where a control inside would nest, and its own props
 * keep their meaning: a form's `action` stays the form's destination.
 */
type CardSlot<T extends ElementType> =
  | {
      as?: undefined
      /** A control beside the title and description, such as the card's one action. */
      action?: ReactNode
    }
  | { as: T }

export type CardProps<T extends ElementType = 'div'> = CardBaseProps &
  Omit<React.ComponentPropsWithoutRef<T>, 'as' | keyof CardBaseProps> &
  CardSlot<T>

export function Card<T extends ElementType = 'div'>(
  props: CardProps<T> & { ref?: React.Ref<HTMLElement> },
) {
  const {
    as,
    children,
    className,
    description,
    descriptionLines,
    headerPill,
    icon,
    interactive,
    ref,
    title,
    ...others
  } = props
  const { action: slot, ...plain } = others as typeof others & { action?: ReactNode }
  const action = as === undefined ? slot : undefined
  const rest = as === undefined ? plain : others

  const Component = (as ?? 'div') as ElementType
  const type = 'type' in props ? props.type! : 'button'
  const isInteractive = interactive ?? (as !== undefined || 'onClick' in props)

  // An action keeps its pill beside the title, so the action alone holds the right edge.
  const pill = headerPill ? (
    <HeaderPill
      {...headerPill}
      className={classNames(action ? styles.inlinePill : undefined, headerPill.className)}
    />
  ) : null
  const summary = (
    <>
      <span className={styles.header}>
        {icon}
        <Typography.BodyLargeStrong className={styles.title} truncate>
          {title}
        </Typography.BodyLargeStrong>
        {pill}
      </span>
      {/* A clamped description shows in full on hover, as menu and tree items do. */}
      {description && (
        <Tooltip content={description} showOnlyWhenTruncated>
          <Typography.Body
            className={styles.description}
            style={descriptionLines ? { WebkitLineClamp: descriptionLines } : undefined}
            variant="tertiary"
          >
            {description}
          </Typography.Body>
        </Tooltip>
      )}
    </>
  )
  const content = (
    <>
      {action ? (
        <div className={styles.withAction}>
          <div className={styles.summary}>{summary}</div>
          <div className={styles.action}>{action}</div>
        </div>
      ) : (
        summary
      )}
      {children}
    </>
  )

  const componentProps = {
    className,
    ref,
    ...rest,
    ...(Component === 'button' && { type }),
  }

  return (
    <Container as={Component} interactive={isInteractive} {...componentProps}>
      {content}
    </Container>
  )
}

function HeaderPill({ className, label, placement = 'end', ...props }: CardHeaderPill) {
  return (
    <Pill as="span" {...props} className={classNames(styles.headerPill[placement], className)}>
      {label}
    </Pill>
  )
}

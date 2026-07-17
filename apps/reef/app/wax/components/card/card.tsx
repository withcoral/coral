import classNames from 'classnames'
import React, { ElementType, ReactNode } from 'react'

import { Pill, type PillProps } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import * as styles from './card.css'

export type CardHeaderPill = Omit<PillProps, 'children' | 'size'> & {
  label: string
}

interface CardBaseProps {
  className?: string
  description: string
  headerPill?: CardHeaderPill
  icon?: ReactNode
  interactive?: boolean
  title: string
}

export type CardProps<T extends ElementType = 'div'> = CardBaseProps &
  Omit<React.ComponentPropsWithoutRef<T>, 'as' | keyof CardBaseProps> & {
    as?: T
  }

export function Card<T extends ElementType = 'div'>(
  props: CardProps<T> & { ref?: React.Ref<HTMLElement> },
) {
  const { as, className, description, headerPill, icon, interactive, ref, title, ...rest } = props

  const Component = (as ?? 'div') as ElementType
  const type = 'type' in props ? props.type! : 'button'
  const isInteractive = interactive ?? (as !== undefined || 'onClick' in props)

  const content = (
    <>
      <span className={styles.header}>
        {icon}
        <Typography.BodyLargeStrong className={styles.title} truncate>
          {title}
        </Typography.BodyLargeStrong>
        {headerPill ? <HeaderPill {...headerPill} /> : null}
      </span>
      <Typography.Body className={styles.description} variant="tertiary">
        {description}
      </Typography.Body>
    </>
  )

  const componentProps = {
    className: classNames(styles.card, { [styles.cardButton]: isInteractive }, className),
    ref,
    ...rest,
    ...(Component === 'button' && { type }),
  }

  return <Component {...componentProps}>{content}</Component>
}

function HeaderPill({ className, label, ...props }: CardHeaderPill) {
  return (
    <Pill {...props} className={classNames(styles.headerPill, className)}>
      {label}
    </Pill>
  )
}

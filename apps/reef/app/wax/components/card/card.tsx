import classNames from 'classnames'
import { ReactNode } from 'react'

import { Button } from '@/wax/components'
import { Pill, type PillProps } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import * as styles from './card.css'

export type CardHeaderPill = Omit<PillProps, 'children' | 'size'> & {
  label: string
}

export interface CardProps {
  description: string
  headerPill?: CardHeaderPill
  icon?: ReactNode
  onSelect?: () => void
  title: string
}

export function Card({ description, headerPill, icon, onSelect, title }: CardProps) {
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

  if (onSelect) {
    return (
      <Button.Container
        className={classNames(styles.card, styles.cardButton)}
        onClick={onSelect}
        variant="bare"
      >
        {content}
      </Button.Container>
    )
  }

  return <div className={styles.card}>{content}</div>
}

function HeaderPill({ className, label, ...props }: CardHeaderPill) {
  return (
    <Pill {...props} className={classNames(styles.headerPill, className)}>
      {label}
    </Pill>
  )
}

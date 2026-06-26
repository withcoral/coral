import classNames from 'classnames'
import { ReactNode } from 'react'

import { Button } from '@/wax/components'
import { Typography } from '@/wax/components/typography'

import * as styles from './card.css'

export interface CardProps {
  description: string
  icon?: ReactNode
  onSelect?: () => void
  title: string
}

export function Card({ description, icon, onSelect, title }: CardProps) {
  const content = (
    <>
      <span className={styles.header}>
        {icon}
        <Typography.BodyLargeStrong className={styles.title} truncate>
          {title}
        </Typography.BodyLargeStrong>
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

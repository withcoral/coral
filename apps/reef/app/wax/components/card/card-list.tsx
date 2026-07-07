import React, { ElementType, ReactNode } from 'react'

import { Card, type CardHeaderPill } from './card'
import * as styles from './card-list.css'

export interface CardItem {
  description: string
  headerPill?: CardHeaderPill
  icon?: ReactNode
  id: string
  title: string
}

type CardListCardProps<T extends ElementType> = Omit<
  React.ComponentPropsWithoutRef<T>,
  'as' | 'children' | 'className' | 'description' | 'headerPill' | 'icon' | 'title'
> & {
  className?: string
  interactive?: boolean
}

export interface CardListProps<T extends ElementType = 'div'> {
  as?: T
  getCardProps?: (item: CardItem) => CardListCardProps<T>
  items: CardItem[]
  onSelect?: (item: CardItem) => void
}

export function CardList<T extends ElementType = 'div'>({
  as,
  getCardProps,
  items,
  onSelect,
}: CardListProps<T>) {
  return (
    <ul className={styles.grid}>
      {items.map((item) => {
        const cardProps = getCardProps?.(item)
        const Component = (onSelect && !as ? 'button' : as) as ElementType | undefined

        return (
          <li className={styles.item} key={item.id}>
            <Card
              as={Component}
              description={item.description}
              headerPill={item.headerPill}
              icon={item.icon}
              onClick={onSelect ? () => onSelect(item) : undefined}
              title={item.title}
              {...cardProps}
            />
          </li>
        )
      })}
    </ul>
  )
}

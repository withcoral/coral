import { ReactNode } from 'react'

import { Card, type CardHeaderPill } from './card'
import * as styles from './card-list.css'

export interface CardItem {
  description: string
  headerPill?: CardHeaderPill
  icon?: ReactNode
  id: string
  title: string
}

export interface CardListProps {
  items: CardItem[]
  onSelect?: (item: CardItem) => void
}

export function CardList({ items, onSelect }: CardListProps) {
  return (
    <ul className={styles.grid}>
      {items.map((item) => (
        <li className={styles.item} key={item.id}>
          <Card
            description={item.description}
            headerPill={item.headerPill}
            icon={item.icon}
            onSelect={onSelect ? () => onSelect(item) : undefined}
            title={item.title}
          />
        </li>
      ))}
    </ul>
  )
}

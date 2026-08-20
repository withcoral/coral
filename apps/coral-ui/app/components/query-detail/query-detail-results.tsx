import type { ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'

import * as styles from './query-detail-results.css'

export interface QueryDetailResultsProps {
  children?: ReactNode
  emptyMessage?: string
  title?: ReactNode
}

export function QueryDetailResults({
  children,
  emptyMessage = 'No rows returned.',
  title = 'Results',
}: QueryDetailResultsProps) {
  return (
    <div className={styles.root}>
      <Typography.HeadingXSmall as="h2">{title}</Typography.HeadingXSmall>
      {children ?? <Typography.Body variant="tertiary">{emptyMessage}</Typography.Body>}
    </div>
  )
}

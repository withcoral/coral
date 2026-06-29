import type { ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'
import * as styles from './page-header.css'

interface PageHeaderProps {
  title: ReactNode
  subtitle?: ReactNode
  leading?: ReactNode
  children?: ReactNode
}

export function PageHeader({ title, subtitle, leading, children }: PageHeaderProps) {
  return (
    <header className={styles.header}>
      {leading ? <div className={styles.leading}>{leading}</div> : null}
      <div className={styles.titleArea}>
        {typeof title === 'string' ? (
          <Typography.HeadingMedium as="h1">{title}</Typography.HeadingMedium>
        ) : (
          title
        )}
        {subtitle ? (
          typeof subtitle === 'string' ? (
            <Typography.BodySmall as="p" variant="tertiary">
              {subtitle}
            </Typography.BodySmall>
          ) : (
            subtitle
          )
        ) : null}
      </div>
      {children ? <div className={styles.actions}>{children}</div> : null}
    </header>
  )
}

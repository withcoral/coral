import { useId, type ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'

import * as styles from './description-list.css'

/** Read-only label and value pairs. An optional title heads the list. */
export function List({ title, children }: { title?: string; children: ReactNode }) {
  const id = useId()
  return (
    <section className={styles.section} aria-labelledby={title ? id : undefined}>
      {title && (
        <Typography.BodySmallStrong as="h4" id={id} className={styles.title}>
          {title}
        </Typography.BodySmallStrong>
      )}
      <dl className={styles.list}>{children}</dl>
    </section>
  )
}

/** One fact: what it is beside its exact value. The value can be text or a code block. */
export function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className={styles.row}>
      <Typography.BodySmall as="dt" className={styles.label}>
        {label}
      </Typography.BodySmall>
      <Typography.BodySmall as="dd" variant="primary" className={styles.value}>
        {children}
      </Typography.BodySmall>
    </div>
  )
}

import { useId, type ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'

import * as styles from './description-list.css'

/** Read-only label and value pairs. An optional title heads the list, at the heading level
 * the caller names: one under the heading the list sits in. */
export function List({
  title,
  heading,
  children,
}: { children: ReactNode } & (
  | { title: string; heading: 'h2' | 'h3' | 'h4' }
  | { title?: undefined; heading?: undefined }
)) {
  const id = useId()
  return (
    <section className={styles.section} aria-labelledby={title ? id : undefined}>
      {title && (
        <Typography.BodySmallStrong as={heading} id={id} className={styles.title}>
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

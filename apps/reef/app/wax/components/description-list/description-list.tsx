import classNames from 'classnames'
import { useId, type ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'

import * as styles from './description-list.css'

/** Label and value pairs. An optional title heads the list, at the heading level
 * the caller names: one under the heading the list sits in.
 *
 * `compact` suits a form's short facts: the labels take one column as wide as the widest,
 * and each row is as tall as a control, so a value can be a picker. */
export function List({
  title,
  heading,
  density = 'default',
  children,
}: { children: ReactNode; density?: 'default' | 'compact' } & (
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
      <dl className={classNames(styles.list, density === 'compact' && styles.compactList)}>
        {children}
      </dl>
    </section>
  )
}

/** One fact: what it is beside its exact value. The value can be text or a code block, or
 * in a compact list, a control. */
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

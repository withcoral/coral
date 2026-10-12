import type { ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'

import * as styles from './settings-layout.css'

export interface SettingsLayoutProps {
  /** The resource's name, its status and any page-wide notice. */
  header: ReactNode
  /** One card per setting, each filling the main column. */
  children: ReactNode
  /** A panel beside the cards, such as the resource's source. */
  aside?: ReactNode
  /** The removal section, apart at the end. */
  removal?: ReactNode
}

/** One resource's settings page: one card per setting, then removal apart at the end. */
export function SettingsLayout({ header, children, aside, removal }: SettingsLayoutProps) {
  return (
    <div className={styles.page}>
      <div className={styles.top}>
        <div className={styles.header}>{header}</div>
        <div className={styles.columns}>
          <div className={styles.main}>{children}</div>
          {aside}
        </div>
      </div>
      {removal}
    </div>
  )
}

export interface SettingsRemovalProps {
  title: string
  /** What removal deletes and what Coral keeps. */
  description: string
  /** The confirming control, usually a destructive dialog trigger. */
  action: ReactNode
}

/** Removal closes a settings page, apart from the rest, with its action at the right. */
export function SettingsRemoval({ title, description, action }: SettingsRemovalProps) {
  return (
    <section className={styles.removal} aria-label={title}>
      <div className={styles.removalText}>
        <Typography.HeadingSmall as="h2">{title}</Typography.HeadingSmall>
        <Typography.BodySmall as="p">{description}</Typography.BodySmall>
      </div>
      {action}
    </section>
  )
}

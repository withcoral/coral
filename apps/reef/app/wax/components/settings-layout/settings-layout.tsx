import type { ReactNode } from 'react'

import * as CardFrame from '@/wax/components/card'
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

export interface SettingsAsideProps {
  /** Heads the panel and names it for assistive technology. */
  title: string
  /** Usually `SettingsFacts`, then any actions. */
  children: ReactNode
}

export interface SettingsWideColumnsProps {
  /** Content as wide as the page allows, such as a long list. */
  children: ReactNode
  /** The settings aside, at the width it has on a settings page. */
  aside: ReactNode
}

/** A wide main column with a settings aside beside it; narrow screens stack them. */
export function SettingsWideColumns({ children, aside }: SettingsWideColumnsProps) {
  return (
    <div className={styles.wideColumns}>
      <div className={styles.wideMain}>{children}</div>
      {aside}
    </div>
  )
}

/** The panel beside a settings page's cards: what the resource is, at a glance. */
export function SettingsAside({ title, children }: SettingsAsideProps) {
  return (
    <CardFrame.Container as="aside" className={styles.aside} aria-label={title}>
      <Typography.BodyLargeStrong as="h2">{title}</Typography.BodyLargeStrong>
      {children}
    </CardFrame.Container>
  )
}

/** A list of labelled facts in a settings aside. */
export function SettingsFacts({ children }: { children: ReactNode }) {
  return <dl className={styles.facts}>{children}</dl>
}

export interface SettingsFactProps {
  label: string
  children: ReactNode
}

/** One labelled fact, its label over its value. */
export function SettingsFact({ label, children }: SettingsFactProps) {
  return (
    <div className={styles.fact}>
      <Typography.BodySmallStrong as="dt">{label}</Typography.BodySmallStrong>
      <Typography.BodySmall as="dd" className={styles.factValue}>
        {children}
      </Typography.BodySmall>
    </div>
  )
}

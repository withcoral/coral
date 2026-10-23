import type { ReactNode } from 'react'
import { Button, Typography } from '@/wax/components'
import { Meter } from '@/wax/components/charts'
import { HighlightedMatches } from '@/wax/components/highlighted-matches'
import { Icon } from '@/wax/components/icon'
import { Logo } from '@/wax/components/logo'
import * as Table from '@/wax/components/table'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import type { StopReason } from '@/lib/activity-summary'
import type { FilterQuery } from '@/utils/filter-query'
import { counted, number, outcomes } from './format'
import { STOP_REASONS, type OperationRow } from './operations'
import * as styles from './summary.css'

export function Note({ children }: { children: string }) {
  return (
    <Typography.BodySmall as="p" variant="tertiary">
      {children}
    </Typography.BodySmall>
  )
}

/** A count, then a bar of it against `max`. */
export function BarCell({
  value,
  label = number(value),
  max,
}: {
  value: number
  label?: string
  max: number
}) {
  return (
    <span className={styles.barCell}>
      <Typography.BodySmall as="span" variant="tertiary" className={styles.count}>
        {label}
      </Typography.BodySmall>
      <Meter value={value} max={max} size="small" />
    </span>
  )
}

/** An empty cell, marked so it reads as none rather than missing. */
export function None() {
  return (
    <Typography.BodySmall as="span" variant="tertiary">
      —
    </Typography.BodySmall>
  )
}

/** Stopped calls by who must act, each as an icon and a count. */
export function StopSplit({ counts }: { counts: Record<StopReason, number> }) {
  return (
    <span className={styles.stopSplit}>
      {STOP_REASONS.filter((reason) => counts[reason] > 0).map((reason) => (
        <span key={reason} className={styles.labeled}>
          <Icon name={outcomes[reason].icon} size="14" color="tertiary" />
          <span aria-hidden>{number(counts[reason])}</span>
          <span className={visuallyHidden}>{counted(reason, counts[reason])}</span>
        </span>
      ))}
    </span>
  )
}

export function Reason({ reason }: { reason: StopReason }) {
  return (
    <span className={styles.labeled}>
      <Icon name={outcomes[reason].icon} size="14" color="tertiary" />
      <Typography.BodySmall as="span">{outcomes[reason].label}</Typography.BodySmall>
    </span>
  )
}

/**
 * The provider's logo and name, then the operation's own name. Under a provider's
 * heading, only the operation. The full Call Path is the cell's title.
 */
export function OperationName({
  row,
  to,
  query,
  grouped = false,
}: {
  row: OperationRow
  to?: string
  query?: FilterQuery
  grouped?: boolean
}) {
  const mark = (text: string) => (query ? <HighlightedMatches text={text} query={query} /> : text)
  const operation = (
    <Typography.CodeSmallInline className={styles.truncate}>
      {mark(row.operation)}
    </Typography.CodeSmallInline>
  )
  return (
    <span className={grouped ? styles.grouped : styles.operation}>
      {!grouped && (
        <>
          <Logo name={row.provider} icons={row.icons} size="20" />
          <Typography.BodySmall as="span" variant="tertiary" className={styles.operationProvider}>
            {mark(row.provider)}
          </Typography.BodySmall>
        </>
      )}
      {to ? <Table.RowLink to={to}>{operation}</Table.RowLink> : operation}
    </span>
  )
}

export function ActionLink({ to, children }: { to: string; children: string }) {
  return (
    <Button.InternalLink to={to} variant="link" size="x-small">
      {children}
    </Button.InternalLink>
  )
}

/** A titled part of a lens, with a count beside its title and a line that says what it holds. */
export function Section({
  title,
  count,
  note,
  children,
}: {
  title: string
  count: number
  note: string
  children: ReactNode
}) {
  return (
    <section className={styles.section} aria-label={title}>
      <div className={styles.sectionHeading}>
        <Typography.BodySmallStrong as="h3">{title}</Typography.BodySmallStrong>
        <Typography.BodySmall as="span" variant="tertiary">
          {number(count)}
        </Typography.BodySmall>
      </div>
      <Note>{note}</Note>
      {children}
    </section>
  )
}

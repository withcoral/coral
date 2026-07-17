import type { ReactNode } from 'react'

import { highlightSQL } from '@/lib/sql-highlight'
import { Typography } from '@/wax/components/typography'

import * as styles from './query-detail-summary.css'

export type QueryDetailStatusTone = 'error' | 'ok' | 'running'

export interface QueryDetailStat {
  label: string
  value: ReactNode
}

export interface QueryDetailSummaryProps {
  actions?: ReactNode
  children?: ReactNode
  shortcuts?: ReactNode
  sql: string
  stats?: QueryDetailStat[]
  statusLabel?: ReactNode
  statusTone?: QueryDetailStatusTone
  title: ReactNode
}

export function QueryDetailSummary({
  actions,
  children,
  shortcuts,
  sql,
  stats = [],
  statusLabel,
  statusTone = 'running',
  title,
}: QueryDetailSummaryProps) {
  const hasHeaderActions = Boolean(statusLabel || actions)

  return (
    <div className={styles.root}>
      {shortcuts}
      <header className={styles.header}>
        <div className={styles.headerTitle}>
          {typeof title === 'string' ? (
            <Typography.BodyStrong as="span" variant="secondary">
              {title}
            </Typography.BodyStrong>
          ) : (
            title
          )}
        </div>
        {hasHeaderActions ? (
          <div className={styles.headerActions}>
            {statusLabel ? (
              <span className={styles.statusBadge} data-tone={statusTone}>
                {statusLabel}
              </span>
            ) : null}
            {actions}
          </div>
        ) : null}
      </header>
      <div className={styles.scrollBody}>
        <div className={styles.content}>
          <div className={styles.sqlBlock}>
            <pre>
              <QuerySqlCode sql={sql} />
            </pre>
          </div>
          {stats.length > 0 ? (
            <div className={styles.statGrid}>
              {stats.map((stat) => (
                <QueryDetailStatCard key={stat.label} label={stat.label} value={stat.value} />
              ))}
            </div>
          ) : null}
          {children}
        </div>
      </div>
    </div>
  )
}

function QuerySqlCode({ sql }: { sql: string }) {
  return <code dangerouslySetInnerHTML={{ __html: highlightSQL(sql) }} />
}

function QueryDetailStatCard({ label, value }: QueryDetailStat) {
  return (
    <div className={styles.statCard}>
      <Typography.Body variant="tertiary">{label}</Typography.Body>
      <Typography.BodyLargeStrong>{value}</Typography.BodyLargeStrong>
    </div>
  )
}

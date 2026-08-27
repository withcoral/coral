import type { ReactNode } from 'react'

import { CodeBlock, type CodeLanguage } from '@/components/code-block'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
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
  codeLanguage?: CodeLanguage
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
  codeLanguage = 'sql',
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
      <ScrollArea className={styles.scrollBody} constrainWidth fade="none" fillContent>
        <div className={styles.content}>
          <CodeBlock code={sql} language={codeLanguage} />
          <QueryDetailStats stats={stats} />
          {children}
        </div>
      </ScrollArea>
    </div>
  )
}

export function QueryDetailStats({ stats }: { stats: QueryDetailStat[] }) {
  if (stats.length === 0) return null

  return (
    <div className={styles.statGrid}>
      {stats.map((stat) => (
        <QueryDetailStatCard key={stat.label} label={stat.label} value={stat.value} />
      ))}
    </div>
  )
}

function QueryDetailStatCard({ label, value }: QueryDetailStat) {
  return (
    <div className={styles.statCard}>
      <Typography.Body variant="tertiary">{label}</Typography.Body>
      <Typography.BodyLargeStrong>{value}</Typography.BodyLargeStrong>
    </div>
  )
}

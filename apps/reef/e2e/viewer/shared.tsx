import type { ComponentProps, ReactNode } from 'react'

import { Banner } from '@/wax/components/banner'
import { Breadcrumbs as WaxBreadcrumbs } from '@/wax/components/breadcrumbs/breadcrumbs'
import * as Button from '@/wax/components/button'
import * as Card from '@/wax/components/card'
import { Pill, type PillColor } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import type { Result, RunSummary, ScenarioEntry, ScenarioResult } from './types'
import * as styles from './reel.css'

export const evidencePath = (path: string) => path.split('/').map(encodeURIComponent).join('/')
export const seconds = (ms?: number) => (ms === undefined ? '—' : `${(ms / 1000).toFixed(1)}s`)
export const date = (value?: string) =>
  value && Number.isFinite(Date.parse(value))
    ? new Intl.DateTimeFormat('en', {
        dateStyle: 'medium',
        timeStyle: 'medium',
        timeZone: 'UTC',
      }).format(new Date(value)) + ' UTC'
    : 'Time unavailable'
export const sessionLabel = (result: Result) =>
  `${result.actor ? `${result.actor} · ` : ''}${result.label}`

export function EvidenceLink({
  children,
  wrap = false,
  ...props
}: ComponentProps<'a'> & { wrap?: boolean }) {
  return (
    <Button.Container as="a" variant="link" size="22" {...props}>
      <Button.Text wrap={wrap}>{children}</Button.Text>
    </Button.Container>
  )
}
const statusColors: Record<ScenarioResult['status'], PillColor> = {
  passed: 'green',
  failed: 'red',
  pending: 'blue',
  skipped: 'amber',
}
export function Status({
  status,
  children,
}: {
  status: ScenarioResult['status']
  children?: ReactNode
}) {
  return (
    <Pill color={statusColors[status]}>
      {children ??
        { pending: 'Running / incomplete', passed: 'Passed', failed: 'Failed', skipped: 'Skipped' }[
          status
        ]}
    </Pill>
  )
}
export function Counts({ results }: { results: ScenarioEntry[] }) {
  if (!results.length) return <Typography.BodySmall>No scenario results</Typography.BodySmall>
  return (
    <div className={styles.row}>
      {(['failed', 'pending', 'passed', 'skipped'] as const).map((status) => {
        const count = results.filter(({ result }) => result.status === status).length
        return count ? (
          <Status
            key={status}
            status={status}
          >{`${count} ${status === 'pending' ? 'incomplete' : status}`}</Status>
        ) : null
      })}
    </div>
  )
}
export function RunContext({ run }: { run: RunSummary }) {
  return (
    <div className={styles.row}>
      <Typography.BodySmall>{date(run.startedAt)}</Typography.BodySmall>
      {run.targets.map((target) => (
        <Typography.BodySmall key={target.name}>{target.name}</Typography.BodySmall>
      ))}
      {run.commit && (
        <Typography.CodeSmallInline as="code">{run.commit.slice(0, 7)}</Typography.CodeSmallInline>
      )}
      {run.dirty && <Typography.BodySmall>Uncommitted changes</Typography.BodySmall>}
      <Typography.BodySmall>{run.film ? 'Film mode' : 'Fast mode'}</Typography.BodySmall>
    </div>
  )
}
export function Breadcrumbs({ items }: { items: { label: string; href?: string }[] }) {
  return (
    <WaxBreadcrumbs
      ariaLabel="Breadcrumb"
      items={items.map((item, index) =>
        item.href
          ? { id: String(index), type: 'anchor', text: item.label, href: item.href }
          : { id: String(index), type: 'text', text: item.label },
      )}
    />
  )
}
export function Section({
  title,
  id,
  aside,
  children,
}: {
  title: string
  id?: string
  aside?: ReactNode
  children: ReactNode
}) {
  return (
    <section className={styles.stack} aria-label={title} id={id}>
      <div className={styles.sectionHeading}>
        <Typography.HeadingMedium as="h2">{title}</Typography.HeadingMedium>
        {aside}
      </div>
      {children}
    </section>
  )
}
export function Empty({ children }: { children: ReactNode }) {
  return (
    <Card.Container className={styles.empty}>
      <Typography.Body as="div">{children}</Typography.Body>
    </Card.Container>
  )
}
export function Failure({
  title = 'What failed',
  children,
}: {
  title?: string
  children: ReactNode
}) {
  return (
    <Banner variant="error" title={title}>
      <div className={styles.stack}>{children}</div>
    </Banner>
  )
}
export function Source({
  source,
  path,
  session = false,
}: {
  source?: string
  path: string
  session?: boolean
}) {
  return (
    <Card.Container as="details" data-testid="source">
      <Typography.BodyStrong as="summary" className={styles.summary}>
        Test source <Typography.CodeSmallInline as="span">{path}</Typography.CodeSmallInline>
      </Typography.BodyStrong>
      <div id="source" className={styles.detailsContent}>
        {source !== undefined ? (
          <div className={styles.stack}>
            {session && (
              <Typography.BodySmall as="p">
                Source snapshot captured for this recording. The file may contain other scenarios.
              </Typography.BodySmall>
            )}
            <Typography.Code as="pre" className={styles.code}>
              <Typography.Code as="code">{source}</Typography.Code>
            </Typography.Code>
          </div>
        ) : (
          <Typography.Body as="p">No source snapshot available.</Typography.Body>
        )}
      </div>
    </Card.Container>
  )
}
export function RecordedFrame({ children }: { children: ReactNode }) {
  return (
    <div className={styles.frame}>
      <EvidenceLink className={styles.skip} href="#main">
        Skip to content
      </EvidenceLink>
      <header className={styles.header}>
        <EvidenceLink href="/">Reel</EvidenceLink>
        <Pill color="amber">Private test evidence</Pill>
      </header>
      <main id="main" className={styles.main}>
        {children}
      </main>
      <footer className={styles.footer}>
        <Typography.BodySmall as="p">
          Videos, traces, source and logs may contain credentials or private content. This viewer
          has no authentication. Share only with trusted reviewers; do not publish evidence
          automatically.
        </Typography.BodySmall>
      </footer>
    </div>
  )
}

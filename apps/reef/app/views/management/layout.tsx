import type { ReactNode } from 'react'
import { Banner, Typography } from '@/wax/components'
import { RestoreWorkspaceButton } from './restore-workspace'
import type { Segment } from '@/wax/components/breadcrumbs/breadcrumbs'
import { PageLayout } from '@/wax/components/page-layout'
import type { Crumb, WorkspaceView } from './types'
import * as styles from './management.css'

export function ManagementLayout({
  breadcrumbs,
  titled,
  compact,
  actions,
  bounded,
  children,
}: {
  breadcrumbs: Segment[]
  /** The content carries the page's h1; see `PageLayout`. */
  titled?: boolean
  /** Tighter page spacing; see `PageLayout`. */
  compact?: boolean
  /** Page actions at the end of the top bar. */
  actions?: ReactNode
  /** The page is no taller than the viewport; its scroll table scrolls instead. */
  bounded?: boolean
  children: ReactNode
}) {
  return (
    <PageLayout
      breadcrumbs={breadcrumbs}
      titled={titled}
      compact={compact}
      actions={actions}
      bounded={bounded}
    >
      {children}
    </PageLayout>
  )
}

/**
 * One line under the breadcrumb for the current page's status.
 * Renders nothing when a page has none.
 */
export function PageIntro({ status }: { status?: ReactNode }) {
  if (!status) return null
  return <div className={styles.pageIntro}>{status}</div>
}

/** Links to each parent page, ending on the current page as text. */
export function trail(parents: Crumb[], current: string): Segment[] {
  return [
    ...parents.map((p, i) => ({
      id: `parent-${i}`,
      type: 'link' as const,
      text: p.label,
      to: p.to,
    })),
    { id: 'current', type: 'text' as const, text: current },
  ]
}

export function Section({
  id,
  title,
  description,
  actions,
  children,
}: {
  id?: string
  title: string
  description?: string
  actions?: ReactNode
  children: ReactNode
}) {
  return (
    <section className={styles.section} aria-label={title} id={id}>
      <div className={styles.sectionHeading}>
        <div className={styles.intro}>
          <Typography.HeadingSmall as="h2">{title}</Typography.HeadingSmall>
          {description && <Typography.Body as="p">{description}</Typography.Body>}
        </div>
        {actions && <div className={styles.headerActions}>{actions}</div>}
      </div>
      {children}
    </section>
  )
}

/** Says the Workspace is archived and what that blocks. Only an Owner can restore it here. */
export function ArchivedBanner({
  workspace,
  children,
}: {
  workspace: WorkspaceView
  /** What the archive blocks on this page. */
  children: ReactNode
}) {
  return (
    <Banner
      variant="warning"
      title="This workspace is archived"
      action={
        workspace.effectiveRole === 'owner' ? (
          <RestoreWorkspaceButton workspace={workspace} />
        ) : undefined
      }
    >
      {children}
    </Banner>
  )
}

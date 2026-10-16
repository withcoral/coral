import type { ReactNode } from 'react'

import { Breadcrumbs, type Segment } from '@/wax/components/breadcrumbs/breadcrumbs'

import * as ScrollArea from '@/wax/components/scroll-area'
import * as styles from './page-layout.css'

export interface PageLayoutProps {
  children: ReactNode
  /** Notices and context above the content. No divider is added. */
  header?: ReactNode
  /** Navigation in a separate top bar above the scrolling content. The current segment titles the page. */
  breadcrumbs?: Segment[]
  /**
   * The page's h1 in place of the current breadcrumb, for a title that
   * carries its own control, such as a name to rename.
   */
  title?: ReactNode
  /** Page-level actions at the end of the top bar. */
  actions?: ReactNode
  /** Standard pages are centered; wide pages fill the width. Split pages bound their own scrolling. */
  variant?: 'standard' | 'wide' | 'split'
  /**
   * Keeps a standard or wide page no taller than the viewport. The page does not
   * scroll, so put a `layout="scroll"` table in it to scroll its rows instead.
   */
  bounded?: boolean
}

/**
 * The shared page frame. Standard content is centered and capped at 960px;
 * wide and split pages fill the width from the left edge.
 * Split pages provide a bounded body for independent pane scrolling.
 * All variants use the same page gutters, header spacing, and mobile treatment.
 */
export function PageLayout({
  children,
  header,
  breadcrumbs,
  title,
  actions,
  variant = 'standard',
  bounded = false,
}: PageLayoutProps) {
  const frame = (
    <div className={styles.frame({ variant, bounded })}>
      {header && <div className={styles.header}>{header}</div>}
      {variant === 'split' ? <div className={styles.splitBody}>{children}</div> : children}
    </div>
  )
  return (
    <div className={styles.container}>
      {(breadcrumbs || title) && (
        <TopBar breadcrumbs={breadcrumbs ?? []} title={title} actions={actions} />
      )}
      {variant === 'split' || bounded ? (
        <div className={styles.scrollContainer}>{frame}</div>
      ) : (
        <ScrollArea.Container
          className={styles.scrollContainer}
          constrainWidth
          // The frame centres short pages against the viewport's height, which
          // only resolves when the scroll content is at least as tall as it.
          fillContent
          // No tab stop of its own: the page outline would take focus before the
          // content. Browsers make a scroller focusable when nothing in it is.
          scrollDirection="vertical"
        >
          {frame}
        </ScrollArea.Container>
      )}
    </div>
  )
}

/**
 * Coral's navigation bar keeps breadcrumbs separate from page content. Pages
 * have no other title: the current segment is the page's h1.
 */
export function TopBar({
  breadcrumbs,
  title,
  actions,
}: {
  breadcrumbs: Segment[]
  title?: ReactNode
  actions?: ReactNode
}) {
  return (
    <header className={styles.topBar}>
      <Breadcrumbs items={breadcrumbs} heading current={title} />
      {actions && <div className={styles.topBarActions}>{actions}</div>}
    </header>
  )
}

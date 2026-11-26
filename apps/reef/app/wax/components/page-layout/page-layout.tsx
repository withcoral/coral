import classNames from 'classnames'
import type { ReactNode } from 'react'

import { Breadcrumbs, type Segment } from '@/wax/components/breadcrumbs/breadcrumbs'
import { NavigationTrigger } from '@/wax/components/navigation-frame/navigation-frame'

import * as ScrollArea from '@/wax/components/scroll-area'
import { lightTheme } from '@/wax/theme/theme-light.css'
import * as styles from './page-layout.css'

interface PageLayoutBaseProps {
  children: ReactNode
  /** Notices and context above the content. No divider is added. */
  header?: ReactNode
  /** Navigation in a separate top bar above the scrolling content. The current segment titles the page. */
  breadcrumbs?: Segment[]
  /**
   * The content carries the page's h1, such as a toolbar's title or a name
   * to rename, so the current breadcrumb is not a heading.
   */
  titled?: boolean
  /**
   * 24px from the edges, with 16px between the page's parts, such as a table
   * under its filter toolbar, so the content gets the room.
   */
  compact?: boolean
  /** Page-level actions at the end of the top bar. */
  actions?: ReactNode
  /** Standard pages are centered; wide pages fill the width. Split pages bound their own scrolling. */
  variant?: 'standard' | 'wide' | 'split'
  /**
   * Keeps a standard or wide page no taller than the viewport. The page does not
   * scroll, so put a `layout="scroll"` table in it to scroll its rows instead.
   */
  bounded?: boolean
  /**
   * The content pins a row to the top as it scrolls, such as a step's title and
   * actions. The page fades only its bottom edge, so the pinned row stays clear.
   */
  pinnedTop?: boolean
}

// Only a standard page that scrolls has room for a panel.
export type PageLayoutProps = PageLayoutBaseProps &
  (
    | { panel?: never }
    | {
        variant?: 'standard'
        bounded?: false
        /**
         * A light panel beside the page, to the window's right and bottom
         * edges, for content that only comes in light colors, such as Stripe's
         * checkout form. The page's content keeps to the window's middle beside
         * it. A narrow window puts the panel under the content. A falsy panel,
         * such as `condition && <Form />`, shows none.
         */
        panel?: ReactNode
      }
  )

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
  titled = false,
  compact = false,
  actions,
  variant = 'standard',
  bounded = false,
  pinnedTop = false,
  panel,
}: PageLayoutProps) {
  const beside = Boolean(panel)
  const frame = (
    <div className={styles.frame({ variant, bounded, compact, beside })}>
      {header && <div className={styles.header}>{header}</div>}
      {variant === 'split' ? <div className={styles.splitBody}>{children}</div> : children}
    </div>
  )
  return (
    <div className={styles.container}>
      {breadcrumbs && <TopBar breadcrumbs={breadcrumbs} heading={!titled} actions={actions} />}
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
          // A fade would dim the panel's ends, and the scrollbar sits on the panel.
          // A top fade would dim a pinned row.
          fade={beside ? 'none' : pinnedTop ? 'bottom' : 'both'}
          scrollbarClassName={beside ? lightTheme : undefined}
        >
          {beside ? (
            <div className={styles.withPanel}>
              {frame}
              <div className={classNames(lightTheme, styles.panel)}>
                <div className={styles.panelContent}>{panel}</div>
              </div>
            </div>
          ) : (
            frame
          )}
        </ScrollArea.Container>
      )}
    </div>
  )
}

/**
 * Coral's navigation bar keeps breadcrumbs separate from page content. Unless
 * the content has its own title, the current segment is the page's h1. A page
 * outside the navigation, such as onboarding, puts `start` in their place.
 */
export function TopBar({
  breadcrumbs,
  start,
  heading = true,
  actions,
}: { actions?: ReactNode } & (
  | { breadcrumbs: Segment[]; heading?: boolean; start?: never }
  | { breadcrumbs?: never; heading?: never; start: ReactNode }
)) {
  const custom = !breadcrumbs
  return (
    <header className={styles.topBar}>
      {/* On a phone the navigation is a drawer, opened from the start of this bar. */}
      <div className={styles.topBarStart({ custom })}>
        <NavigationTrigger />
        {breadcrumbs ? <Breadcrumbs items={breadcrumbs} heading={heading} /> : start}
      </div>
      {actions && <div className={styles.topBarActions({ custom })}>{actions}</div>}
    </header>
  )
}

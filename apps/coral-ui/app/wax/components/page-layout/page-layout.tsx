import { type ReactNode } from 'react'

import { copyTextToClipboard } from '@/utils/copy-text-to-clipboard'
import { Breadcrumbs, type Segment } from '@/wax/components/breadcrumbs/breadcrumbs'
import { Button } from '@/wax/components'
import { Tooltip } from '@/wax/components/tooltip'

import * as styles from './page-layout.css'

interface PageLayoutProps {
  /** Additional actions to render in the top bar (before the copy link button) */
  actions?: ReactNode
  /** Breadcrumb items for navigation */
  breadcrumbs: Segment[]
  /** Page content */
  children: ReactNode
  /** Whether to show the copy link button (defaults to true) */
  showCopyLink?: boolean
}

interface TopBarProps {
  /** Additional actions to render (before the copy link button) */
  actions?: ReactNode
  /** Breadcrumb items for navigation */
  breadcrumbs: Segment[]
  /** Whether to show the copy link button (defaults to true) */
  showCopyLink?: boolean
}

/**
 * Copy link button that copies the current page URL to clipboard.
 */
export function CopyLinkButton() {
  return (
    <Tooltip content="Copy link to clipboard">
      <Button.Container onClick={() => copyTextToClipboard(window.location.href)} variant="bare">
        <Button.Icon name="Link" />
      </Button.Container>
    </Tooltip>
  )
}

/**
 * PageLayout provides a consistent layout structure for detail pages.
 * Includes a top bar with breadcrumbs, optional actions, and a copy link button.
 */
export function PageLayout({
  actions,
  breadcrumbs,
  children,
  showCopyLink = true,
}: PageLayoutProps) {
  return (
    <div className={styles.container}>
      <TopBar actions={actions} breadcrumbs={breadcrumbs} showCopyLink={showCopyLink} />
      <div className={styles.content}>{children}</div>
    </div>
  )
}

/**
 * TopBar component for page headers with breadcrumbs and actions.
 * Can be used standalone or as part of PageLayout.
 */
export function TopBar({ actions, breadcrumbs, showCopyLink = true }: TopBarProps) {
  return (
    <header className={styles.topBar}>
      <Breadcrumbs items={breadcrumbs} />
      <div className={styles.topBarActions}>
        {actions}
        {showCopyLink && <CopyLinkButton />}
      </div>
    </header>
  )
}

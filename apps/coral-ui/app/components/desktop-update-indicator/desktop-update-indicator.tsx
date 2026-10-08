import classNames from 'classnames'

import { animations } from '@/wax/animations'
import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import * as styles from './desktop-update-indicator.css'

export type DesktopUpdateIndicatorState =
  | { status: 'available'; version: string }
  | { status: 'downloading'; version: string }
  | { status: 'ready'; version: string }

export interface DesktopUpdateIndicatorProps {
  readonly isMinimized: boolean
  readonly isPending?: boolean
  readonly onDownload: () => void
  readonly onInstall: () => void
  readonly state: DesktopUpdateIndicatorState
}

const PRESENTATION: Record<
  DesktopUpdateIndicatorState['status'],
  { icon: IconName; title: string }
> = {
  available: {
    icon: 'Download',
    title: 'Update available',
  },
  downloading: {
    icon: 'RefreshCw',
    title: 'Downloading',
  },
  ready: {
    icon: 'CircleCheck',
    title: 'Update ready',
  },
}

export function DesktopUpdateIndicator({
  isMinimized,
  isPending = false,
  onDownload,
  onInstall,
  state,
}: DesktopUpdateIndicatorProps) {
  const presentation = PRESENTATION[state.status]
  const accessibleLabel = updateAccessibleLabel(state)
  const className = styles.indicator({
    isInteractive: state.status !== 'downloading',
    isMinimized,
    status: state.status,
  })
  const content = (
    <>
      <Icon
        className={classNames(styles.icon, {
          [animations.spinAnimation]: state.status === 'downloading',
        })}
        color="inherit"
        name={presentation.icon}
        size="18"
      />
      {!isMinimized && (
        <div className={styles.copy}>
          <Typography.BodySmallStrong truncate>{presentation.title}</Typography.BodySmallStrong>
          <Typography.BodySmall truncate>
            {state.status === 'ready' ? 'Restart to install' : `Coral ${state.version}`}
          </Typography.BodySmall>
        </div>
      )}
    </>
  )

  // The button cannot also be a role="status" region, so aria-live stands in to
  // keep it announcing transitions.
  const indicator =
    state.status === 'downloading' ? (
      <div
        aria-atomic="true"
        aria-label={accessibleLabel}
        className={className}
        role="status"
        tabIndex={isMinimized ? 0 : undefined}
      >
        {content}
      </div>
    ) : (
      <button
        aria-atomic="true"
        aria-label={accessibleLabel}
        aria-live="polite"
        className={className}
        disabled={isPending}
        onClick={state.status === 'available' ? onDownload : onInstall}
        type="button"
      >
        {content}
      </button>
    )

  if (!isMinimized) return indicator

  return (
    <Tooltip content={accessibleLabel} side="right">
      {indicator}
    </Tooltip>
  )
}

function updateAccessibleLabel(state: DesktopUpdateIndicatorState): string {
  switch (state.status) {
    case 'available':
      return `Download Coral ${state.version}`
    case 'downloading':
      return `Coral ${state.version} is downloading.`
    case 'ready':
      return `Restart to install Coral ${state.version}`
  }
}

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

interface DesktopUpdateIndicatorProps {
  isMinimized: boolean
  state: DesktopUpdateIndicatorState
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

export function DesktopUpdateIndicator({ isMinimized, state }: DesktopUpdateIndicatorProps) {
  const presentation = PRESENTATION[state.status]
  const accessibleLabel = updateAccessibleLabel(state)
  const indicator = (
    <div
      aria-atomic="true"
      aria-label={accessibleLabel}
      className={styles.indicator({ isMinimized, status: state.status })}
      role="status"
      tabIndex={isMinimized ? 0 : undefined}
    >
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
    </div>
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
      return `Coral ${state.version} is available and will download automatically.`
    case 'downloading':
      return `Coral ${state.version} is downloading.`
    case 'ready':
      return `Coral ${state.version} is ready. Restart to install.`
  }
}

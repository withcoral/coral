import type React from 'react'

import { IconButton } from '../button/icon-button'
import { TextButton } from '../button/text-button'
import { Icon } from '../icon/icon'
import type { IconColor, IconName } from '../icon/icon'
import { Typography } from '../typography/typography'

import type { ToastAction, ToastVariant } from './toast'

import * as styles from './toast-content.css'

const ICON_CONFIG: Record<ToastVariant, { color: IconColor; name: IconName }> = {
  error: { color: 'error', name: 'CircleAlertFull' },
  neutral: { color: 'secondary', name: 'Info' },
  success: { color: 'success', name: 'CircleCheckFull' },
  warning: { color: 'warning', name: 'TriangleAlertFull' },
}

interface ToastContentProps {
  action?: ToastAction
  description?: React.ReactNode
  onClose: () => void
  title: React.ReactNode
  variant: ToastVariant
}

export function ToastContent({ action, description, onClose, title, variant }: ToastContentProps) {
  const iconConfig = ICON_CONFIG[variant]

  return (
    <div className={styles.container}>
      <div className={styles.iconWrapper}>
        <Icon color={iconConfig.color} name={iconConfig.name} size="18" />
      </div>
      <div className={styles.content}>
        <Typography.BodyStrong variant="primary">{title}</Typography.BodyStrong>
        {description && <Typography.Body>{description}</Typography.Body>}
        {action && (
          <TextButton onClick={action.onClick} size="22" variant="secondary">
            {action.label}
          </TextButton>
        )}
      </div>
      <IconButton
        ariaLabel="Dismiss"
        className={styles.closeButton}
        name="X"
        onClick={onClose}
        size="22"
        variant="bare"
      />
    </div>
  )
}

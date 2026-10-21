import classNames from 'classnames'
import type React from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'

import * as styles from './banner.css'

export type BannerVariant = 'error' | 'info' | 'success' | 'warning'

const ICONS: Record<BannerVariant, IconName> = {
  error: 'CircleAlert',
  info: 'Info',
  success: 'CircleCheck',
  warning: 'TriangleAlert',
}

export type BannerProps = Omit<React.ComponentPropsWithoutRef<'div'>, 'children' | 'title'> & {
  action?: React.ReactNode
  /** The message under the title. A title alone needs none. */
  children?: React.ReactNode
  title?: React.ReactNode
  variant?: BannerVariant
}

export function Banner({
  action,
  children,
  className,
  role,
  title,
  variant = 'info',
  ...props
}: BannerProps) {
  return (
    <div
      {...props}
      className={classNames(styles.banner({ variant, oneLine: !children }), className)}
      role={role ?? (variant === 'error' ? 'alert' : 'status')}
    >
      <Icon className={styles.icon} color="inherit" name={ICONS[variant]} size="18" />
      <div className={styles.content}>
        {title ? <span className={styles.title}>{title}</span> : null}
        {children ? <div className={styles.message}>{children}</div> : null}
      </div>
      {action ? <div className={styles.action}>{action}</div> : null}
    </div>
  )
}

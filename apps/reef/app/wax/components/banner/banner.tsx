import classNames from 'classnames'
import type React from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

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
  children: React.ReactNode
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
      className={classNames(styles.banner({ variant }), className)}
      role={role ?? (variant === 'error' ? 'alert' : 'note')}
    >
      <Icon className={styles.icon} color="inherit" name={ICONS[variant]} size="18" />
      <div className={styles.content}>
        {title ? <Typography.BodySmallStrong>{title}</Typography.BodySmallStrong> : null}
        <Typography.BodySmall as="div">{children}</Typography.BodySmall>
      </div>
      {action ? <div className={styles.action}>{action}</div> : null}
    </div>
  )
}

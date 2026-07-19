import type { ReactNode } from 'react'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './empty-page.css'

export function EmptyPage({
  action,
  description,
  fullSize = true,
  iconName,
  title,
}: {
  action?: ReactNode
  description: string
  fullSize?: boolean
  iconName: IconName
  title: string
}) {
  const content = (
    <div className={styles.container}>
      <Icon color="placeholder" name={iconName} size="30" />
      <div className={styles.typographyContainer}>
        <Typography.BodyStrong>{title}</Typography.BodyStrong>
        <Typography.Body variant="tertiary">{description}</Typography.Body>
      </div>
      {action}
    </div>
  )

  if (fullSize) {
    return <div className={styles.fullSizeWrapper}>{content}</div>
  }

  return content
}

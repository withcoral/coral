import type { ReactNode } from 'react'

import { Typography } from '@/wax/components/typography'

import * as styles from './setting-row.css'

/** Stacks SettingRows with one rule between each. */
export function List({ children }: { children: ReactNode }) {
  return <div className={styles.list}>{children}</div>
}

export interface RowProps {
  label: string
  /** The current value, shown beside the label. A pill or short text. */
  value?: ReactNode
  /** One sentence of consequence: what changing this does and does not do. */
  description?: ReactNode
  /** The control that changes it. */
  action?: ReactNode
}

/** One setting: what it is, what it is now, what changing it means, how to change it. */
export function Row({ label, value, description, action }: RowProps) {
  return (
    <div className={styles.row}>
      <div className={styles.text}>
        <div className={styles.label}>
          <Typography.BodyStrong as="span">{label}</Typography.BodyStrong>
          {value}
        </div>
        {description && <Typography.BodySmall as="p">{description}</Typography.BodySmall>}
      </div>
      {action && <div className={styles.action}>{action}</div>}
    </div>
  )
}

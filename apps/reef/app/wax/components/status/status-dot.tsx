import { Icon } from '@/wax/components/icon'

import * as styles from './status.css'

export interface StatusDotProps {
  /** Every reason the resource cannot be used, in priority order. */
  blockers: readonly string[]
  /**
   * What you must do with your own account before you use the resource, such as
   * connecting it. It is not a blocker: it shows only when nothing blocks.
   */
  personalAccountAction?: string | null
  ready?: string
}

const icons = { blocked: 'TriangleAlert', account: 'UserRoundKey', ready: 'CircleCheck' } as const

/**
 * The smallest readiness signal: for rows and tree nodes where a pill is too loud.
 * Each state has its own shape, so the signal does not depend on color alone.
 */
export function StatusDot({ blockers, personalAccountAction, ready = 'Ready' }: StatusDotProps) {
  const [first] = blockers
  // A blocker wins over your account: connecting it does not help while the
  // resource itself cannot be used.
  const tone = first ? 'blocked' : personalAccountAction ? 'account' : 'ready'
  const label = first ?? personalAccountAction ?? ready
  return (
    <span className={styles.dot({ tone })} role="img" aria-label={label} title={label}>
      <Icon name={icons[tone]} size="14" color="inherit" />
    </span>
  )
}

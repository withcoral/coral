import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import * as styles from './status.css'

export interface StatusProps {
  /** Every reason the resource cannot be used, in priority order. */
  blockers: readonly string[]
  /** The label shown when nothing blocks the resource. */
  ready?: string
  /** Lists remaining blockers under the pill instead of only in the title. */
  detail?: boolean
}

/**
 * One derived readiness pill. Ready is green; otherwise the first blocker is
 * amber and the rest are listed so nobody clears one blocker expecting access.
 */
export function Status({ blockers, ready = 'Ready', detail = false }: StatusProps) {
  const [first, ...rest] = blockers
  if (!first) return <Pill color="green">{ready}</Pill>
  // The title holds the first blocker too, since a narrow column truncates it.
  const title = rest.length ? `${first}. Also blocked: ${rest.join('; ')}` : first
  return (
    <span className={styles.root}>
      <span className={styles.pills}>
        <Pill color="amber" title={title}>
          {first}
        </Pill>
      </span>
      {detail && rest.length > 0 && (
        <Typography.BodySmall as="span">Also blocked: {rest.join('; ')}</Typography.BodySmall>
      )}
    </span>
  )
}

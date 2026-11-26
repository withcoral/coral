import type React from 'react'

import { Banner } from '../banner'
import { Icon } from '../icon'
import { Pill } from '../pill'
import type { PillSize } from '../pill'

/** Marks something only the Enterprise Edition enables. An icon and text carry the meaning, not colour. */
export function EnterpriseBadge({ size = 'small' }: { size?: PillSize }) {
  return (
    <Pill color="purple" size={size}>
      <Icon color="inherit" name="Gem" size="14" />
      Enterprise
    </Pill>
  )
}

export interface EnterpriseGateProps {
  /** Whether the feature works now. */
  enabled: boolean
  /** The feature's name, as people read it. */
  feature: string
  /** What to show while the feature works. */
  children: React.ReactNode
  /** What the reader can do to turn the feature on. */
  description?: React.ReactNode
  action?: React.ReactNode
}

/**
 * Shows `children` while the feature works. Otherwise it names the feature
 * and says it needs the Enterprise Edition, with the badge beside it. The
 * name stands alone as the title, so any feature name reads well.
 */
export function EnterpriseGate({
  action,
  children,
  description = 'Add an Enterprise license to turn it on.',
  enabled,
  feature,
}: EnterpriseGateProps) {
  if (enabled) return children
  return (
    <Banner
      action={
        <>
          <EnterpriseBadge />
          {action}
        </>
      }
      title={feature}
    >
      You need the Enterprise Edition for this feature. {description}
    </Banner>
  )
}

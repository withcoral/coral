import type { ReactNode } from 'react'
import { Fragment } from 'react'

import { Inputs, Table, Typography } from '@/wax/components'

import * as styles from './runtime-features-list.css'

export interface RuntimeFeatureListItem {
  readonly description: string
  /** Stable `[features]` key in `config.toml`. */
  readonly key: string
  /** Resolved state from config. This is what the next Coral start uses. */
  readonly enabled: boolean
  /** Human-readable feature name derived from the key. */
  readonly label: string
}

export interface RuntimeFeatureRowProps {
  /** Why the last write for this feature failed, if it did. */
  readonly error?: string
  readonly feature: RuntimeFeatureListItem
  readonly onToggle: (enabled: boolean) => void
  readonly pending?: boolean
  /** Whether this feature is somebody else's to change, leaving the row a view of it. */
  readonly readOnly?: boolean
}

export interface RuntimeFeaturesListProps {
  readonly error?: string
  readonly features: ReadonlyArray<RuntimeFeatureListItem>
  /** Renders one row. Each row writes on its own, so the caller owns the row. */
  readonly renderRow: (feature: RuntimeFeatureListItem) => ReactNode
}

// The switch column shows no label: the switch already reads as on or off, so all
// that is left is the name a reader hears.
const FEATURE_COLUMNS: Table.Column[] = [
  { label: 'Feature', width: 'fill' },
  { align: 'right', ariaLabel: 'Enabled', width: 96 },
]

export function RuntimeFeaturesList({ error, features, renderRow }: RuntimeFeaturesListProps) {
  const status = error ? (
    <Typography.BodySmall role="alert" variant="error">
      {error}
    </Typography.BodySmall>
  ) : features.length === 0 ? (
    <Typography.BodySmall variant="tertiary">
      This Coral build has no runtime features.
    </Typography.BodySmall>
  ) : null

  return (
    <Table.Container columns={FEATURE_COLUMNS} layout="fixed" variant="card">
      <Table.Head />
      <Table.Body>
        {status ? (
          <Table.Status>{status}</Table.Status>
        ) : (
          features.map((feature) => <Fragment key={feature.key}>{renderRow(feature)}</Fragment>)
        )}
      </Table.Body>
    </Table.Container>
  )
}

export function RuntimeFeatureRow({
  error,
  feature,
  onToggle,
  pending = false,
  readOnly = false,
}: RuntimeFeatureRowProps) {
  return (
    <Table.Row>
      <Table.Cell wrap>
        <div className={styles.feature}>
          <Typography.BodyStrong variant="primary">{feature.label}</Typography.BodyStrong>
          <Typography.BodySmall variant="secondary">{feature.description}</Typography.BodySmall>
          {/* Beside the switch that failed, so a reader never has to work out
              which of several rows an error belongs to. */}
          {error && (
            <Typography.BodySmall role="alert" variant="error">
              {error}
            </Typography.BodySmall>
          )}
        </div>
      </Table.Cell>
      <Table.Cell className={styles.enabledCell}>
        <Inputs.Switch
          aria-label={feature.label}
          checked={feature.enabled}
          disabled={pending || readOnly}
          onCheckedChange={onToggle}
        />
      </Table.Cell>
    </Table.Row>
  )
}

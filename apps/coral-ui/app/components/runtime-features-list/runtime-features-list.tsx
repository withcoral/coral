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
}

export interface RuntimeFeaturesListProps {
  readonly error?: string
  readonly features: ReadonlyArray<RuntimeFeatureListItem>
  /** Renders one row. Each row writes on its own, so the caller owns the row. */
  readonly renderRow: (feature: RuntimeFeatureListItem) => ReactNode
}

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
    <div className={styles.tableContainer}>
      <Table.Wrapper>
        <Table.Root className={styles.table}>
          <Table.Head>
            <Table.Row>
              <Table.HeaderCell>Feature</Table.HeaderCell>
              {/* The switch column shows no label: the switch already reads as on
                  or off. The name stays for screen readers. */}
              <Table.HeaderCell
                align="right"
                ariaLabel="Enabled"
                className={styles.enabledColumn}
              />
            </Table.Row>
          </Table.Head>
          <Table.Body>
            {status ? (
              <Table.Row>
                <td className={styles.statusCell} colSpan={2}>
                  {status}
                </td>
              </Table.Row>
            ) : (
              features.map((feature) => <Fragment key={feature.key}>{renderRow(feature)}</Fragment>)
            )}
          </Table.Body>
        </Table.Root>
      </Table.Wrapper>
    </div>
  )
}

export function RuntimeFeatureRow({
  error,
  feature,
  onToggle,
  pending = false,
}: RuntimeFeatureRowProps) {
  return (
    <Table.Row>
      <Table.Cell className={styles.featureCell}>
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
      <Table.Cell align="right" className={styles.enabledColumn}>
        <Inputs.Switch
          aria-label={feature.label}
          checked={feature.enabled}
          disabled={pending}
          onCheckedChange={onToggle}
        />
      </Table.Cell>
    </Table.Row>
  )
}

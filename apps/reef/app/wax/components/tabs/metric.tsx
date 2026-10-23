import { Tabs as BaseTabs } from '@base-ui/react/tabs'
import classNames from 'classnames'

import { Icon, type IconName } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './metric.css'

/** A row of metric tabs. Each tab is a number that opens its own panel. */
export function MetricList({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof BaseTabs.List>) {
  return <BaseTabs.List className={classNames(styles.metricList, className)} {...props} />
}

export interface MetricTabProps {
  value: string
  icon: IconName
  label: string
  /** The number, as you want it read. */
  metric: string
  /** Lines under the number, one fact each. */
  details?: readonly string[]
  variant?: 'success' | 'error' | 'tertiary'
}

/** A tab that shows a metric: its label, its value, and a line or two that explain it. */
export function MetricTab({ value, icon, label, metric, details = [], variant }: MetricTabProps) {
  return (
    <BaseTabs.Tab value={value} className={styles.metricTab}>
      <span className={styles.metricLabel}>
        <Icon name={icon} size="14" color="tertiary" />
        <Typography.BodySmall as="span" variant="tertiary">
          {label}
        </Typography.BodySmall>
      </span>
      <Typography.HeadingMedium as="span" variant={variant}>
        {metric}
      </Typography.HeadingMedium>
      {details.map((detail, index) => (
        <Typography.BodySmall
          // Two details can read the same.
          key={index}
          as="span"
          variant="tertiary"
          className={styles.metricDetail}
        >
          {detail}
        </Typography.BodySmall>
      ))}
    </BaseTabs.Tab>
  )
}

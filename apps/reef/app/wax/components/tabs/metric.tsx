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

/** A number and the lines under it. */
export interface MetricFigure {
  /** The number, as you want it read. */
  metric: string
  /** Lines under the number, one fact each. */
  details?: readonly string[]
}

export interface MetricTabProps extends MetricFigure {
  value: string
  icon: IconName
  label: string
  variant?: 'success' | 'error' | 'tertiary'
  /** Numbers beside the first, such as its parts. */
  parts?: readonly MetricFigure[]
}

function Figure({ metric, details = [], variant }: MetricFigure & Pick<MetricTabProps, 'variant'>) {
  return (
    <span className={styles.metricFigure}>
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
    </span>
  )
}

/** A tab that shows a metric: its label, its value, and a line or two that explain it. */
export function MetricTab({ value, icon, label, variant, parts = [], ...figure }: MetricTabProps) {
  return (
    <BaseTabs.Tab value={value} className={styles.metricTab}>
      <span className={styles.metricLabel}>
        <Icon name={icon} size="14" color="tertiary" />
        <Typography.BodySmall as="span" variant="tertiary">
          {label}
        </Typography.BodySmall>
      </span>
      <span className={styles.metricFigures}>
        <Figure {...figure} variant={variant} />
        {parts.map((part, index) => (
          // Two parts can read the same.
          <Figure key={index} {...part} />
        ))}
      </span>
    </BaseTabs.Tab>
  )
}

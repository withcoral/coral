import { Radio as BaseRadio } from '@base-ui/react/radio'
import classNames from 'classnames'
import { useId, type ReactNode } from 'react'
import { Typography } from '@/wax/components/typography'

import * as styles from './radio.css'

export type ItemProps<Value> = Omit<React.ComponentPropsWithoutRef<'label'>, 'value'> & {
  disabled?: boolean
  variant?: 'plain' | 'card'
  description?: ReactNode
  /** A control that belongs to this choice, on the right of the card, level with its copy. */
  action?: ReactNode
  /** Fields that belong to this choice, inside the card below its copy. */
  fields?: ReactNode
  value: Value
}

export function Item<Value>({
  children,
  className,
  disabled,
  value,
  variant = 'plain',
  description,
  action,
  fields,
  ...props
}: ItemProps<Value>) {
  const id = useId()
  if (variant === 'card' && (action || fields))
    return (
      <div className={classNames(styles.item, styles.card, styles.cardStack, className)}>
        <div className={styles.cardRow}>
          <label className={styles.cardLabel} {...props}>
            <BaseRadio.Root
              className={styles.control}
              disabled={disabled}
              value={value}
              aria-labelledby={`${id}-label`}
              aria-describedby={description ? `${id}-description` : undefined}
            >
              <BaseRadio.Indicator className={styles.indicator} />
            </BaseRadio.Root>
            <span className={classNames(styles.label, styles.copy)}>
              <span id={`${id}-label`}>{children}</span>
              {description && (
                <Typography.BodySmall as="span" id={`${id}-description`} variant="tertiary">
                  {description}
                </Typography.BodySmall>
              )}
            </span>
          </label>
          {action && <div className={styles.cardAside}>{action}</div>}
        </div>
        {fields && <div className={styles.cardAction}>{fields}</div>}
      </div>
    )
  return (
    <label
      className={classNames(styles.item, variant === 'card' && styles.card, className)}
      {...props}
    >
      <BaseRadio.Root
        className={styles.control}
        disabled={disabled}
        value={value}
        aria-labelledby={`${id}-label`}
        aria-describedby={description ? `${id}-description` : undefined}
      >
        <BaseRadio.Indicator className={styles.indicator} />
      </BaseRadio.Root>
      <span className={classNames(styles.label, styles.copy)}>
        <span id={`${id}-label`}>{children}</span>
        {description && (
          <Typography.BodySmall as="span" id={`${id}-description`} variant="tertiary">
            {description}
          </Typography.BodySmall>
        )}
      </span>
    </label>
  )
}

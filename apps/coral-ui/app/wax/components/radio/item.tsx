import { Radio as BaseRadio } from '@base-ui/react/radio'
import classNames from 'classnames'

import * as styles from './radio.css'

export type ItemProps<Value> = Omit<React.ComponentPropsWithoutRef<'label'>, 'value'> & {
  disabled?: boolean
  value: Value
}

export function Item<Value>({ children, className, disabled, value, ...props }: ItemProps<Value>) {
  return (
    <label className={classNames(styles.item, className)} {...props}>
      <BaseRadio.Root className={styles.control} disabled={disabled} value={value}>
        <BaseRadio.Indicator className={styles.indicator} />
      </BaseRadio.Root>
      <span className={styles.label}>{children}</span>
    </label>
  )
}

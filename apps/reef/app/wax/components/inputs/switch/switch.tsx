import { Switch as BaseSwitch } from '@base-ui/react/switch'
import classNames from 'classnames'

import * as styles from './switch.css'

export interface SwitchProps {
  'aria-label'?: string
  checked?: boolean
  className?: string
  defaultChecked?: boolean
  disabled?: boolean
  name?: string
  onCheckedChange?: (checked: boolean) => void
  ref?: React.Ref<HTMLButtonElement>
}

export function Switch({
  'aria-label': ariaLabel,
  checked,
  className,
  defaultChecked,
  disabled,
  name,
  onCheckedChange,
  ref,
}: SwitchProps) {
  return (
    <BaseSwitch.Root
      aria-label={ariaLabel}
      checked={checked}
      className={classNames(styles.root, className)}
      defaultChecked={defaultChecked}
      disabled={disabled}
      name={name}
      onCheckedChange={onCheckedChange}
      ref={ref}
    >
      <BaseSwitch.Thumb className={styles.thumb} />
    </BaseSwitch.Root>
  )
}

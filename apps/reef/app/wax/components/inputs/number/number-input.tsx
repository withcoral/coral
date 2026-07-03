import { NumberField as BaseNumberField } from '@base-ui/react/number-field'
import classNames from 'classnames'

import * as styles from '@/wax/components/inputs/base-input.css'

export interface NumberInputProps {
  className?: string
  disabled?: boolean
  max?: number
  min?: number
  name?: string
  onBlur?: () => void
  onChange?: (value: null | number) => void
  placeholder?: string
  ref?: React.Ref<HTMLInputElement>
  step?: number
  value?: null | number
}

export function NumberInput({
  className,
  disabled,
  max,
  min,
  name,
  onBlur,
  onChange,
  placeholder,
  ref,
  step = 1,
  value,
}: NumberInputProps) {
  return (
    <BaseNumberField.Root
      disabled={disabled}
      max={max}
      min={min}
      onValueChange={onChange}
      step={step}
      value={value}
    >
      <BaseNumberField.Input
        className={classNames(styles.input, className)}
        name={name}
        onBlur={onBlur}
        placeholder={placeholder}
        ref={ref}
      />
    </BaseNumberField.Root>
  )
}

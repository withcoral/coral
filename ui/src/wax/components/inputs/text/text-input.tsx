import { Field } from '@base-ui-components/react/field'
import classNames from 'classnames'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import * as styles from '@/wax/components/inputs/base-input.css'

export interface TextInputProps {
  autoFocus?: boolean
  className?: string
  disabled?: boolean
  icon?: IconName
  name?: string
  onBlur?: () => void
  onChange?: (value: string) => void
  onKeyDown?: (e: React.KeyboardEvent<HTMLInputElement>) => void
  placeholder?: string
  ref?: React.Ref<HTMLInputElement>
  type?: 'email' | 'password' | 'search' | 'tel' | 'text' | 'url'
  value?: string
}

export function TextInput({
  autoFocus,
  className,
  disabled,
  icon,
  name,
  onBlur,
  onChange,
  onKeyDown,
  placeholder,
  ref,
  type = 'text',
  value,
}: TextInputProps) {
  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange?.(event.target.value)
  }

  return (
    <Field.Root disabled={disabled}>
      <div className={styles.container}>
        {icon && (
          <Icon className={styles.iconWrapper} color={disabled ? 'disabled' : 'placeholder'} name={icon} size="20" />
        )}
        <Field.Control
          autoFocus={autoFocus}
          className={classNames(styles.input, { [styles.inputWithIcon]: !!icon }, className)}
          name={name}
          onBlur={onBlur}
          onChange={handleChange}
          onKeyDown={onKeyDown}
          placeholder={placeholder}
          ref={ref}
          type={type}
          value={value}
        />
      </div>
    </Field.Root>
  )
}

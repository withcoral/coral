import { Field } from '@base-ui/react/field'
import classNames from 'classnames'

import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import * as styles from '@/wax/components/inputs/base-input.css'

export interface TextInputProps {
  ariaLabel?: string
  autoFocus?: boolean
  className?: string
  disabled?: boolean
  icon?: IconName
  id?: string
  invalid?: boolean
  name?: string
  onBlur?: () => void
  onChange?: (value: string) => void
  onFocus?: () => void
  onKeyDown?: (e: React.KeyboardEvent<HTMLInputElement>) => void
  placeholder?: string
  readOnly?: boolean
  ref?: React.Ref<HTMLInputElement>
  type?: 'email' | 'password' | 'search' | 'tel' | 'text' | 'url'
  value?: string
}

export function TextInput({
  ariaLabel,
  autoFocus,
  className,
  disabled,
  icon,
  id,
  invalid,
  name,
  onBlur,
  onChange,
  onFocus,
  onKeyDown,
  placeholder,
  readOnly,
  ref,
  type = 'text',
  value,
}: TextInputProps) {
  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange?.(event.target.value)
  }

  return (
    <Field.Root disabled={disabled} invalid={invalid}>
      <div className={styles.container}>
        {icon && (
          <Icon
            className={styles.iconWrapper}
            color={disabled ? 'disabled' : 'placeholder'}
            name={icon}
            size="20"
          />
        )}
        <Field.Control
          aria-label={ariaLabel}
          autoFocus={autoFocus}
          className={classNames(styles.input, { [styles.inputWithIcon]: !!icon }, className)}
          id={id}
          name={name}
          onBlur={onBlur}
          onChange={handleChange}
          onFocus={onFocus}
          onKeyDown={onKeyDown}
          placeholder={placeholder}
          readOnly={readOnly}
          ref={ref}
          type={type}
          value={value}
        />
      </div>
    </Field.Root>
  )
}

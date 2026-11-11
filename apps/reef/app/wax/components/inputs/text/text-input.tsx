import { Field } from '@base-ui/react/field'
import classNames from 'classnames'
import { useId, useRef } from 'react'

import { useComposedRefs } from '@/utils/compose-refs'
import { IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import * as styles from '@/wax/components/inputs/base-input.css'

export interface TextInputProps {
  ariaLabel?: string
  ariaDescribedBy?: string
  /**
   * Off for a name the browser should not suggest, such as a new identifier.
   * Off also keeps password managers out of the field.
   */
  autoComplete?: string
  autoFocus?: boolean
  /** Shows no border until hovered or focused, for an input that sits in a table cell. */
  bare?: boolean
  className?: string
  defaultValue?: string
  disabled?: boolean
  icon?: IconName
  id?: string
  invalid?: boolean
  name?: string
  maxLength?: number
  onBlur?: () => void
  onChange?: (value: string) => void
  onFocus?: () => void
  onKeyDown?: (e: React.KeyboardEvent<HTMLInputElement>) => void
  /** A regular expression the whole value must match, checked by the browser on submit. */
  pattern?: string
  placeholder?: string
  /**
   * Fixed text inside the field, before the value, such as the start of a path
   * that the reader cannot change. It is not part of the value.
   */
  prefix?: string
  readOnly?: boolean
  ref?: React.Ref<HTMLInputElement>
  required?: boolean
  spellCheck?: boolean
  step?: number | string
  /** A controlled `search` input shows a clear button while it has text. */
  type?: 'datetime-local' | 'email' | 'password' | 'search' | 'tel' | 'text' | 'url'
  value?: string
}

export function TextInput({
  ariaLabel,
  ariaDescribedBy,
  autoComplete,
  autoFocus,
  bare = false,
  className,
  defaultValue,
  disabled,
  icon,
  id,
  invalid,
  maxLength,
  name,
  onBlur,
  onChange,
  onFocus,
  onKeyDown,
  pattern,
  placeholder,
  prefix,
  readOnly,
  ref,
  required,
  spellCheck,
  step,
  type = 'text',
  value,
}: TextInputProps) {
  const input = useRef<HTMLInputElement>(null)
  const inputRef = useComposedRefs(ref, input)
  const prefixId = useId()
  // The prefix comes first, as it does in the field.
  const describedBy = prefix
    ? [prefixId, ariaDescribedBy].filter(Boolean).join(' ')
    : ariaDescribedBy
  const clearable = type === 'search' && !!value && !!onChange && !disabled && !readOnly
  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange?.(event.target.value)
  }
  return (
    <Field.Root disabled={disabled} invalid={invalid}>
      <div className={classNames(styles.container, prefix && styles.prefixedContainer)}>
        {prefix && (
          // A press on the prefix puts the caret in the field, as a press on its border does.
          // It describes the field, so a screen reader says it with the field's name.
          <span
            className={styles.prefix}
            id={prefixId}
            onMouseDown={(event) => {
              event.preventDefault()
              input.current?.focus()
            }}
          >
            {prefix}
          </span>
        )}
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
          aria-describedby={describedBy}
          autoComplete={autoComplete}
          // Password managers ignore autocomplete="off", so they each need their own opt-out.
          data-1p-ignore={autoComplete === 'off' || undefined}
          data-lpignore={autoComplete === 'off' ? 'true' : undefined}
          data-bwignore={autoComplete === 'off' || undefined}
          autoFocus={autoFocus}
          className={classNames(
            styles.input,
            {
              [styles.inputWithIcon]: !!icon,
              [styles.inputWithClear]: clearable,
              [styles.bareInput]: bare,
              [styles.prefixedInput]: !!prefix,
            },
            className,
          )}
          defaultValue={defaultValue}
          id={id}
          maxLength={maxLength}
          name={name}
          onBlur={onBlur}
          onChange={handleChange}
          onFocus={onFocus}
          onKeyDown={onKeyDown}
          pattern={pattern}
          placeholder={placeholder}
          readOnly={readOnly}
          ref={inputRef}
          required={required}
          spellCheck={spellCheck}
          step={step}
          type={type}
          value={value}
        />
        {clearable && (
          <IconButton
            ariaLabel="Clear search"
            className={styles.clearButton}
            name="X"
            size="22"
            variant="bare"
            onClick={() => {
              onChange('')
              // The button goes away, so focus goes back to the input.
              input.current?.focus()
            }}
          />
        )}
      </div>
    </Field.Root>
  )
}

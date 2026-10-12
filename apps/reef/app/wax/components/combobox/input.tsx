import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'
import { FocusEventHandler, KeyboardEventHandler, RefObject } from 'react'

import * as styles from './combobox.css'

export interface InputProps {
  ariaDescribedBy?: string
  bare?: boolean
  className?: string
  id?: string
  invalid?: boolean
  maxLength?: number
  onBlur?: FocusEventHandler<HTMLInputElement>
  onKeyDown?: KeyboardEventHandler<HTMLInputElement>
  placeholder?: string
  ref?: RefObject<HTMLInputElement | null>
  /** Announces the field as required without blocking the form's own submission. */
  required?: boolean
}

export function Input({
  ariaDescribedBy,
  bare = false,
  className,
  id,
  invalid,
  maxLength,
  onBlur,
  onKeyDown,
  placeholder,
  ref,
  required,
}: InputProps) {
  return (
    <BaseCombobox.Input
      aria-describedby={ariaDescribedBy}
      aria-invalid={invalid || undefined}
      aria-required={required || undefined}
      className={classNames(bare ? styles.inputBare : styles.input, className)}
      id={id}
      maxLength={maxLength}
      onBlur={onBlur}
      onKeyDown={onKeyDown}
      placeholder={placeholder}
      ref={ref}
    />
  )
}

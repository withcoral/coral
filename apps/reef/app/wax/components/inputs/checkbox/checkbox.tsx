import { useEffect, useRef, type ReactNode } from 'react'

import * as styles from './checkbox.css'

export interface CheckboxProps {
  children: ReactNode
  checked?: boolean
  defaultChecked?: boolean
  disabled?: boolean
  indeterminate?: boolean
  name?: string
  onCheckedChange?: (checked: boolean) => void
  /** Submitted with the form when checked. */
  value?: string
}

/** One labelled native checkbox. Several with the same name make a checklist the form submits. */
export function Checkbox({
  children,
  checked,
  defaultChecked,
  disabled,
  indeterminate = false,
  name,
  onCheckedChange,
  value,
}: CheckboxProps) {
  const ref = useRef<HTMLInputElement>(null)
  useEffect(() => {
    if (ref.current) ref.current.indeterminate = indeterminate
  }, [indeterminate])
  return (
    <label className={styles.label}>
      <input
        type="checkbox"
        ref={ref}
        className={styles.input}
        checked={checked}
        defaultChecked={defaultChecked}
        disabled={disabled}
        name={name}
        value={value}
        onChange={(event) => onCheckedChange?.(event.target.checked)}
      />
      <span>{children}</span>
    </label>
  )
}

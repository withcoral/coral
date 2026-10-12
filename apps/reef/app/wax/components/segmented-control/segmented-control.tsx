import { useRef, type KeyboardEvent } from 'react'

import { Icon, type IconName } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'

import * as styles from './segmented-control.css'

export interface SegmentedOption<Value extends string> {
  value: Value
  /** The segment's text, or its accessible name and tooltip when it has an icon. */
  label: string
  /** Shows the icon in place of the label. */
  icon?: IconName
  /** Says what choosing an icon segment does, under its label in the tooltip. */
  description?: string
  disabled?: boolean
}

export interface SegmentedControlProps<Value extends string> {
  /** The accessible name of the group. */
  label: string
  options: readonly SegmentedOption<Value>[]
  value: Value
  onChange: (value: Value) => void
  /** Submits the value as a hidden form field. */
  name?: string
  disabled?: boolean
  /** `small` fits a compact table row. */
  size?: 'default' | 'small'
}

/**
 * One choice among a few, all visible. It behaves as a radio group: arrow
 * keys move and select, Tab leaves the group.
 */
export function SegmentedControl<Value extends string>({
  label,
  options,
  value,
  onChange,
  name,
  disabled,
  size = 'default',
}: SegmentedControlProps<Value>) {
  const buttons = useRef(new Map<Value, HTMLButtonElement>())
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const enabled = options.filter((option) => !option.disabled && !disabled)
    const index = enabled.findIndex((option) => option.value === value)
    if (index < 0) return
    let next: SegmentedOption<Value> | undefined
    switch (event.key) {
      case 'ArrowRight':
      case 'ArrowDown':
        next = enabled[(index + 1) % enabled.length]
        break
      case 'ArrowLeft':
      case 'ArrowUp':
        next = enabled[(index - 1 + enabled.length) % enabled.length]
        break
      case 'Home':
        next = enabled[0]
        break
      case 'End':
        next = enabled.at(-1)
        break
      default:
        return
    }
    event.preventDefault()
    if (next) {
      select(next.value)
      buttons.current.get(next.value)?.focus()
    }
  }
  // A radio that is already checked does not change.
  const select = (next: Value) => {
    if (next !== value) onChange(next)
  }
  return (
    <div role="radiogroup" aria-label={label} className={styles.group} onKeyDown={onKeyDown}>
      {name && <input type="hidden" name={name} value={value} />}
      {options.map((option) => {
        const checked = option.value === value
        const segment = (
          <button
            key={option.value}
            type="button"
            role="radio"
            aria-checked={checked}
            aria-label={option.icon ? option.label : undefined}
            aria-description={option.icon ? option.description : undefined}
            tabIndex={checked ? 0 : -1}
            disabled={disabled || option.disabled}
            className={styles.segment({ checked, icon: Boolean(option.icon), size })}
            onClick={() => select(option.value)}
            ref={(element) => {
              if (element) buttons.current.set(option.value, element)
              else buttons.current.delete(option.value)
            }}
          >
            {option.icon ? (
              <Icon
                name={option.icon}
                className={styles.icon}
                color="inherit"
                size={size === 'small' ? '16' : '18'}
              />
            ) : (
              option.label
            )}
          </button>
        )
        return option.icon ? (
          <Tooltip
            key={option.value}
            maxWidth={260}
            content={
              option.description ? (
                <>
                  <span className={styles.tooltipLabel}>{option.label}</span>
                  <span className={styles.tooltipDescription}>{option.description}</span>
                </>
              ) : (
                option.label
              )
            }
          >
            {segment}
          </Tooltip>
        ) : (
          segment
        )
      })}
    </div>
  )
}

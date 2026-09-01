import { useId } from 'react'

import { Button, Menu, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import type { IconColor, IconName } from '@/wax/components/icon'

import * as styles from './picker.css'

export interface PickerOption {
  icon?: { color: IconColor; name: IconName }
  label: string
  value: string
}

export interface PickerProps {
  disabled?: boolean
  fullWidth?: boolean
  label?: string
  /**
   * `inside` heads the option list with the label. `outside` stacks it over
   * the trigger, so a row of pickers reads as a labelled form.
   */
  labelPlacement?: 'inside' | 'outside'
  onChange?: (value: string) => void
  options: Omit<PickerOption, 'icon'>[] | Required<PickerOption>[]
  value: string
}

export function Picker({
  disabled = false,
  fullWidth = false,
  label,
  labelPlacement = 'inside',
  onChange,
  options,
  value,
}: PickerProps) {
  const labelId = useId()
  const selectedOption = options.find((opt) => opt.value === value) as PickerOption | undefined
  const displayLabel = selectedOption?.label ?? value
  const selectedIcon = selectedOption && hasIcon(selectedOption) ? selectedOption.icon : undefined
  const labelOutside = label !== undefined && labelPlacement === 'outside'

  const picker = (
    <Menu.Container>
      <Menu.Trigger
        className={styles.trigger}
        render={
          <Button.Container
            {...(labelOutside && { 'aria-labelledby': labelId })}
            disabled={disabled}
            fullWidth={fullWidth}
            variant="secondary"
          />
        }
      >
        {selectedIcon && <Icon color={selectedIcon.color} name={selectedIcon.name} size="16" />}
        <Button.Text>{displayLabel}</Button.Text>
        <Button.Icon name="ChevronDown" />
      </Menu.Trigger>
      <Menu.Content className={styles.menu}>
        {label && !labelOutside && (
          <div className={styles.label}>
            <Typography.BodySmallStrong variant="tertiary">{label}</Typography.BodySmallStrong>
          </div>
        )}
        <Menu.RadioGroup onValueChange={onChange} value={value}>
          {options.map((option) => (
            <Menu.RadioItem
              key={option.value}
              value={option.value}
              {...(hasIcon(option) && { iconColor: option.icon.color, iconName: option.icon.name })}
            >
              {option.label}
            </Menu.RadioItem>
          ))}
        </Menu.RadioGroup>
      </Menu.Content>
    </Menu.Container>
  )

  if (!labelOutside) return picker

  return (
    <div className={styles.field}>
      <Typography.BodySmallStrong id={labelId} variant="tertiary">
        {label}
      </Typography.BodySmallStrong>
      {picker}
    </div>
  )
}

function hasIcon(option: PickerOption): option is Required<PickerOption> {
  return 'icon' in option
}

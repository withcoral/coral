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
  label?: string
  onChange?: (value: string) => void
  options: Omit<PickerOption, 'icon'>[] | Required<PickerOption>[]
  value: string
}

export function Picker({ label, onChange, options, value }: PickerProps) {
  const selectedOption = options.find((opt) => opt.value === value) as PickerOption | undefined
  const displayLabel = selectedOption?.label ?? value
  const selectedIcon = selectedOption && hasIcon(selectedOption) ? selectedOption.icon : undefined

  return (
    <Menu.Container>
      <Menu.Trigger render={<Button.Container variant="secondary" />}>
        {selectedIcon && <Icon color={selectedIcon.color} name={selectedIcon.name} size="16" />}
        <Button.Text>{displayLabel}</Button.Text>
      </Menu.Trigger>
      <Menu.Content>
        {label && (
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
}

function hasIcon(option: PickerOption): option is Required<PickerOption> {
  return 'icon' in option
}

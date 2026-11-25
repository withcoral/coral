import classNames from 'classnames'
import { Button, Menu, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import type { IconColor, IconName } from '@/wax/components/icon'

import * as styles from './picker.css'

export interface PickerOption {
  /** Consecutive options with the same group sit under one heading. */
  group?: string
  icon?: { color: IconColor; name: IconName }
  label: string
  value: string
}

type IconOption = PickerOption & Required<Pick<PickerOption, 'icon'>>

export interface PickerProps {
  /** Names the control for assistive technology when no visible label does. */
  ariaLabel?: string
  /** The id of the text that explains the value, such as its error. */
  ariaDescribedBy?: string
  disabled?: boolean
  /** The trigger's id, for a page that moves focus to it. */
  id?: string
  /**
   * Takes its container's width, its label at the start and the chevron at the end.
   * It is never narrower than its longest option, so a column of them keeps one
   * width whatever each one picks.
   */
  fill?: boolean
  /** Draws the error border and marks the value invalid. */
  invalid?: boolean
  label?: string
  onChange?: (value: string) => void
  options: Omit<PickerOption, 'icon'>[] | IconOption[]
  /** Shown on the trigger while no option matches the value. */
  placeholder?: string
  value: string
}

export function Picker({
  ariaLabel,
  ariaDescribedBy,
  disabled = false,
  id,
  invalid = false,
  fill = false,
  label,
  onChange,
  options,
  placeholder,
  value,
}: PickerProps) {
  const selectedOption = options.find((opt) => opt.value === value) as PickerOption | undefined
  const displayLabel = selectedOption?.label ?? placeholder ?? value
  const selectedIcon = selectedOption && hasIcon(selectedOption) ? selectedOption.icon : undefined

  return (
    <Menu.Container>
      <Menu.Trigger
        disabled={disabled}
        id={id}
        render={
          // Menu.Trigger forwards only its own props, so ARIA state goes on the button.
          <Button.Container
            variant="secondary"
            ariaLabel={ariaLabel}
            aria-invalid={invalid || undefined}
            aria-describedby={ariaDescribedBy}
            className={classNames(invalid && styles.invalid, fill && styles.fill)}
          />
        }
      >
        {selectedIcon && <Icon color={selectedIcon.color} name={selectedIcon.name} size="16" />}
        <Button.Text>
          {displayLabel}
          {fill &&
            // Each option's label takes its width in no height, so the trigger keeps its
            // longest option's width and a new choice does not move what sits beside it.
            options.map((option) => (
              <span
                key={option.value}
                aria-hidden
                className={styles.widthOf}
                data-label={option.label}
              />
            ))}
        </Button.Text>
        <Button.Icon name="ChevronDown" />
      </Menu.Trigger>
      <Menu.Content className={styles.popup}>
        {label && (
          <div className={styles.label}>
            <Typography.BodySmallStrong variant="tertiary">{label}</Typography.BodySmallStrong>
          </div>
        )}
        <Menu.RadioGroup onValueChange={onChange} value={value}>
          {groupsOf(options).map(({ group, members }) =>
            group === undefined ? (
              members.map(radioItem)
            ) : (
              <Menu.Group key={group}>
                <Menu.GroupLabel>{group}</Menu.GroupLabel>
                {members.map(radioItem)}
              </Menu.Group>
            ),
          )}
        </Menu.RadioGroup>
      </Menu.Content>
    </Menu.Container>
  )
}

function radioItem(option: PickerOption) {
  return (
    <Menu.RadioItem
      key={option.value}
      value={option.value}
      {...(hasIcon(option) && { iconColor: option.icon.color, iconName: option.icon.name })}
    >
      {option.label}
    </Menu.RadioItem>
  )
}

function groupsOf(options: PickerOption[]) {
  const groups: { group?: string; members: PickerOption[] }[] = []
  for (const option of options) {
    const last = groups.at(-1)
    if (last && last.group === option.group) last.members.push(option)
    else groups.push({ group: option.group, members: [option] })
  }
  return groups
}

function hasIcon(option: PickerOption): option is IconOption {
  return 'icon' in option
}

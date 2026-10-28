import type { IconName } from '@/wax/components/icon'
import { SegmentedControl, type SegmentedControlProps } from '@/wax/components/segmented-control'

export type PolicyState = 'enabled' | 'approval_required' | 'disabled'

/** Every policy control names and draws the three states the same way. */
export const policyOptions: readonly {
  value: PolicyState
  label: string
  icon: IconName
  description?: string
}[] = [
  { value: 'enabled', label: 'Always allow', icon: 'CircleCheck' },
  {
    value: 'approval_required',
    label: 'Needs approval',
    icon: 'Hand',
    description: 'Coral asks before each use.',
  },
  {
    value: 'disabled',
    label: 'Disabled',
    icon: 'Ban',
    description: 'Coral blocks every call. Search results do not show this tool.',
  },
]

export const policyLabels = Object.fromEntries(
  policyOptions.map(({ value, label }) => [value, label]),
) as Record<PolicyState, string>

export interface PolicyToggleProps {
  /** The accessible name of the group. */
  label: string
  value: PolicyState
  onChange?: (value: PolicyState) => void
  disabled?: boolean
  size?: SegmentedControlProps<PolicyState>['size']
}

/** One policy, chosen from three icon segments. */
export function PolicyToggle({ label, value, onChange, disabled, size }: PolicyToggleProps) {
  return (
    <SegmentedControl
      label={label}
      options={policyOptions}
      value={value}
      onChange={(next) => onChange?.(next)}
      disabled={disabled}
      size={size}
    />
  )
}

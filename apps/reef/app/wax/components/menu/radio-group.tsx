import { Menu as BaseMenu } from '@base-ui/react/menu'

export interface RadioGroupProps<T extends string> {
  /** Names the group when it has no `GroupLabel` to show. */
  ariaLabel?: string
  children: React.ReactNode
  onValueChange?: (value: T) => void
  value?: T
}

export function RadioGroup<T extends string>({
  ariaLabel,
  children,
  onValueChange,
  value,
}: RadioGroupProps<T>) {
  return (
    <BaseMenu.RadioGroup
      aria-label={ariaLabel}
      onValueChange={onValueChange as (value: string) => void}
      value={value}
    >
      {children}
    </BaseMenu.RadioGroup>
  )
}

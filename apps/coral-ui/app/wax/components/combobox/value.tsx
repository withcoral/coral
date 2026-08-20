import { Combobox as BaseCombobox } from '@base-ui/react/combobox'

export interface ValueProps {
  children?: ((selectedValue: unknown) => React.ReactNode) | React.ReactNode
}

export function Value({ children }: ValueProps) {
  return <BaseCombobox.Value>{children}</BaseCombobox.Value>
}

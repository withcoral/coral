import { Combobox as BaseCombobox } from '@base-ui/react/combobox'

export interface CollectionProps<Item = string> {
  children: (item: Item, index: number) => React.ReactNode
}

export function Collection<Item = string>({ children }: CollectionProps<Item>) {
  return <BaseCombobox.Collection>{children}</BaseCombobox.Collection>
}

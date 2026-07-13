import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import { useState } from 'react'

import { AnchorContext } from './anchor-context'

export interface RootProps {
  children: React.ReactNode
  defaultValue?: null | string | string[]
  disabled?: boolean
  items?: string[]
  multiple?: boolean
  onValueChange?: (value: null | string | string[]) => void
  value?: null | string | string[]
}

export function Root({
  children,
  defaultValue,
  disabled,
  items,
  multiple,
  onValueChange,
  value,
}: RootProps) {
  const filter = BaseCombobox.useFilter({ sensitivity: 'base' })
  const [inputGroupAnchor, setInputGroupAnchor] = useState<HTMLDivElement | null>(null)

  return (
    <AnchorContext.Provider value={{ inputGroupAnchor, setInputGroupAnchor }}>
      <BaseCombobox.Root
        defaultValue={defaultValue}
        disabled={disabled}
        filter={filter.contains}
        items={items}
        multiple={multiple}
        onValueChange={onValueChange}
        value={value ?? undefined}
      >
        {children}
      </BaseCombobox.Root>
    </AnchorContext.Provider>
  )
}

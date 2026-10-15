import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import { useState } from 'react'

import { AnchorContext } from './anchor-context'

export interface RootProps {
  /** Highlight the first match as you type, so Enter picks it. */
  autoHighlight?: boolean
  children: React.ReactNode
  defaultValue?: null | string | string[]
  disabled?: boolean
  /**
   * Keep the popup closed while no item matches the text. An open popup hides
   * the rest of the page from assistive technology, so a list with nothing to
   * offer must not hold it open. Leave it off to show `Empty` instead.
   */
  hideWhenEmpty?: boolean
  /**
   * The text in the input. Control it to keep text the list does not offer;
   * the combobox otherwise replaces it with the selected item when it closes.
   */
  inputValue?: string
  /** The text an item shows and matches the query on. Omit it when each item is its own text. */
  itemToStringLabel?: (item: string) => string
  items?: string[]
  multiple?: boolean
  onInputValueChange?: (value: string) => void
  onValueChange?: (value: null | string | string[]) => void
  value?: null | string | string[]
}

export function Root({
  autoHighlight,
  children,
  defaultValue,
  disabled,
  hideWhenEmpty = false,
  inputValue,
  itemToStringLabel,
  items,
  multiple,
  onInputValueChange,
  onValueChange,
  value,
}: RootProps) {
  const filter = BaseCombobox.useFilter({ sensitivity: 'base' })
  const [inputGroupAnchor, setInputGroupAnchor] = useState<HTMLDivElement | null>(null)
  const [open, setOpen] = useState(false)
  // The text the person typed, unless the caller controls it. Passing it back
  // keeps Base UI from clearing it on its own; see onInputValueChange.
  const [typed, setTyped] = useState('')
  const query = inputValue ?? typed
  const matches = (items ?? []).some((item) => filter.contains(item, query, itemToStringLabel))

  return (
    <AnchorContext.Provider value={{ inputGroupAnchor, setInputGroupAnchor }}>
      <BaseCombobox.Root
        autoHighlight={autoHighlight}
        defaultValue={defaultValue}
        disabled={disabled}
        filter={filter.contains}
        inputValue={query}
        itemToStringLabel={itemToStringLabel}
        items={items}
        multiple={multiple}
        open={hideWhenEmpty ? open && matches : undefined}
        onOpenChange={hideWhenEmpty ? setOpen : undefined}
        onInputValueChange={(next, details) => {
          // A multiple combobox clears the typed text once its popup closes.
          // Hiding an empty list is not the person closing it, so keep the text.
          if (hideWhenEmpty && open && !matches && details.reason === 'input-clear') return
          setTyped(next)
          onInputValueChange?.(next)
        }}
        onValueChange={onValueChange}
        // null selects nothing and keeps the combobox controlled; undefined leaves
        // the caller's selection to the combobox itself.
        value={value}
      >
        {children}
      </BaseCombobox.Root>
    </AnchorContext.Provider>
  )
}

import { useState } from 'react'

import * as Combobox from '@/wax/components/combobox'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { Tooltip } from '@/wax/components/tooltip'

import * as styles from './token-input.css'

/** A space or a comma ends a token; people never type the separator into one. */
const SEPARATOR = /[\s,]+/

export interface TokenInputProps {
  ariaDescribedBy?: string
  id?: string
  invalid?: boolean
  onChange: (value: string[]) => void
  placeholder?: string
  /** The accessible name of the button that removes a token. */
  removeLabel?: (token: string) => string
  /** Tokens offered as the person types; chosen ones leave the list. */
  suggestions?: string[]
  /**
   * The text a chip shows for its token. When it differs from the token, or
   * the chip cuts it off, a tooltip shows the whole token.
   */
  tokenLabel?: (token: string) => string
  value: string[]
}

/**
 * Free-form tokens shown as removable chips. Space, comma, Enter and leaving the
 * field turn the typed text into a token; a pasted list splits into tokens.
 */
export function TokenInput({
  ariaDescribedBy,
  id,
  invalid,
  onChange,
  placeholder,
  removeLabel = (token) => `Remove ${token}`,
  suggestions = [],
  tokenLabel = (token) => token,
  value,
}: TokenInputProps) {
  const [text, setText] = useState('')
  const add = (tokens: string[]) => {
    const next = [...value]
    for (const token of tokens) if (token && !next.includes(token)) next.push(token)
    if (next.length !== value.length) onChange(next)
  }
  const type = (input: string) => {
    const parts = input.split(SEPARATOR)
    const rest = parts.pop() ?? ''
    add(parts)
    setText(rest)
  }
  const commit = () => {
    add([text.trim()])
    setText('')
  }

  return (
    <Combobox.Root
      hideWhenEmpty
      items={suggestions.filter((suggestion) => !value.includes(suggestion))}
      multiple
      value={value}
      inputValue={text}
      onInputValueChange={type}
      onValueChange={(next) => {
        if (!Array.isArray(next)) return
        const unique = [...new Set(next)]
        // A token added from the list replaces whatever was typed to find it.
        if (unique.length > value.length) setText('')
        onChange(unique)
      }}
    >
      <Combobox.InputGroup className={invalid ? styles.invalid : undefined}>
        {/* Many tokens scroll inside the field, so a long list does not push the
            rest of the form down. */}
        <ScrollArea maxHeight="144px" scrollDirection="vertical" constrainWidth>
          <Combobox.Chips>
            {value.map((token) => {
              const label = tokenLabel(token)
              return (
                <Combobox.Chip key={token}>
                  <Tooltip content={token} showOnlyWhenTruncated={label === token}>
                    <Combobox.ChipLabel>{label}</Combobox.ChipLabel>
                  </Tooltip>
                  <Combobox.ChipRemove aria-label={removeLabel(token)} />
                </Combobox.Chip>
              )
            })}
            <Combobox.Input
              bare
              id={id}
              invalid={invalid}
              ariaDescribedBy={ariaDescribedBy}
              placeholder={value.length > 0 ? '' : placeholder}
              onBlur={commit}
              onKeyDown={(event) => {
                // Tab closes an open list, and the combobox clears the text as it
                // closes, before the blur that would add it. Add it first.
                if (event.key === 'Tab') {
                  commit()
                  return
                }
                // A highlighted suggestion is the combobox's to choose.
                if (
                  event.key !== 'Enter' ||
                  !text.trim() ||
                  event.currentTarget.getAttribute('aria-activedescendant')
                )
                  return
                // Enter adds the token rather than submitting the form around it.
                event.preventDefault()
                commit()
              }}
            />
          </Combobox.Chips>
        </ScrollArea>
      </Combobox.InputGroup>
      <Combobox.Content>
        <Combobox.List>
          {(suggestion: string) => (
            <Combobox.Item key={suggestion} value={suggestion}>
              {suggestion}
            </Combobox.Item>
          )}
        </Combobox.List>
      </Combobox.Content>
    </Combobox.Root>
  )
}

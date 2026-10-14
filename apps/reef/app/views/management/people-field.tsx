import { useId, useState } from 'react'

import { Combobox, Typography } from '@/wax/components'

import { personName } from './person'
import type { PersonView } from './types'
import * as styles from './management.css'

/** One person you can pick, and the form value that names them. */
export interface PickablePerson {
  value: string
  person: PersonView
}

/**
 * Picks people by name or email. The form gets one `name` field per pick,
 * holding its value. Values compare without case, as emails do.
 */
export function PeopleField({
  name,
  people,
  currentUser,
  placeholder,
  value,
  onChange,
  acceptTyped,
}: {
  /** The form field name. */
  name: string
  people: PickablePerson[]
  currentUser: string
  placeholder: string
  value: string[]
  onChange: (values: string[]) => void
  /** Turns typed text that the list does not offer into a value, or null. Omit it to offer only the list. */
  acceptTyped?: (text: string) => string | null
}) {
  const id = useId()
  const [query, setQuery] = useState('')
  // Each picked item, its value and its chip. Values are not unique, so a person
  // who later shares one must not take over the chip; and a chip keeps its value
  // and its name after its person leaves the list.
  const [picked, setPicked] = useState(new Map<string, { value: string; chip: string }>())
  // The combobox filters on its item strings, so each holds the name and email.
  const label = (p: PersonView) =>
    p.name && p.email ? `${personName(p, currentUser)} (${p.email})` : personName(p, currentUser)
  const byLabel = new Map(people.map((p) => [label(p.person), p]))
  const byValue = new Map<string, PickablePerson[]>()
  for (const p of people) {
    const key = p.value.toLowerCase()
    byValue.set(key, [...(byValue.get(key) ?? []), p])
  }
  // An item is a person's label when exactly one person has its value; otherwise, it is the value.
  const itemOf = (v: string) => {
    const key = v.toLowerCase()
    for (const [item, p] of picked) if (p.value.toLowerCase() === key) return item
    const matches = byValue.get(key)
    return matches?.length === 1 ? label(matches[0].person) : v
  }
  const pick = (item: string) => {
    const known = picked.get(item)
    if (known) return known
    const p = byLabel.get(item)
    return p
      ? { value: p.value, chip: personName(p.person, currentUser) }
      : { value: item, chip: item }
  }
  const selected = value.map(itemOf)
  const typed = query.trim()
  // A new typed value comes first, so Enter adds it. A listed or picked value
  // stays its own item, so Enter on a picked one removes it.
  const fromTyped = typed ? (acceptTyped?.(typed) ?? null) : null
  const extra =
    fromTyped &&
    !byValue.has(fromTyped.toLowerCase()) &&
    !value.some((v) => v.toLowerCase() === fromTyped.toLowerCase())
      ? [fromTyped]
      : []
  const items = [...extra, ...selected.filter((i) => !byLabel.has(i)), ...byLabel.keys()]
  return (
    <div className={styles.addField}>
      <Typography.BodySmallStrong as="label" htmlFor={id}>
        People
      </Typography.BodySmallStrong>
      {value.map((v) => (
        <input key={v} type="hidden" name={name} value={v} />
      ))}
      <Combobox.Root
        autoHighlight
        hideWhenEmpty
        items={[...new Set(items)]}
        multiple
        inputValue={query}
        onInputValueChange={setQuery}
        value={selected}
        onValueChange={(next) => {
          const chosen = Array.isArray(next) ? next : []
          const picks = new Map(chosen.map((item) => [item, pick(item)]))
          setPicked(picks)
          onChange([...picks.values()].map((p) => p.value))
          setQuery('')
        }}
      >
        <Combobox.InputGroup>
          <Combobox.Chips>
            <Combobox.Value>
              {(chosen) =>
                Array.isArray(chosen)
                  ? chosen.map((item) => {
                      const chip = pick(item).chip
                      return (
                        <Combobox.Chip key={item}>
                          <Combobox.ChipLabel>{chip}</Combobox.ChipLabel>
                          <Combobox.ChipRemove aria-label={`Remove ${chip}`} />
                        </Combobox.Chip>
                      )
                    })
                  : null
              }
            </Combobox.Value>
            <Combobox.Input
              id={id}
              bare
              maxLength={320}
              placeholder={value.length > 0 ? '' : placeholder}
            />
          </Combobox.Chips>
        </Combobox.InputGroup>
        <Combobox.Content>
          <Combobox.List>
            {(item: string) => (
              <Combobox.Item key={item} value={item}>
                {extra.includes(item) ? `Add ${item}` : item}
              </Combobox.Item>
            )}
          </Combobox.List>
        </Combobox.Content>
      </Combobox.Root>
    </div>
  )
}

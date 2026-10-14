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
  // Each item is a key: a person's user, or a value that names no one person.
  // Keys keep two people with the same name or value apart.
  const listed = new Map<string, Entry>(
    people.map((p) => [
      `person:${p.person.user}`,
      {
        value: p.value,
        label: label(p.person, currentUser),
        name: personName(p.person, currentUser),
      },
    ]),
  )
  // Each picked item, kept so a chip keeps its value and its name after its
  // person leaves the list, or after another person shares its value.
  const [picked, setPicked] = useState(new Map<string, Entry>())
  const entryOf = (item: string): Entry => {
    const known = picked.get(item) ?? listed.get(item)
    if (known) return known
    const v = item.slice('value:'.length)
    return { value: v, label: v, name: v }
  }
  const byValue = new Map<string, string[]>()
  for (const [item, e] of listed) {
    const key = e.value.toLowerCase()
    byValue.set(key, [...(byValue.get(key) ?? []), item])
  }
  // A value's item is its pick, its one matching person, or the value itself.
  const itemOf = (v: string) => {
    const key = v.toLowerCase()
    for (const [item, e] of picked) if (e.value.toLowerCase() === key) return item
    const matches = byValue.get(key)
    return matches?.length === 1 ? matches[0] : `value:${v}`
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
      ? [`value:${fromTyped}`]
      : []
  const items = [...extra, ...selected.filter((i) => !listed.has(i)), ...listed.keys()]
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
        itemToStringLabel={(item) => entryOf(item).label}
        multiple
        inputValue={query}
        onInputValueChange={setQuery}
        value={selected}
        onValueChange={(next) => {
          const chosen = Array.isArray(next) ? next : []
          // The form submits values, so one value names one pick: a new pick
          // replaces an earlier one with the same value.
          const seen = new Set<string>()
          const kept = chosen.toReversed().filter((item) => {
            const key = entryOf(item).value.toLowerCase()
            if (seen.has(key)) return false
            seen.add(key)
            return true
          })
          const picks = new Map(kept.toReversed().map((item) => [item, entryOf(item)]))
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
                  ? chosen.map((item, _, all) => {
                      // Two chips with one name show their emails too.
                      const { name: chipName, label: full } = entryOf(item)
                      const twin = all.some((o) => o !== item && entryOf(o).name === chipName)
                      const chip = twin ? full : chipName
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
                {extra.includes(item) ? `Add ${entryOf(item).label}` : entryOf(item).label}
              </Combobox.Item>
            )}
          </Combobox.List>
        </Combobox.Content>
      </Combobox.Root>
    </div>
  )
}

/** An item's value, its text in the list, and its name on a chip. */
interface Entry {
  value: string
  label: string
  name: string
}

// The combobox matches an item on its label, so the label holds the name and email.
function label(p: PersonView, currentUser: string) {
  return p.name && p.email
    ? `${personName(p, currentUser)} (${p.email})`
    : personName(p, currentUser)
}

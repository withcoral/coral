import { useId, useRef, useState } from 'react'

import { Button, Combobox, Typography } from '@/wax/components'

import { ActionDialog, RoleField } from './forms'
import { personLabel } from './person'
import type { FormContext, PersonView } from './types'
import * as styles from './management.css'

/**
 * Adds people to a workspace with one role. You can always type an email.
 * Organization owners can also pick people from the organization; other
 * owners cannot list it.
 */
export function AddMembersDialog({
  candidates,
  disabled,
  form,
}: {
  /** Organization members who are not in the workspace, or null when you cannot list them. */
  candidates: PersonView[] | null
  disabled: boolean
  form: FormContext
}) {
  const [open, setOpen] = useState(false)
  const [emails, setEmails] = useState<string[]>([])
  const [role, setRole] = useState('member')
  const trigger = useRef<HTMLButtonElement>(null)
  const peopleId = useId()
  const noun = role === 'owner' ? 'owner' : 'member'
  return (
    <>
      <Button.TextButton
        ref={trigger}
        variant="primary"
        disabled={disabled}
        onClick={() => setOpen(true)}
      >
        Add members
      </Button.TextButton>
      <ActionDialog
        open={open}
        onOpenChange={(next) => {
          setOpen(next)
          if (!next) {
            setEmails([])
            setRole('member')
          }
        }}
        finalFocus={trigger}
        size="l"
        title="Add members"
        description={
          candidates
            ? 'Choose people or enter their email. A person appears here after they sign in to Coral.'
            : 'Enter the email of a member of your organization. They must sign in to Coral before you can add them.'
        }
        submit={emails.length > 1 ? `Add ${emails.length} ${noun}s` : `Add ${noun}`}
        form={form}
        intent="addMember"
        disabled={disabled || emails.length === 0}
        // A partial add keeps only the refused people, so a retry adds no one twice.
        onError={(result) => {
          if (result.refused?.length) setEmails(result.refused)
        }}
      >
        <div className={styles.addFields}>
          <div className={styles.addField}>
            <Typography.BodySmallStrong as="label" htmlFor={peopleId}>
              People
            </Typography.BodySmallStrong>
            {emails.map((email) => (
              <input key={email} type="hidden" name="email" value={email} />
            ))}
            <PeoplePicker
              id={peopleId}
              candidates={candidates ?? []}
              placeholder={
                candidates?.length ? 'Search people or enter an email' : 'Enter an email'
              }
              value={emails}
              onChange={setEmails}
            />
          </div>
          <div className={styles.addField}>
            <Typography.BodySmallStrong as="span">Role</Typography.BodySmallStrong>
            <RoleField label="Role" defaultValue={role} fullWidth onChange={setRole} />
          </div>
        </div>
      </ActionDialog>
    </>
  )
}

// The server checks the address; this only tells an email from a search.
const EMAIL = /^[^\s@]+@[^\s@]+$/

// The combobox filters on its item strings, so each holds the name and email.
const label = (p: PersonView) => (p.name ? `${p.name} (${p.email})` : p.email)

/**
 * Picks people by name or email, or takes an email typed in full. The value is
 * their emails.
 */
function PeoplePicker({
  id,
  candidates,
  placeholder,
  value,
  onChange,
}: {
  id: string
  candidates: PersonView[]
  placeholder: string
  value: string[]
  onChange: (emails: string[]) => void
}) {
  const [query, setQuery] = useState('')
  // Each picked item and its email. Emails are not unique, so a person who later
  // shares one must not take over the chip; and a chip keeps its email after
  // its person leaves the list.
  const [picked, setPicked] = useState(new Map<string, string>())
  const byLabel = new Map(candidates.map((p) => [label(p), p]))
  const byEmail = new Map<string, PersonView[]>()
  for (const p of candidates) {
    const key = p.email.toLowerCase()
    byEmail.set(key, [...(byEmail.get(key) ?? []), p])
  }
  // An item is a candidate's label, or an email that matches no one candidate.
  const itemOf = (email: string) => {
    const key = email.toLowerCase()
    const people = byEmail.get(key)
    for (const [item, e] of picked) if (e.toLowerCase() === key) return item
    return people?.length === 1 ? label(people[0]) : email
  }
  const emailOf = (item: string) => picked.get(item) ?? byLabel.get(item)?.email ?? item
  const selected = value.map(itemOf)
  const typed = query.trim()
  // A new typed email comes first, so Enter adds it. A listed or picked email
  // stays its own item, so Enter on a picked one removes it.
  const known = (email: string) =>
    byEmail.has(email.toLowerCase()) || value.some((e) => e.toLowerCase() === email.toLowerCase())
  const extra = EMAIL.test(typed) && !known(typed) ? [typed] : []
  const items = [...extra, ...selected.filter((i) => !byLabel.has(i)), ...byLabel.keys()]
  return (
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
        setPicked(new Map(chosen.map((item) => [item, emailOf(item)])))
        onChange(chosen.map(emailOf))
        setQuery('')
      }}
    >
      <Combobox.InputGroup>
        <Combobox.Chips>
          <Combobox.Value>
            {(chosen) =>
              Array.isArray(chosen)
                ? chosen.map((item) => {
                    const person = byLabel.get(item)
                    const name = person ? personLabel(person) : item
                    return (
                      <Combobox.Chip key={item}>
                        <Combobox.ChipLabel>{name}</Combobox.ChipLabel>
                        <Combobox.ChipRemove aria-label={`Remove ${name}`} />
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
  )
}

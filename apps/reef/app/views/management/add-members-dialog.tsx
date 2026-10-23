import { useRef, useState } from 'react'

import { Button, Typography } from '@/wax/components'

import { ActionDialog, RoleField } from './forms'
import { PeopleField } from './people-field'
import type { FormContext, PersonView } from './types'
import * as styles from './management.css'

/**
 * Adds people to a workspace with one role. You can always type an email.
 * Organization owners can also pick people from the organization; other
 * owners cannot list it.
 */
export function AddMembersDialog({
  candidates,
  currentUser,
  disabled,
  form,
  labelClassName,
}: {
  /** Organization members who are not in the workspace, or null when you cannot list them. */
  candidates: PersonView[] | null
  currentUser: string
  disabled: boolean
  form: FormContext
  /** Styles the button's label, such as a toolbar's `toolbarActionLabel`. */
  labelClassName?: string
}) {
  const [open, setOpen] = useState(false)
  const [emails, setEmails] = useState<string[]>([])
  const [role, setRole] = useState('member')
  const trigger = useRef<HTMLButtonElement>(null)
  const noun = role === 'owner' ? 'owner' : 'member'
  return (
    <>
      <Button.Container ref={trigger} disabled={disabled} onClick={() => setOpen(true)}>
        <Button.Icon name="Plus" />
        <Button.Text className={labelClassName}>Add members</Button.Text>
      </Button.Container>
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
          <PeopleField
            name="email"
            people={(candidates ?? []).map((p) => ({ value: p.email, person: p }))}
            currentUser={currentUser}
            placeholder={candidates?.length ? 'Search people or enter an email' : 'Enter an email'}
            value={emails}
            onChange={setEmails}
            acceptTyped={(text) => (EMAIL.test(text) ? text : null)}
          />
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

import { useRef, useState } from 'react'
import { Banner, Button, Menu, Typography } from '@/wax/components'
import { ActionDialog, PersonField } from './forms'
import { ManagementLayout, trail } from './layout'
import { MembersTable } from './members-table'
import { personLabel } from './person'
import type { FormContext, OrganizationPersonView, PersonView } from './types'
import * as styles from './management.css'

interface OrganizationMembershipView {
  name: string
  etag: string
  profile: PersonView
  role: string
  enabled: boolean
  /** The workspaces this person alone owns, by display name. */
  soleOwnerOf: string[]
}

export function OrganizationMembersPage({
  memberships,
  candidates,
  currentUser,
  nextPage,
  form,
}: {
  memberships: OrganizationMembershipView[]
  /** Every enabled organization member: the people who can take over a workspace. */
  candidates: OrganizationPersonView[]
  currentUser: string
  nextPage: string
  form: FormContext
}) {
  return (
    <ManagementLayout breadcrumbs={trail([], 'Members')} titled compact>
      <MembersTable
        ariaLabel="Members"
        currentUser={currentUser}
        rows={memberships.map((m) => ({
          key: m.name,
          profile: m.profile,
          role: m.role,
          enabled: m.enabled,
          actions: (
            <OrganizationMembershipActions
              membership={m}
              candidates={candidates.filter((c) => c.name !== m.name)}
              currentUser={currentUser}
              form={form}
            />
          ),
        }))}
      />
      {nextPage && (
        <Button.InternalLink to={`?page=${encodeURIComponent(nextPage)}`}>
          Next members
        </Button.InternalLink>
      )}
    </ManagementLayout>
  )
}

function OrganizationMembershipActions({
  membership: m,
  candidates,
  currentUser,
  form,
}: {
  membership: OrganizationMembershipView
  /** The other enabled organization members. */
  candidates: OrganizationPersonView[]
  currentUser: string
  form: FormContext
}) {
  const [open, setOpen] = useState(false)
  const [transferTo, setTransferTo] = useState('')
  const transfer = m.soleOwnerOf.length > 0
  const trigger = useRef<HTMLButtonElement>(null)
  const label = personLabel(m.profile)
  return (
    <>
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.IconButton
              ref={trigger}
              name="Ellipsis"
              ariaLabel={`Actions for ${label}`}
              variant="bare"
            />
          }
        >
          {null}
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item
            onClick={() => {
              setTransferTo('')
              setOpen(true)
            }}
          >
            Remove from organization
          </Menu.Item>
        </Menu.Content>
      </Menu.Container>
      <ActionDialog
        open={open}
        onOpenChange={setOpen}
        finalFocus={trigger}
        title="Remove from organization"
        submit="Remove from organization"
        destructive
        description={`Remove ${label} from every workspace. This removes their accounts and current saved functions. Historical runs remain.`}
        intent="removeOrganizationMember"
        name={m.name}
        etag={m.etag}
        form={form}
        disabled={transfer && !transferTo}
      >
        {transfer && (
          <>
            <div className={styles.intro}>
              <Typography.Body as="p">
                {m.soleOwnerOf.length === 1
                  ? 'This person is the only owner of this workspace:'
                  : 'This person is the only owner of these workspaces:'}
              </Typography.Body>
              <ul aria-label="Workspaces that need a new owner">
                {m.soleOwnerOf.map((workspace, index) => (
                  // Display names are not unique.
                  <Typography.Body as="li" key={index}>
                    {workspace}
                  </Typography.Body>
                ))}
              </ul>
            </div>
            {candidates.length > 0 ? (
              <PersonField
                label="New owner"
                name="transferTo"
                people={candidates}
                currentUser={currentUser}
                onChange={setTransferTo}
              />
            ) : (
              <Banner variant="warning">
                No other enabled member can take over. You cannot remove this person now.
              </Banner>
            )}
          </>
        )}
      </ActionDialog>
    </>
  )
}

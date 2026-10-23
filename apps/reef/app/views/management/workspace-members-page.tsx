import { useRef, useState } from 'react'
import { Banner, Button, Menu, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { InfoTip } from '@/wax/components/tooltip'
import type {
  ActionResult,
  FormContext,
  MembershipView,
  OwnerView,
  PersonView,
  WorkspaceView,
} from './types'
import { toolbarActionLabel } from '@/components/catalog'
import { AddMembersDialog } from './add-members-dialog'
import { ActionDialog, ActionNotice, RoleField } from './forms'
import { ArchivedBanner, ManagementLayout, trail, PageIntro } from './layout'
import { personLabel } from './person'
import { MembersTable } from './members-table'
import * as styles from './management.css'

export interface WorkspaceMembersPageProps {
  workspace: WorkspaceView
  memberships: MembershipView[]
  owners: OwnerView[]
  ownMembership: MembershipView | null
  nextMembers: string
  nextOwners: string
  /** Organization members you can add, or null when you cannot list them. */
  candidates: PersonView[] | null
  form: FormContext
  pending?: boolean
  result?: ActionResult
}

export function WorkspaceMembersPage({
  workspace: w,
  memberships,
  owners,
  ownMembership,
  nextMembers,
  nextOwners,
  candidates,
  form,
  result,
}: WorkspaceMembersPageProps) {
  const owner = w.effectiveRole === 'owner'
  const active = w.state === 'active'
  const path = `${w.path}/members`
  // An owner lists every membership. A member sees their own, then the owners.
  const members = new Map<
    string,
    { profile: PersonView; member?: MembershipView; owner: boolean; enabled: boolean }
  >()
  for (const m of owner ? memberships : ownMembership ? [ownMembership] : [])
    members.set(m.user, {
      profile: m.profile,
      member: m,
      owner: m.role === 'owner',
      enabled: m.enabled,
    })
  for (const o of owners)
    members.set(o.user, {
      member: members.get(o.user)?.member,
      profile: o.profile,
      owner: true,
      enabled: o.enabled,
    })
  const rows = Array.from(members, ([user, person]) => ({
    key: user,
    profile: person.profile,
    role: person.owner ? 'Owner' : 'Member',
    enabled: person.enabled,
    actions: person.member && active && (owner || person.member.user === w.currentUser) && (
      <MembershipActions
        membership={person.member}
        currentUser={w.currentUser}
        owner={owner}
        form={form}
      />
    ),
  }))
  return (
    <ManagementLayout breadcrumbs={trail([], 'Members')} titled compact>
      <PageIntro status={!active ? <Pill color="gray">Archived</Pill> : undefined}>
        {!owner ? (
          <InfoTip label="Workspace members">
            As a member, you see only yourself and the owners. Owners see all members.
          </InfoTip>
        ) : undefined}
      </PageIntro>
      {!active && (
        <ArchivedBanner workspace={w}>
          You cannot change access until you restore the workspace.
        </ArchivedBanner>
      )}
      <ActionNotice result={result} />
      <MembersTable
        ariaLabel="Members"
        currentUser={w.currentUser}
        rows={rows}
        action={
          owner && (
            <AddMembersDialog
              candidates={candidates}
              currentUser={w.currentUser}
              disabled={!active}
              form={form}
              labelClassName={toolbarActionLabel}
            />
          )
        }
      />
      {(nextMembers || nextOwners) && (
        <div className={styles.row}>
          {nextMembers && (
            <Button.InternalLink to={`${path}?members=${encodeURIComponent(nextMembers)}`}>
              Next members
            </Button.InternalLink>
          )}
          {nextOwners && (
            <Button.InternalLink to={`${path}?owners=${encodeURIComponent(nextOwners)}`}>
              Next owners
            </Button.InternalLink>
          )}
        </div>
      )}
    </ManagementLayout>
  )
}

function MembershipActions({
  membership: m,
  currentUser,
  owner,
  form,
}: {
  membership: MembershipView
  currentUser: string
  owner: boolean
  form: FormContext
}) {
  const [action, setAction] = useState<'role' | 'remove' | null>(null)
  const [role, setRole] = useState(m.role)
  const trigger = useRef<HTMLButtonElement>(null)
  const self = m.user === currentUser
  const removeLabel = self ? 'Leave workspace' : 'Remove from workspace'
  const consequence = self
    ? 'You lose access to this workspace.'
    : 'They lose access to this workspace.'
  return (
    <>
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.IconButton
              ref={trigger}
              name="Ellipsis"
              ariaLabel={`Actions for ${personLabel(m.profile)}`}
              variant="bare"
            />
          }
        >
          {null}
        </Menu.Trigger>
        <Menu.Content>
          {owner && (
            <Menu.Item
              onClick={() => {
                setRole(m.role)
                setAction('role')
              }}
            >
              Change role
            </Menu.Item>
          )}
          <Menu.Item onClick={() => setAction('remove')}>{removeLabel}</Menu.Item>
        </Menu.Content>
      </Menu.Container>
      <ActionDialog
        open={action !== null}
        onOpenChange={(open) => {
          if (!open) setAction(null)
        }}
        finalFocus={trigger}
        title={
          action === 'role'
            ? self
              ? 'Change your role'
              : `Change ${personLabel(m.profile)}'s role`
            : removeLabel
        }
        submit={action === 'role' ? 'Save role' : removeLabel}
        description={
          action === 'role'
            ? 'Choose their role in this workspace.'
            : `${consequence} Coral removes ${self ? 'your' : 'their'} saved functions and account connections from this workspace. Coral keeps the run history.`
        }
        form={form}
        intent={owner ? 'membership' : 'leave'}
        name={m.name}
        etag={m.etag}
        destructive={action === 'remove'}
      >
        <input type="hidden" name="operation" value={action === 'role' ? 'update' : 'remove'} />
        {action === 'role' && (
          <>
            <div className={styles.roleField}>
              <Typography.BodySmallStrong as="span">Role</Typography.BodySmallStrong>
              <RoleField
                label={`Role for ${personLabel(m.profile)}`}
                defaultValue={m.role}
                fullWidth
                onChange={setRole}
              />
            </div>
            {self && role === 'member' && (
              <Banner variant="warning">
                Saving this role removes your owner access. You keep member access.
              </Banner>
            )}
          </>
        )}
      </ActionDialog>
    </>
  )
}

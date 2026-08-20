import { useCallback, useEffect, useRef, useState } from 'react'
import { useFetcher } from 'react-router'

import { Banner, Button, Combobox, Dialog, Menu, Table, Typography } from '@/wax/components'
import { Avatar } from '@/wax/components/avatar'
import { TextInput } from '@/wax/components/inputs/text'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'

import { filterWorkspaceUsers } from './filter-workspace-users'
import * as styles from './workspace-users.css'

export type WorkspaceUserRole = 'member' | 'owner'

export interface WorkspaceUser {
  readonly displayName?: string
  readonly role: WorkspaceUserRole
  readonly userId: string
}

export interface WorkspaceUserCandidate {
  readonly displayName?: string
  readonly userId: string
}

export interface WorkspaceUsersData {
  readonly availableUsers: ReadonlyArray<WorkspaceUserCandidate>
  readonly currentUserId: string
  readonly currentUserRole: WorkspaceUserRole
  readonly error?: string
  readonly members: ReadonlyArray<WorkspaceUser>
  readonly workspaceName: string
}

export interface WorkspaceUsersActionData {
  readonly intent: 'add' | 'remove' | 'role'
  readonly message: string
  readonly status: 'error' | 'success'
  readonly userId: string
}

const USER_COLUMNS: Table.Column[] = [
  { label: 'Name', width: 'minmax(280px, 1fr)' },
  { label: 'Role', width: 180 },
  { align: 'right', label: 'Actions', width: 120 },
]

export function WorkspaceUsers({ data }: { readonly data: WorkspaceUsersData }) {
  const addFetcher = useFetcher<WorkspaceUsersActionData>()
  const removalFetcher = useFetcher<WorkspaceUsersActionData>()
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [addRole, setAddRole] = useState<WorkspaceUserRole>('member')
  const [showAddResult, setShowAddResult] = useState(false)
  const [addUserId, setAddUserId] = useState<string>()
  const [removingMember, setRemovingMember] = useState<WorkspaceUser | null>(null)
  const [search, setSearch] = useState('')
  const searchInputRef = useRef<HTMLInputElement>(null)
  const isOwner = data.currentUserRole === 'owner'
  const ownerCount = data.members.filter((member) => member.role === 'owner').length
  const visibleMembers = filterWorkspaceUsers(data.members, search)
  const memberIds = new Set(data.members.map((member) => member.userId))
  const availableUsers = data.availableUsers.filter((user) => !memberIds.has(user.userId))
  const availableUserLabels = availableUsers.map(userCandidateLabel)
  const selectedAddUser = availableUsers.find((user) => user.userId === addUserId)
  const addPending = addFetcher.state !== 'idle'
  const removalPending = removalFetcher.state !== 'idle'
  const mutationsDisabled = addPending || removalPending
  const addError =
    showAddResult && addFetcher.data?.status === 'error' ? addFetcher.data.message : undefined
  const removalError =
    removalFetcher.data?.status === 'error' && removalFetcher.data.userId === removingMember?.userId
      ? removalFetcher.data.message
      : undefined

  const onSearchShortcut = useCallback((event: KeyboardEvent) => {
    const input = searchInputRef.current
    if (!input) return

    event.preventDefault()
    input.focus()
    input.select()
  }, [])

  useEffect(() => {
    if (
      addFetcher.data?.status === 'success' &&
      addFetcher.data.intent === 'add' &&
      addFetcher.data.userId === addUserId
    ) {
      setAddDialogOpen(false)
      setAddRole('member')
      setShowAddResult(false)
      setAddUserId(undefined)
    }
  }, [addFetcher.data, addUserId])

  useEffect(() => {
    if (
      removalFetcher.data?.status === 'success' &&
      removalFetcher.data.intent === 'remove' &&
      removalFetcher.data.userId === removingMember?.userId
    ) {
      setRemovingMember(null)
    }
  }, [removalFetcher.data, removingMember?.userId])

  if (!isOwner) {
    return (
      <WorkspaceUsersPageHeader>
        <Banner title="Owner access required">
          Only workspace owners can view and manage workspace users. Your current role is Member.
        </Banner>
      </WorkspaceUsersPageHeader>
    )
  }

  return (
    <WorkspaceUsersPageHeader
      actions={
        <Dialog.Root
          onOpenChange={(open) => {
            if (addPending) return
            setAddDialogOpen(open)
            setShowAddResult(false)
            if (!open) {
              setAddRole('member')
              setAddUserId(undefined)
            }
          }}
          open={addDialogOpen}
        >
          <Dialog.Trigger
            render={<Button.Container disabled={removalPending || Boolean(data.error)} />}
          >
            <Button.Icon name="Plus" />
            <Button.Text>Add user</Button.Text>
          </Dialog.Trigger>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup size="l">
              <Dialog.Title>Add workspace user</Dialog.Title>
              <Dialog.Description>
                Choose a user and the role they should have in {data.workspaceName}.
              </Dialog.Description>
              <addFetcher.Form
                className={styles.addForm}
                method="post"
                onSubmit={() => setShowAddResult(true)}
              >
                <input name="intent" type="hidden" value="add" />
                <input name="userId" type="hidden" value={addUserId ?? ''} />
                <input name="role" type="hidden" value={addRole} />
                <div className={styles.addFields}>
                  <div className={styles.addField}>
                    <Typography.BodyStrong>User</Typography.BodyStrong>
                    {availableUsers.length === 0 ? (
                      <Typography.Body variant="secondary">
                        All users already have access.
                      </Typography.Body>
                    ) : (
                      <Combobox.Root
                        disabled={addPending}
                        items={availableUserLabels}
                        onValueChange={(value) => {
                          const user = availableUsers.find(
                            (candidate) => userCandidateLabel(candidate) === value,
                          )
                          setShowAddResult(false)
                          setAddUserId(user?.userId)
                        }}
                        value={selectedAddUser ? userCandidateLabel(selectedAddUser) : undefined}
                      >
                        <Combobox.Input placeholder="Search users..." />
                        <Combobox.Content>
                          <Combobox.Empty>No users found.</Combobox.Empty>
                          <Combobox.List>
                            {(label) => (
                              <Combobox.Item key={label} value={label}>
                                {label}
                              </Combobox.Item>
                            )}
                          </Combobox.List>
                        </Combobox.Content>
                      </Combobox.Root>
                    )}
                  </div>
                  <div className={styles.addField}>
                    <Typography.BodyStrong>Role</Typography.BodyStrong>
                    <Menu.Container>
                      <Menu.Trigger
                        render={
                          <Button.Container
                            disabled={addPending}
                            fullWidth
                            size="36"
                            variant="secondary"
                          />
                        }
                      >
                        <Button.Text>{roleLabel(addRole)}</Button.Text>
                        <Button.Icon name="ChevronDown" />
                      </Menu.Trigger>
                      <Menu.Content align="start" className={styles.addRoleMenu}>
                        <Menu.RadioGroup
                          onValueChange={(role) => setAddRole(role as WorkspaceUserRole)}
                          value={addRole}
                        >
                          <Menu.RadioItem value="member">Member</Menu.RadioItem>
                          <Menu.RadioItem value="owner">Owner</Menu.RadioItem>
                        </Menu.RadioGroup>
                      </Menu.Content>
                    </Menu.Container>
                  </div>
                </div>
                {addError ? <Banner variant="error">{addError}</Banner> : null}
                <Dialog.Actions>
                  <Button.TextButton
                    disabled={addPending}
                    onClick={() => {
                      setAddDialogOpen(false)
                      setAddRole('member')
                      setShowAddResult(false)
                      setAddUserId(undefined)
                    }}
                    variant="secondary"
                  >
                    Cancel
                  </Button.TextButton>
                  <Button.TextButton
                    disabled={!addUserId || addPending}
                    type="submit"
                    variant="primary"
                  >
                    {addPending ? 'Adding…' : 'Add user'}
                  </Button.TextButton>
                </Dialog.Actions>
              </addFetcher.Form>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      }
      search={
        <TextInput
          ariaLabel="Search workspace users"
          className={styles.searchInput}
          icon="Search"
          onChange={setSearch}
          placeholder="Search users"
          ref={searchInputRef}
          type="search"
          value={search}
        />
      }
    >
      <KeyboardShortcut handler={onSearchShortcut} shortcut="$mod+f" />
      <Table.Container columns={USER_COLUMNS} variant="card">
        <Table.Head />
        <Table.Body>
          {data.error ? (
            <Table.Status>
              <Banner variant="error">{data.error}</Banner>
            </Table.Status>
          ) : data.members.length === 0 ? (
            <Table.Status>
              <Typography.BodySmall variant="tertiary">
                This workspace has no users.
              </Typography.BodySmall>
            </Table.Status>
          ) : visibleMembers.length === 0 ? (
            <Table.Status>
              <Typography.BodySmall variant="tertiary">
                No users match "{search}".
              </Typography.BodySmall>
            </Table.Status>
          ) : (
            visibleMembers.map((member) => (
              <WorkspaceUserRow
                currentUserId={data.currentUserId}
                isLastOwner={member.role === 'owner' && ownerCount === 1}
                key={member.userId}
                member={member}
                mutationsDisabled={mutationsDisabled}
                onRemove={setRemovingMember}
              />
            ))
          )}
        </Table.Body>
      </Table.Container>

      <Dialog.Root
        onOpenChange={(open) => {
          if (!open && removalFetcher.state === 'idle') setRemovingMember(null)
        }}
        open={removingMember !== null}
      >
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup>
            <Dialog.Title>Remove workspace user?</Dialog.Title>
            <Dialog.Description>
              {removingMember
                ? `${removingMember.displayName || removingMember.userId} will lose access to ${data.workspaceName}.`
                : ''}
            </Dialog.Description>
            {removalError ? <Banner variant="error">{removalError}</Banner> : null}
            <Dialog.Actions>
              <Button.TextButton
                disabled={removalPending}
                onClick={() => setRemovingMember(null)}
                variant="secondary"
              >
                Cancel
              </Button.TextButton>
              <removalFetcher.Form method="post">
                <input name="intent" type="hidden" value="remove" />
                <input name="userId" type="hidden" value={removingMember?.userId ?? ''} />
                <Button.TextButton disabled={removalPending} type="submit" variant="destructive">
                  Remove user
                </Button.TextButton>
              </removalFetcher.Form>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </WorkspaceUsersPageHeader>
  )
}

function WorkspaceUserRow({
  currentUserId,
  isLastOwner,
  member,
  mutationsDisabled,
  onRemove,
}: {
  readonly currentUserId: string
  readonly isLastOwner: boolean
  readonly member: WorkspaceUser
  readonly mutationsDisabled: boolean
  readonly onRemove: (member: WorkspaceUser) => void
}) {
  const roleFetcher = useFetcher<WorkspaceUsersActionData>()
  const rolePending = roleFetcher.state !== 'idle'
  const rowError = roleFetcher.data?.status === 'error' ? roleFetcher.data.message : undefined
  const controlsDisabled = isLastOwner || rolePending || mutationsDisabled

  return (
    <Table.Row className={styles.memberTableRow}>
      <Table.Cell wrap>
        <div className={styles.memberRow}>
          <Avatar name={member.displayName || member.userId} seed={member.userId} size="20" />
          <div className={styles.memberIdentity}>
            <div className={styles.memberNameLine}>
              <Typography.BodyStrong>
                {member.displayName || member.userId}
                {member.userId === currentUserId ? ' (you)' : ''}
              </Typography.BodyStrong>
              {member.displayName ? (
                <Typography.CodeSmallInline variant="tertiary">
                  {member.userId}
                </Typography.CodeSmallInline>
              ) : null}
            </div>
            {isLastOwner ? (
              <Typography.BodySmall variant="tertiary">
                Last owner — role and removal are locked.
              </Typography.BodySmall>
            ) : null}
            {rowError ? (
              <Typography.BodySmall role="alert" variant="error">
                {rowError}
              </Typography.BodySmall>
            ) : null}
          </div>
        </div>
      </Table.Cell>
      <Table.Cell>
        <Menu.Container>
          <Menu.Trigger
            className={styles.roleTrigger}
            render={
              <Button.Container
                ariaLabel={`Role for ${member.displayName || member.userId}`}
                disabled={controlsDisabled}
                fullWidth
                variant="secondary"
              />
            }
          >
            <Button.Text>{rolePending ? 'Saving…' : roleLabel(member.role)}</Button.Text>
            <Button.Icon name="ChevronDown" />
          </Menu.Trigger>
          <Menu.Content align="end" className={styles.roleMenu}>
            <Menu.RadioGroup
              onValueChange={(role) => {
                roleFetcher.submit(
                  { intent: 'role', role, userId: member.userId },
                  { method: 'post' },
                )
              }}
              value={member.role}
            >
              <Menu.RadioItem value="owner">Owner</Menu.RadioItem>
              <Menu.RadioItem value="member">Member</Menu.RadioItem>
            </Menu.RadioGroup>
          </Menu.Content>
        </Menu.Container>
      </Table.Cell>
      <Table.Cell>
        <Button.Container
          ariaLabel={`Remove ${member.displayName || member.userId}`}
          className={styles.removeButton}
          disabled={controlsDisabled}
          onClick={() => onRemove(member)}
          size="32"
          variant="bare"
        >
          <Button.Icon name="UserRoundMinus" />
        </Button.Container>
      </Table.Cell>
    </Table.Row>
  )
}

function roleLabel(role: WorkspaceUserRole): string {
  return role === 'owner' ? 'Owner' : 'Member'
}

function userCandidateLabel(user: WorkspaceUserCandidate): string {
  return user.displayName ? `${user.displayName} (${user.userId})` : user.userId
}

function WorkspaceUsersPageHeader({
  actions,
  children,
  search,
}: {
  readonly actions?: React.ReactNode
  readonly children: React.ReactNode
  readonly search?: React.ReactNode
}) {
  return (
    <section className={styles.page}>
      <header className={styles.header}>
        <div className={styles.headerText}>
          <Typography.HeadingLarge as="h1">Users</Typography.HeadingLarge>
          <Typography.Body variant="secondary">Manage workspace members and roles.</Typography.Body>
        </div>
        {search || actions ? (
          <div className={styles.headerControls}>
            {search ? <div className={styles.searchBar}>{search}</div> : null}
            {actions}
          </div>
        ) : null}
      </header>
      {children}
    </section>
  )
}

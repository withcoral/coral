import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { useFetcher, useFetchers } from 'react-router'

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
  const fetchers = useFetchers()
  const addUserLabelId = useId()
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [addRole, setAddRole] = useState<WorkspaceUserRole>('member')
  const [showAddResult, setShowAddResult] = useState(false)
  const [addUserId, setAddUserId] = useState<string>()
  const [search, setSearch] = useState('')
  const submittedAddUserId = useRef<string | undefined>(undefined)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const isOwner = data.currentUserRole === 'owner'
  const ownerCount = data.members.filter((member) => member.role === 'owner').length
  const visibleMembers = filterWorkspaceUsers(data.members, search)
  const memberIds = new Set(data.members.map((member) => member.userId))
  const availableUsers = data.availableUsers.filter((user) => !memberIds.has(user.userId))
  const availableUserLabels = availableUsers.map(userCandidateLabel)
  const selectedAddUser = availableUsers.find((user) => user.userId === addUserId)
  const addPending = addFetcher.state !== 'idle'
  const ownershipReductionPending = fetchers.some((fetcher) => {
    if (fetcher.state === 'idle') return false

    const intent = fetcher.formData?.get('intent')
    const userId = fetcher.formData?.get('userId')
    const member = data.members.find((candidate) => candidate.userId === userId)
    if (member?.role !== 'owner') return false

    return intent === 'remove' || (intent === 'role' && fetcher.formData?.get('role') === 'member')
  })
  const addError =
    !addPending && showAddResult && addFetcher.data?.status === 'error'
      ? addFetcher.data.message
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
      addFetcher.state === 'idle' &&
      addFetcher.data?.status === 'success' &&
      addFetcher.data.intent === 'add' &&
      addFetcher.data.userId === submittedAddUserId.current
    ) {
      submittedAddUserId.current = undefined
      setAddDialogOpen(false)
      setAddRole('member')
      setShowAddResult(false)
      setAddUserId(undefined)
    }
  }, [addFetcher.data, addFetcher.state])

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
          <Dialog.Trigger render={<Button.Container disabled={Boolean(data.error)} />}>
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
                onSubmit={() => {
                  submittedAddUserId.current = addUserId
                  setShowAddResult(true)
                }}
              >
                <input name="intent" type="hidden" value="add" />
                <input name="userId" type="hidden" value={addUserId ?? ''} />
                <input name="role" type="hidden" value={addRole} />
                <div className={styles.addFields}>
                  <div className={styles.addField}>
                    <Typography.BodyStrong as="span" id={addUserLabelId}>
                      User
                    </Typography.BodyStrong>
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
                        <Combobox.Input
                          ariaLabelledby={addUserLabelId}
                          placeholder="Search users..."
                        />
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
                    <Typography.BodyStrong as="span">Role</Typography.BodyStrong>
                    <Menu.Container>
                      <Menu.Trigger
                        render={
                          <Button.Container
                            ariaLabel={`Role: ${roleLabel(addRole)}`}
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
                mutationsDisabled={addPending}
                ownershipReductionPending={ownershipReductionPending}
                workspaceName={data.workspaceName}
              />
            ))
          )}
        </Table.Body>
      </Table.Container>
    </WorkspaceUsersPageHeader>
  )
}

function WorkspaceUserRow({
  currentUserId,
  isLastOwner,
  member,
  mutationsDisabled,
  ownershipReductionPending,
  workspaceName,
}: {
  readonly currentUserId: string
  readonly isLastOwner: boolean
  readonly member: WorkspaceUser
  readonly mutationsDisabled: boolean
  readonly ownershipReductionPending: boolean
  readonly workspaceName: string
}) {
  const roleFetcher = useFetcher<WorkspaceUsersActionData>()
  const removalFetcher = useFetcher<WorkspaceUsersActionData>()
  const [confirmingDemotion, setConfirmingDemotion] = useState(false)
  const [removalDialogOpen, setRemovalDialogOpen] = useState(false)
  const [showRemovalResult, setShowRemovalResult] = useState(false)
  const rolePending = roleFetcher.state !== 'idle'
  const removalPending = removalFetcher.state !== 'idle'
  const roleError =
    !rolePending && roleFetcher.data?.status === 'error' ? roleFetcher.data.message : undefined
  const removalError =
    !removalPending && showRemovalResult && removalFetcher.data?.status === 'error'
      ? removalFetcher.data.message
      : undefined
  const rowError = roleError ?? removalError
  const ownerControlsDisabled = member.role === 'owner' && ownershipReductionPending
  const controlsDisabled =
    isLastOwner || ownerControlsDisabled || rolePending || removalPending || mutationsDisabled

  useEffect(() => {
    if (
      removalFetcher.data?.status === 'success' &&
      removalFetcher.data.intent === 'remove' &&
      removalFetcher.data.userId === member.userId
    ) {
      setRemovalDialogOpen(false)
      setShowRemovalResult(false)
    }
  }, [member.userId, removalFetcher.data])

  const submitRole = (role: WorkspaceUserRole) => {
    roleFetcher.submit({ intent: 'role', role, userId: member.userId }, { method: 'post' })
  }

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
                ariaLabel={`Role for ${member.displayName || member.userId}: ${roleLabel(member.role)}${rolePending ? ', saving' : ''}`}
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
                if (
                  member.userId === currentUserId &&
                  member.role === 'owner' &&
                  role === 'member'
                ) {
                  setConfirmingDemotion(true)
                  return
                }
                submitRole(role as WorkspaceUserRole)
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
          onClick={() => {
            setShowRemovalResult(false)
            setRemovalDialogOpen(true)
          }}
          size="32"
          variant="bare"
        >
          <Button.Icon name="UserRoundMinus" />
        </Button.Container>
      </Table.Cell>
      <Dialog.Root
        onOpenChange={(open) => {
          if (removalPending) return
          setRemovalDialogOpen(open)
          if (open) setShowRemovalResult(false)
        }}
        open={removalDialogOpen}
      >
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup>
            <Dialog.Title>Remove workspace user?</Dialog.Title>
            <Dialog.Description>
              {member.displayName || member.userId} will lose access to {workspaceName}.
            </Dialog.Description>
            {removalError ? <Banner variant="error">{removalError}</Banner> : null}
            <Dialog.Actions>
              <Button.TextButton
                disabled={removalPending}
                onClick={() => setRemovalDialogOpen(false)}
                variant="secondary"
              >
                Cancel
              </Button.TextButton>
              <removalFetcher.Form method="post" onSubmit={() => setShowRemovalResult(true)}>
                <input name="intent" type="hidden" value="remove" />
                <input name="userId" type="hidden" value={member.userId} />
                <Button.TextButton
                  disabled={removalPending || ownerControlsDisabled}
                  type="submit"
                  variant="destructive"
                >
                  Remove user
                </Button.TextButton>
              </removalFetcher.Form>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
      <Dialog.Root open={confirmingDemotion} onOpenChange={setConfirmingDemotion}>
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup>
            <Dialog.Title>Change your role to Member?</Dialog.Title>
            <Dialog.Description>
              You will lose access to manage users in {workspaceName}.
            </Dialog.Description>
            <Dialog.Actions>
              <Button.TextButton onClick={() => setConfirmingDemotion(false)} variant="secondary">
                Cancel
              </Button.TextButton>
              <Button.TextButton
                disabled={ownerControlsDisabled}
                onClick={() => {
                  setConfirmingDemotion(false)
                  submitRole('member')
                }}
                variant="destructive"
              >
                Change my role
              </Button.TextButton>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
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

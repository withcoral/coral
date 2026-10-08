import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { useFetcher, useRevalidator } from 'react-router'

import { Banner, Button, Combobox, Dialog, Menu, Table, Typography } from '@/wax/components'
import { Avatar } from '@/wax/components/avatar'
import { TextInput } from '@/wax/components/inputs/text'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { addToast } from '@/wax/components/toast'

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

export interface WorkspaceUserFailure {
  readonly message: string
  readonly userId: string
}

export interface WorkspaceUsersActionData {
  /**
   * The users the action refused, and why each one. `AddWorkspaceMember` takes one
   * user per call, so a batch add can land in part and the route reports the rest.
   */
  readonly failures?: ReadonlyArray<WorkspaceUserFailure>
  readonly intent: 'add' | 'remove' | 'role' | 'unsupported'
  readonly message: string
  readonly removedCurrentUser?: boolean
  readonly status: 'error' | 'success'
  /** The users the action was about. One add can carry several. */
  readonly userIds: ReadonlyArray<string>
}

const USER_COLUMNS: Table.Column[] = [
  { label: 'Name', width: 'minmax(280px, 1fr)' },
  { label: 'Role', width: 180 },
  { align: 'right', label: 'Actions', width: 120 },
]

export function WorkspaceUsers({ data }: { readonly data: WorkspaceUsersData }) {
  const addFetcher = useFetcher<WorkspaceUsersActionData>()
  const revalidator = useRevalidator()
  const fetcherNamespace = `workspace-users:${useId()}:`
  const pageHeadingId = useId()
  const [addDialogOpen, setAddDialogOpen] = useState(false)
  const [addRole, setAddRole] = useState<WorkspaceUserRole>('member')
  const [addUserIds, setAddUserIds] = useState<ReadonlyArray<string>>([])
  const [search, setSearch] = useState('')
  const submittedAddUserIds = useRef<ReadonlyArray<string>>([])
  const previousCurrentUserRole = useRef(data.currentUserRole)
  const previousMemberIds = useRef(new Set(data.members.map((member) => member.userId)))
  const searchInputRef = useRef<HTMLInputElement>(null)
  const isOwner = data.currentUserRole === 'owner'
  const visibleMembers = filterWorkspaceUsers(data.members, search)
  const memberIds = new Set(data.members.map((member) => member.userId))
  const availableUsers = data.availableUsers.filter((user) => !memberIds.has(user.userId))
  const availableUserLabels = availableUsers.map(userCandidateLabel)
  const candidateByLabel = new Map(availableUsers.map((user) => [userCandidateLabel(user), user]))
  const labelByUserId = new Map(
    availableUsers.map((user) => [user.userId, userCandidateLabel(user)]),
  )
  const addUserLabels = addUserIds
    .map((userId) => labelByUserId.get(userId))
    .filter((label): label is string => label !== undefined)
  const addPending = addFetcher.state !== 'idle'
  const retryPending = revalidator.state !== 'idle'

  const onSearchShortcut = useCallback((event: KeyboardEvent) => {
    if (document.querySelector('[role="dialog"]')) return

    const input = searchInputRef.current
    if (!input) return

    event.preventDefault()
    input.focus()
    input.select()
  }, [])

  useEffect(() => {
    const result = answeredAddResult(addFetcher.state, addFetcher.data, submittedAddUserIds.current)
    const refused = (result?.failures ?? []).map((failure) => failure.userId)
    const added = result?.status === 'success' && refused.length === 0

    if (added) {
      submittedAddUserIds.current = []
      setAddDialogOpen(false)
      setAddRole('member')
      setAddUserIds([])
    }

    // A refused add keeps the dialog open. The toast carries the reason, and the
    // selection narrows to the refused users so the retry carries only them.
    if (result && !added) {
      submittedAddUserIds.current = refused.length > 0 ? refused : submittedAddUserIds.current
      setAddUserIds((current) => (refused.length > 0 ? refused : current))
      addToast('error', { description: failureDescription(result), title: result.message })
    }
  }, [addFetcher.data, addFetcher.state])

  useEffect(() => {
    const previousRole = previousCurrentUserRole.current
    previousCurrentUserRole.current = data.currentUserRole

    if (previousRole === 'owner' && data.currentUserRole === 'member') {
      document.getElementById(pageHeadingId)?.focus()
    }
  }, [data.currentUserRole, pageHeadingId])

  useEffect(() => {
    const currentMemberIds = new Set(data.members.map((member) => member.userId))
    const memberWasRemoved = [...previousMemberIds.current].some(
      (userId) => !currentMemberIds.has(userId),
    )
    previousMemberIds.current = currentMemberIds

    if (memberWasRemoved) document.getElementById(pageHeadingId)?.focus()
  }, [data.members, pageHeadingId])

  if (!isOwner) {
    return (
      <WorkspaceUsersPageHeader headingId={pageHeadingId}>
        <Banner title="Owner access required">
          Only workspace owners can view and manage workspace users. Your current role is Member.
        </Banner>
      </WorkspaceUsersPageHeader>
    )
  }

  return (
    <WorkspaceUsersPageHeader
      headingId={pageHeadingId}
      actions={
        <Dialog.Root
          onOpenChange={(open) => {
            if (addPending) return
            setAddDialogOpen(open)
            if (!open) {
              setAddRole('member')
              setAddUserIds([])
            }
          }}
          open={addDialogOpen}
        >
          <Dialog.Trigger render={<Button.Container disabled={Boolean(data.error)} size="36" />}>
            <Button.Icon name="Plus" />
            <Button.Text>Add users</Button.Text>
          </Dialog.Trigger>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup size="l">
              <Dialog.Title>Add workspace users</Dialog.Title>
              <Dialog.Description>Choose the users to add to this workspace.</Dialog.Description>
              <addFetcher.Form
                className={styles.addForm}
                method="post"
                onSubmit={() => {
                  submittedAddUserIds.current = addUserIds
                }}
              >
                <input name="intent" type="hidden" value="add" />
                {addUserIds.map((userId) => (
                  <input key={userId} name="userId" type="hidden" value={userId} />
                ))}
                <input name="role" type="hidden" value={addRole} />
                <div className={styles.addFields}>
                  <div className={styles.addField}>
                    <Typography.BodyStrong as="span">Users</Typography.BodyStrong>
                    {availableUsers.length === 0 ? (
                      <Typography.Body variant="secondary">
                        Every user already has access.
                      </Typography.Body>
                    ) : (
                      <Combobox.Root
                        disabled={addPending}
                        items={availableUserLabels}
                        multiple
                        onValueChange={(value) => {
                          const labels = Array.isArray(value) ? value : []
                          setAddUserIds(
                            labels
                              .map((label) => candidateByLabel.get(label)?.userId)
                              .filter((userId): userId is string => userId !== undefined),
                          )
                        }}
                        value={addUserLabels}
                      >
                        <Combobox.InputGroup>
                          <Combobox.Chips>
                            <Combobox.Value>
                              {(selected) =>
                                Array.isArray(selected)
                                  ? selected.map((label) => (
                                      <Combobox.Chip key={label}>
                                        <Combobox.ChipLabel>
                                          {candidateByLabel.get(label)?.displayName || label}
                                        </Combobox.ChipLabel>
                                        <Combobox.ChipRemove aria-label={`Remove ${label}`} />
                                      </Combobox.Chip>
                                    ))
                                  : null
                              }
                            </Combobox.Value>
                            <Combobox.Input
                              bare
                              placeholder={addUserIds.length > 0 ? '' : 'Select users'}
                            />
                          </Combobox.Chips>
                        </Combobox.InputGroup>
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
                        className={styles.roleTrigger}
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
                <Dialog.Actions>
                  <Button.TextButton
                    disabled={addPending}
                    onClick={() => {
                      setAddDialogOpen(false)
                      setAddRole('member')
                      setAddUserIds([])
                    }}
                    variant="secondary"
                  >
                    Cancel
                  </Button.TextButton>
                  <Button.TextButton
                    disabled={addUserIds.length === 0 || addPending}
                    type="submit"
                    variant="primary"
                  >
                    {addPending ? 'Adding…' : addSubmitLabel(addUserIds.length, addRole)}
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
              <div className={styles.loadError}>
                <Typography.BodySmall role="alert" variant="error">
                  {data.error}
                </Typography.BodySmall>
                <Button.TextButton
                  disabled={retryPending}
                  onClick={() => void revalidator.revalidate()}
                  size="22"
                  type="button"
                  variant="secondary"
                >
                  {retryPending ? 'Retrying…' : 'Retry'}
                </Button.TextButton>
              </div>
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
                fetcherNamespace={fetcherNamespace}
                key={member.userId}
                member={member}
                mutationsDisabled={addPending}
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
  fetcherNamespace,
  member,
  mutationsDisabled,
  workspaceName,
}: {
  readonly currentUserId: string
  readonly fetcherNamespace: string
  readonly member: WorkspaceUser
  readonly mutationsDisabled: boolean
  readonly workspaceName: string
}) {
  const roleFetcher = useFetcher<WorkspaceUsersActionData>({
    key: `${fetcherNamespace}role:${member.userId}`,
  })
  const removalFetcher = useFetcher<WorkspaceUsersActionData>({
    key: `${fetcherNamespace}remove:${member.userId}`,
  })
  const [confirmingDemotion, setConfirmingDemotion] = useState(false)
  const [removalDialogOpen, setRemovalDialogOpen] = useState(false)
  const [showRemovalResult, setShowRemovalResult] = useState(false)
  const roleTriggerRef = useRef<HTMLButtonElement>(null)
  const rolePending = roleFetcher.state !== 'idle'
  const removalPending = removalFetcher.state !== 'idle'
  const roleError =
    !rolePending && roleFetcher.data?.status === 'error' ? roleFetcher.data.message : undefined
  const removalError =
    !removalPending && showRemovalResult && removalFetcher.data?.status === 'error'
      ? removalFetcher.data.message
      : undefined
  const rowError = roleError ?? (removalDialogOpen ? undefined : removalError)
  const controlsDisabled = rolePending || removalPending || mutationsDisabled

  useEffect(() => {
    if (
      removalFetcher.data?.status === 'success' &&
      removalFetcher.data.intent === 'remove' &&
      removalFetcher.data.userIds.includes(member.userId)
    ) {
      setRemovalDialogOpen(false)
      setShowRemovalResult(false)
    }
  }, [member.userId, removalFetcher.data])

  useEffect(() => {
    if (roleError) roleTriggerRef.current?.focus()
  }, [roleError])

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
                ref={roleTriggerRef}
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
            <Dialog.Title>
              {member.userId === currentUserId
                ? 'Remove yourself from this workspace?'
                : 'Remove workspace user?'}
            </Dialog.Title>
            <Dialog.Description>
              {member.userId === currentUserId
                ? `You will lose access to ${workspaceName} and be redirected.`
                : `${member.displayName || member.userId} will lose access to ${workspaceName}.`}
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
                <Button.TextButton disabled={removalPending} type="submit" variant="destructive">
                  <span role="status">
                    {member.userId === currentUserId
                      ? removalPending
                        ? 'Removing myself…'
                        : 'Remove myself'
                      : removalPending
                        ? 'Removing…'
                        : 'Remove user'}
                  </span>
                </Button.TextButton>
              </removalFetcher.Form>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
      <Dialog.Root
        open={confirmingDemotion}
        onOpenChange={(open) => {
          setConfirmingDemotion(open)
          if (!open) roleTriggerRef.current?.focus()
        }}
      >
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

/**
 * Whether an outcome answers the users this dialog last sent, and not an earlier set.
 * An outcome covers the users it applied to and the users it refused.
 */
function answersSubmission(
  result: WorkspaceUsersActionData,
  submitted: ReadonlyArray<string>,
): boolean {
  if (submitted.length === 0) return false

  const covered = new Set([
    ...result.userIds,
    ...(result.failures ?? []).map((failure) => failure.userId),
  ])

  return covered.size === submitted.length && submitted.every((userId) => covered.has(userId))
}

/**
 * The add outcome this dialog is waiting for, or nothing while the fetcher is busy,
 * the outcome belongs to another intent, or it answers an earlier set of users.
 */
function answeredAddResult(
  state: 'idle' | 'loading' | 'submitting',
  result: WorkspaceUsersActionData | undefined,
  submitted: ReadonlyArray<string>,
): WorkspaceUsersActionData | undefined {
  if (state !== 'idle' || result?.intent !== 'add') return undefined

  return answersSubmission(result, submitted) ? result : undefined
}

/** One line per refused user, so the toast names who was refused and why. */
function failureDescription(result: WorkspaceUsersActionData): React.ReactNode {
  if (!result.failures?.length) return undefined

  return result.failures.map((failure) => (
    <div key={failure.userId}>
      {failure.userId}: {failure.message}
    </div>
  ))
}

function addSubmitLabel(count: number, role: WorkspaceUserRole): string {
  const noun = roleLabel(role).toLowerCase()
  if (count <= 1) return `Add ${noun}`
  return `Add ${count} ${noun}s`
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
  headingId,
  search,
}: {
  readonly actions?: React.ReactNode
  readonly children: React.ReactNode
  readonly headingId?: string
  readonly search?: React.ReactNode
}) {
  return (
    <section className={styles.page}>
      <header className={styles.header}>
        <div className={styles.headerText}>
          <Typography.HeadingLarge as="h1" id={headingId} tabIndex={-1}>
            Users
          </Typography.HeadingLarge>
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

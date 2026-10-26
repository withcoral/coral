/** @vitest-environment happy-dom */
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ActionFunction } from 'react-router'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import type { MembershipView, OwnerView, PersonView, WorkspaceView } from './types'
import { WorkspaceMembersPage } from './workspace-members-page'

const workspace: WorkspaceView = {
  name: 'workspaces/demo',
  displayName: 'Demo',
  state: 'active',
  etag: 'etag',
  effectiveRole: 'owner',
  currentUser: 'users/ada',
  profile: person('ada', 'Ada'),
  mcpEndpoint: '',
  connectEndpoint: '',
  path: '/workspaces/demo',
}

function person(id: string, name: string): PersonView {
  return { user: `users/${id}`, name, email: '', local: false }
}

function membership(id: string, name: string, role: string, enabled = true) {
  return {
    name: `workspaces/demo/memberships/${id}`,
    user: `users/${id}`,
    profile: person(id, name),
    role,
    enabled,
    etag: id,
  } satisfies MembershipView
}

function owner(id: string, name: string, enabled = true): OwnerView {
  return { user: `users/${id}`, profile: person(id, name), enabled }
}

function render(
  props: {
    workspace: WorkspaceView
    memberships: MembershipView[]
    owners: OwnerView[]
    ownMembership: MembershipView | null
  },
  action?: ActionFunction,
) {
  renderRoute(
    <WorkspaceMembersPage
      {...props}
      nextMembers=""
      nextOwners=""
      candidates={null}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
    { entry: '/workspaces/demo/members', action },
  )
}

/** Each person the table lists: name, role, state, and whether you can act on them. */
function listed() {
  return within(screen.getByRole('table', { name: 'Members' }))
    .getAllByRole('row')
    .map((row) => within(row).queryAllByRole('cell'))
    .filter((cells) => cells.length > 0)
    .map(([name, , role, state, actions]) => [
      // The avatar beside the name shows its initial; the name is the longer text.
      within(name).getByText(/\w{2,}/).textContent,
      role.textContent,
      state.textContent,
      within(actions).queryByRole('button') !== null,
    ])
    .toSorted(([a], [b]) => String(a).localeCompare(String(b)))
}

const ada = membership('ada', 'Ada', 'owner')
const ben = membership('ben', 'Ben', 'member')
const dee = membership('dee', 'Dee', 'member')

// Catches: a member seeing the other members of the workspace, which only
// owners may list; and a disabled owner shown to a member as if enabled.
it('shows a member their own membership and the owners, and no one else', () => {
  render({
    workspace: { ...workspace, effectiveRole: 'member', currentUser: ben.user },
    memberships: [ada, ben, dee],
    owners: [owner('ada', 'Ada'), owner('cy', 'Cy', false)],
    ownMembership: ben,
  })
  expect(listed()).toEqual([
    ['Ada', 'Owner', 'Enabled', false],
    ['Ben (You)', 'Member', 'Enabled', true],
    ['Cy', 'Owner', 'Disabled', false],
  ])
})

// Catches: an owner listed twice, once as a member and once as an owner, or
// losing the membership that lets you change their role; and a disabled
// person shown as enabled, or losing their role on record.
it('lists each person once, and marks a disabled person in words', () => {
  render({
    workspace,
    memberships: [
      ada,
      ben,
      membership('cy', 'Cy', 'owner', false),
      membership('eve', 'Eve', 'member', false),
    ],
    owners: [owner('ada', 'Ada'), owner('cy', 'Cy', false)],
    ownMembership: ada,
  })
  expect(listed()).toEqual([
    ['Ada (You)', 'Owner', 'Enabled', true],
    ['Ben', 'Member', 'Enabled', true],
    ['Cy', 'Owner', 'Disabled', true],
    ['Eve', 'Member', 'Disabled', true],
  ])
})

// Catches: a finished change left on the page as a banner when the table already shows it.
it('closes the dialog after a role change and leaves the page without a banner', async () => {
  const message = 'Workspace role changed.'
  render({ workspace, memberships: [ada, ben], owners: [], ownMembership: ada }, () => ({
    error: null,
    credential: null,
    message,
  }))
  const user = userEvent.setup()
  await user.click(screen.getByRole('button', { name: 'Actions for Ben' }))
  await user.click(await screen.findByRole('menuitem', { name: 'Change role' }))
  const dialog = await screen.findByRole('dialog', { name: "Change Ben's role" })
  await user.click(within(dialog).getByRole('button', { name: 'Save role' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  expect(screen.queryByText(message)).toBeNull()
})

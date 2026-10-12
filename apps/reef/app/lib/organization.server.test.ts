import { expect, it } from 'vitest'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagement } from './management.test-support'
import {
  enabledOrganizationPeople,
  organizationWorkspaces,
  soleOwnerWorkspaces,
} from './organization.server'

const owner = (id: string, enabled = true) => ({ user: `users/${id}`, enabled })
const member = (id: string, enabled = true) => ({
  name: `organizationMemberships/${id}`,
  enabled,
  profile: { user: `users/${id}`, displayName: id },
})

// Catches: removing a person without a transfer when they alone own a
// workspace, because the list stopped at the first page or skipped archived
// workspaces; and asking for a transfer when another owner, even a disabled
// one, is still on record.
it('finds the workspaces each person alone owns, on every page and in every state', async () => {
  const pages = [
    [
      { displayName: 'Solo', state: WorkspaceState.ACTIVE, owners: [owner('ada')] },
      { displayName: 'Shared', state: WorkspaceState.ACTIVE, owners: [owner('ada'), owner('ben')] },
      {
        displayName: 'Beside a disabled owner',
        state: WorkspaceState.ACTIVE,
        owners: [owner('ada'), owner('cy', false)],
      },
    ],
    [
      { displayName: 'Archived solo', state: WorkspaceState.ARCHIVED, owners: [owner('ada')] },
      { displayName: 'Disabled solo', state: WorkspaceState.ACTIVE, owners: [owner('cy', false)] },
    ],
  ]
  const api = fakeManagement({
    organizationWorkspaces: {
      listOrganizationWorkspaces: ({ pageToken }) => ({
        workspaces: pages[Number(pageToken || 0)],
        nextPageToken: pageToken ? '' : '1',
      }),
    },
  })
  const owned = soleOwnerWorkspaces(await organizationWorkspaces(api))
  expect(Object.fromEntries(owned)).toEqual({
    'users/ada': ['Solo', 'Archived solo'],
    'users/cy': ['Disabled solo'],
  })
})

// Catches: offering a disabled member as an owner, which Coral refuses; and
// missing the people on later pages.
it('lists the enabled organization members on every page, by name', async () => {
  const api = fakeManagement({
    organizationMemberships: {
      listOrganizationMemberships: ({ pageToken }) =>
        pageToken
          ? { memberships: [member('ada')] }
          : { memberships: [member('eve'), member('cy', false)], nextPageToken: 'p2' },
    },
  })
  expect((await enabledOrganizationPeople(api)).map((p) => p.name)).toEqual([
    'organizationMemberships/ada',
    'organizationMemberships/eve',
  ])
})

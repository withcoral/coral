import type { OrganizationWorkspace } from '@/generated/coral/management/v1/organization_workspace_pb'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import type { OrganizationPersonView, OrganizationWorkspaceView } from '@/views/management/types'
import type { ManagementApi } from './management.server'
import { ownerView, profileView } from './management-views.server'

/**
 * Every enabled organization member, by name. Each can own a workspace. Only
 * an organization owner can list them.
 */
export async function enabledOrganizationPeople(
  api: Pick<ManagementApi, 'organizationMemberships'>,
): Promise<OrganizationPersonView[]> {
  const people: OrganizationPersonView[] = []
  let pageToken = ''
  do {
    const page = await api.organizationMemberships.listOrganizationMemberships({ pageToken })
    for (const m of page.memberships)
      if (m.enabled && m.profile?.user)
        people.push({ name: m.name, profile: profileView(m.profile, m.profile.user) })
    pageToken = page.nextPageToken
  } while (pageToken)
  return people.toSorted((a, b) => sortKey(a).localeCompare(sortKey(b)))
}

const sortKey = (p: OrganizationPersonView) => p.profile.name || p.profile.email

/** Every workspace in the organization. Only an organization owner can list them. */
export async function organizationWorkspaces(api: Pick<ManagementApi, 'organizationWorkspaces'>) {
  const workspaces: OrganizationWorkspace[] = []
  let pageToken = ''
  do {
    const page = await api.organizationWorkspaces.listOrganizationWorkspaces({
      pageSize: 100,
      pageToken,
    })
    workspaces.push(...page.workspaces)
    pageToken = page.nextPageToken
  } while (pageToken)
  return workspaces
}

/**
 * The workspaces each user alone owns, by user. Removing that user from the
 * organization needs another person to take them over. A disabled owner is
 * still an owner, so a workspace with one beside the user is not listed.
 */
export function soleOwnerWorkspaces(workspaces: readonly OrganizationWorkspace[]) {
  const owned = new Map<string, string[]>()
  for (const w of workspaces) {
    if (w.owners.length !== 1) continue
    const user = w.owners[0].user
    owned.set(user, [...(owned.get(user) ?? []), w.displayName])
  }
  return owned
}

export function organizationWorkspaceView(w: OrganizationWorkspace): OrganizationWorkspaceView {
  if (w.state !== WorkspaceState.ACTIVE && w.state !== WorkspaceState.ARCHIVED)
    throw new Response('Unrecognized management classification', { status: 503 })
  return {
    name: w.name,
    displayName: w.displayName,
    state: w.state === WorkspaceState.ACTIVE ? 'active' : 'archived',
    etag: w.etag,
    owners: w.owners.map(ownerView),
  }
}

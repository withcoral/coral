import { data } from 'react-router'
import type { Route } from './+types/organization-members'
import {
  actionFailure,
  assertPeopleManagement,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import {
  type OrganizationMembership,
  OrganizationRole,
} from '@/generated/coral/management/v1/organization_membership_pb'
import { OrganizationMembersPage } from '@/views/management/organization-members-page'
import { profileView } from '@/lib/management-views.server'
import {
  enabledOrganizationPeople,
  organizationWorkspaces,
  soleOwnerWorkspaces,
} from '@/lib/organization.server'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context }: Route.LoaderArgs) {
  assertPeopleManagement(context)
  try {
    const all = management(context)
    const api = all.organizationMemberships
    const [result, current, workspaces, candidates] = await Promise.all([
      api.listOrganizationMemberships({
        pageToken: new URL(request.url).searchParams.get('page') ?? '',
      }),
      api.getCurrentOrganizationMembership({}),
      organizationWorkspaces(all),
      enabledOrganizationPeople(all),
    ])
    const soleOwner = soleOwnerWorkspaces(workspaces)
    return data(
      {
        memberships: result.memberships.map((m) => ({
          name: m.name,
          etag: m.etag,
          profile: memberProfile(m),
          role:
            m.role === OrganizationRole.OWNER
              ? 'Owner'
              : m.role === OrganizationRole.MEMBER
                ? 'Member'
                : 'Unknown',
          enabled: m.enabled,
          soleOwnerOf: soleOwner.get(memberProfile(m).user) ?? [],
        })),
        candidates,
        currentUser: memberProfile(current).user,
        nextPage: result.nextPageToken,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

function memberProfile(membership: OrganizationMembership) {
  const profile = membership.profile
  if (!profile?.user) throw new Response('Missing organization member profile', { status: 503 })
  return profileView(profile, profile.user)
}

export async function action({ request, context }: Route.ActionArgs) {
  assertPeopleManagement(context)
  const form = await mutationForm(request, context)
  if (field(form, 'intent') !== 'removeOrganizationMember')
    throw new Response('Invalid action', { status: 400 })
  // The person who takes over the workspaces this member alone owns, if any.
  const transferTo = form.has('transferTo') ? field(form, 'transferTo') : ''
  if (transferTo && !/^organizationMemberships\/[^/]+$/.test(transferTo))
    throw new Response('Invalid target', { status: 400 })
  try {
    await management(context).organizationMemberships.deleteOrganizationMembership({
      name: field(form, 'name'),
      etag: field(form, 'etag'),
      transferTo,
    })
    return data(
      {
        error: null,
        credential: null,
        message: 'This person no longer has access to your organization.',
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    return actionFailure(error, 'Coral cannot remove this organization membership. ')
  }
}

export default function OrganizationMembersRoute({ loaderData }: Route.ComponentProps) {
  return <OrganizationMembersPage {...loaderData} />
}

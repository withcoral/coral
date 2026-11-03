import { Code, ConnectError } from '@connectrpc/connect'
import { data, redirect, useLocation, useNavigation } from 'react-router'
import type { Route } from './+types/workspace-members'
import {
  actionFailure,
  assertPeopleManagement,
  failureReason,
  field,
  formContext,
  management,
  type ManagementApi,
  managementFailure,
  membershipRole,
  mutationForm,
  privateHeaders,
  workspaceName,
  requestIdFor,
} from '@/lib/management.server'
import { membershipView, ownerView } from '@/lib/management-views.server'
import { enabledOrganizationPeople } from '@/lib/organization.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { WorkspaceMembersPage } from '@/views/management/workspace-members-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export function shouldRevalidate() {
  return true
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  assertPeopleManagement(context)
  try {
    const { api, workspace: w } = await authorizedWorkspace(context, params.workspace)
    const owner = w.effectiveRole === 'owner'
    const query = new URL(request.url).searchParams
    const [members, owners, own, people] = await Promise.all([
      owner
        ? api.memberships.listWorkspaceMemberships({
            parent: w.name,
            pageToken: query.get('members') ?? '',
          })
        : null,
      api.memberships.listWorkspaceOwners({ parent: w.name, pageToken: query.get('owners') ?? '' }),
      owner
        ? null
        : api.memberships.getWorkspaceMembership({
            name: `${w.name}/memberships/${w.currentUser.slice('users/'.length)}`,
          }),
      owner ? candidatesFor(api, w.name) : null,
    ])
    return data(
      {
        workspace: w,
        memberships: members?.memberships.map(membershipView) ?? [],
        owners: owners.owners.map(ownerView),
        ownMembership: own ? membershipView(own) : null,
        nextMembers: members?.nextPageToken ?? '',
        nextOwners: owners.nextPageToken,
        candidates: people,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  assertPeopleManagement(context)
  const form = await mutationForm(request, context)
  const api = management(context)
  const name = workspaceName(params.workspace)
  const intent = field(form, 'intent')
  try {
    switch (intent) {
      case 'addMember': {
        const role = membershipRole(field(form, 'role'))
        const requestId = field(form, 'requestId')
        const fields = form.getAll('email')
        if (fields.length === 0 || fields.some((e) => typeof e !== 'string' || e.length > 320))
          throw new Response('Invalid form', { status: 400 })
        const emails = [...new Set(fields as string[])]
        const added: string[] = []
        const refused: { email: string; status: number; message: string }[] = []
        for (const email of emails) {
          try {
            const person = await api.memberships.createWorkspaceMembership({
              parent: name,
              email,
              role,
              requestId: emails.length === 1 ? requestId : requestIdFor(requestId, email),
            })
            added.push(person.profile?.displayName || person.profile?.email || 'Person')
          } catch (error) {
            const [status, message] = failureReason(error, ADD_FAILED, ADD_NOT_FOUND)
            refused.push({ email, status, message })
          }
        }
        if (refused.length === 0)
          return data(
            {
              error: null,
              credential: null,
              message:
                added.length === 1
                  ? `${added[0]} added to the workspace.`
                  : `${added.length} people added to the workspace.`,
            },
            { headers: privateHeaders },
          )
        return data(
          {
            error:
              emails.length === 1
                ? refused[0].message
                : [
                    added.length > 0 && `Coral added ${added.join(', ')}.`,
                    ...refused.map((r) => `${r.email}: ${r.message}`),
                  ]
                    .filter(Boolean)
                    .join(' '),
            credential: null,
            refused: refused.map((r) => r.email),
          },
          { status: refused[0].status, headers: privateHeaders },
        )
      }
      case 'membership':
      case 'leave': {
        const target = field(form, 'name')
        if (
          !target.startsWith(`${name}/memberships/`) ||
          target.slice(`${name}/memberships/`.length).includes('/')
        )
          throw new Response('Invalid target', { status: 400 })
        if (intent === 'leave' || field(form, 'operation') === 'remove') {
          await api.memberships.deleteWorkspaceMembership({
            name: target,
            etag: field(form, 'etag'),
          })
          try {
            await api.workspaces.getWorkspace({ name })
          } catch (error) {
            if (error instanceof ConnectError && error.code === Code.NotFound)
              return redirect('/', { headers: privateHeaders })
            throw error
          }
          return data(
            {
              error: null,
              credential: null,
              message: 'Removed from the workspace.',
            },
            { headers: privateHeaders },
          )
        }
        await api.memberships.updateWorkspaceMembership({
          membership: {
            name: target,
            role: membershipRole(field(form, 'role')),
            etag: field(form, 'etag'),
          },
          updateMask: { paths: ['role'] },
        })
        return data(
          {
            error: null,
            credential: null,
            message: 'Workspace role changed.',
          },
          { headers: privateHeaders },
        )
      }
      default:
        throw new Response('Invalid action', { status: 400 })
    }
  } catch (error) {
    const failures: Record<string, string> = {
      addMember: ADD_FAILED,
      membership: "Coral could not change this person's access. ",
      leave: 'Coral could not remove you from the workspace. ',
    }
    return actionFailure(
      error,
      failures[intent],
      intent === 'addMember' ? ADD_NOT_FOUND : undefined,
    )
  }
}

const ADD_FAILED = 'Coral could not add this person. '
const ADD_NOT_FOUND =
  'Coral cannot find this person or workspace. Check the email address. Check that you can access the workspace.'

/**
 * The people a workspace owner can add: enabled organization members with an
 * email who are not designated members on any page of the workspace. Only
 * organization owners can list them, so other workspace owners get null and
 * type an email instead.
 */
async function candidatesFor(api: ManagementApi, workspace: string) {
  const people = await organizationPeople(api)
  if (!people) return null
  const members = await memberUsers(api, workspace)
  return people.filter((p) => !members.has(p.user))
}

async function memberUsers(api: ManagementApi, workspace: string) {
  const users = new Set<string>()
  let pageToken = ''
  do {
    const page = await api.memberships.listWorkspaceMemberships({ parent: workspace, pageToken })
    for (const m of page.memberships) users.add(m.user)
    pageToken = page.nextPageToken
  } while (pageToken)
  return users
}

async function organizationPeople(api: ManagementApi) {
  try {
    const people = await enabledOrganizationPeople(api)
    return people.map((p) => p.profile).filter((p) => p.email)
  } catch (error) {
    if (error instanceof ConnectError && error.code === Code.PermissionDenied) return null
    throw error
  }
}

export default function WorkspaceMembersRoute({ loaderData, actionData }: Route.ComponentProps) {
  return (
    <WorkspaceMembersPage
      key={useLocation().key}
      {...loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

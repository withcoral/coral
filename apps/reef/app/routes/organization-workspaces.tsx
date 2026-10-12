import { ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/organization-workspaces'
import {
  actionFailure,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { enabledOrganizationPeople, organizationWorkspaceView } from '@/lib/organization.server'
import { OrganizationWorkspacesPage } from '@/views/management/organization-workspaces-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

/** Only an organization owner can list every workspace; Coral denies anyone else. */
export async function loader({ request, context }: Route.LoaderArgs) {
  try {
    const api = management(request, context)
    const [result, current, candidates] = await Promise.all([
      api.organizationWorkspaces.listOrganizationWorkspaces({
        pageToken: new URL(request.url).searchParams.get('page') ?? '',
      }),
      api.organizationMemberships.getCurrentOrganizationMembership({}),
      enabledOrganizationPeople(api),
    ])
    return data(
      {
        workspaces: result.workspaces.map(organizationWorkspaceView),
        candidates,
        currentUser: current.profile?.user ?? '',
        nextPage: result.nextPageToken,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

const done: Record<string, string> = {
  assignOwner: 'Owner assigned.',
  archiveWorkspace: 'Workspace archived.',
  restoreWorkspace: 'Workspace restored. Other access restrictions still apply.',
}
const failed: Record<string, string> = {
  assignOwner: 'Coral could not assign this owner. ',
  archiveWorkspace: 'Coral could not archive the workspace. ',
  restoreWorkspace: 'Coral could not restore the workspace. ',
}

export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const intent = field(form, 'intent')
  if (!Object.hasOwn(done, intent)) throw new Response('Invalid action', { status: 400 })
  const name = field(form, 'name')
  if (!/^workspaces\/[^/]+$/.test(name)) throw new Response('Invalid target', { status: 400 })
  const api = management(request, context).organizationWorkspaces
  try {
    if (intent === 'assignOwner') {
      const owner = field(form, 'owner')
      if (!/^organizationMemberships\/[^/]+$/.test(owner))
        throw new Response('Invalid target', { status: 400 })
      await api.assignWorkspaceOwner({ name, owner, requestId: field(form, 'requestId') })
    } else if (intent === 'archiveWorkspace') {
      await api.archiveOrganizationWorkspace({ name, etag: field(form, 'etag') })
    } else {
      await api.restoreOrganizationWorkspace({ name, etag: field(form, 'etag') })
    }
    return data(
      { error: null, credential: null, message: done[intent] },
      { headers: privateHeaders },
    )
  } catch (error) {
    // An organization owner restores here, not in the workspace's settings.
    if (error instanceof ConnectError && error.rawMessage === 'WORKSPACE_ARCHIVED')
      return data(
        {
          error: 'This workspace is archived. Restore it before you assign an owner.',
          credential: null,
        },
        { status: 409, headers: privateHeaders },
      )
    return actionFailure(error, failed[intent])
  }
}

export default function OrganizationWorkspacesRoute({ loaderData }: Route.ComponentProps) {
  return <OrganizationWorkspacesPage {...loaderData} />
}

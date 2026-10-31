import { ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/organization-workspaces'
import {
  actionFailure,
  failureReason,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  requestIdFor,
} from '@/lib/management.server'
import { enabledOrganizationPeople, organizationWorkspaceView } from '@/lib/organization.server'
import { OrganizationWorkspacesPage } from '@/views/management/organization-workspaces-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

/** Only an organization owner can list every workspace; Coral denies anyone else. */
export async function loader({ request, context }: Route.LoaderArgs) {
  try {
    const api = management(context)
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
  const api = management(context).organizationWorkspaces
  try {
    if (intent === 'assignOwner') {
      const fields = form.getAll('owner')
      if (
        fields.length === 0 ||
        fields.some(
          (o) =>
            typeof o !== 'string' || o.length > 4096 || !/^organizationMemberships\/[^/]+$/.test(o),
        )
      )
        throw new Response('Invalid target', { status: 400 })
      const owners = [...new Set(fields as string[])]
      // The workspace is in the key: every row on the page shares the form's key.
      const requestIds = owners.map((o) => requestIdFor(field(form, 'requestId'), `${name}\0${o}`))
      const refused: { owner: string; status: number; message: string }[] = []
      for (const [i, owner] of owners.entries()) {
        try {
          await api.assignWorkspaceOwner({ name, owner, requestId: requestIds[i] })
        } catch (error) {
          const [status, message] =
            error instanceof ConnectError && error.rawMessage === 'WORKSPACE_ARCHIVED'
              ? [409, 'This workspace is archived. Restore it before you assign an owner.']
              : failureReason(error, failed.assignOwner)
          refused.push({ owner, status, message })
          // Only a refusal of this one person lets the others go ahead.
          if (!(error instanceof ConnectError && error.rawMessage === 'OWNER_TARGET_NOT_ENABLED')) {
            refused.push(...owners.slice(i + 1).map((o) => ({ owner: o, status, message })))
            break
          }
        }
      }
      if (refused.length > 0)
        return data(
          {
            error:
              owners.length === 1
                ? refused[0].message
                : [
                    `Coral could not assign ${refused.length} of ${owners.length} owners.`,
                    ...new Set(refused.map((r) => r.message)),
                  ].join(' '),
            credential: null,
            refused: refused.map((r) => r.owner),
          },
          { status: refused[0].status, headers: privateHeaders },
        )
      return data(
        {
          error: null,
          credential: null,
          message: owners.length === 1 ? 'Owner assigned.' : `${owners.length} owners assigned.`,
        },
        { headers: privateHeaders },
      )
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
    return actionFailure(error, failed[intent])
  }
}

export default function OrganizationWorkspacesRoute({ loaderData }: Route.ComponentProps) {
  return <OrganizationWorkspacesPage {...loaderData} />
}

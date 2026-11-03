import { WorkspaceState, type Workspace } from '@/generated/coral/management/v1/workspace_pb'
import type { RouterContextProvider } from 'react-router'
import { workspaceView } from './management-views.server'
import { management, privateHeaders, workspaceName } from './management.server'

/**
 * Every Workspace page starts here: it authorizes the current User against the
 * Workspace and returns the view. Owner-only pages are never linked for a
 * Member, so reaching one directly is a permission error, not a redirect that
 * would also swallow a denied mutation's status.
 */
export async function authorizedWorkspace(
  context: Readonly<RouterContextProvider>,
  id: string,
  options: { ownerOnly?: boolean } = {},
) {
  const api = management(context)
  const workspace = workspaceView(await api.workspaces.getWorkspace({ name: workspaceName(id) }))
  if (options.ownerOnly && workspace.effectiveRole !== 'owner')
    throw new Response('Forbidden', { status: 403, headers: privateHeaders })
  return { api, workspace }
}

/** Every Workspace the caller is a member of, active or archived, in listing order.
 * The sidebar and the landing page both need the whole list, so this reads every page. */
export async function memberWorkspaces(
  api: Pick<ReturnType<typeof management>, 'workspaces'>,
  signal?: AbortSignal,
) {
  const workspaces: Workspace[] = []
  let pageToken = ''
  do {
    const page = await api.workspaces.listWorkspaces({ pageSize: 100, pageToken }, { signal })
    workspaces.push(...page.workspaces)
    pageToken = page.nextPageToken
  } while (pageToken)
  return workspaces
}

/** Every active Workspace the caller can open, in listing order. */
export async function activeWorkspaces(
  api: Pick<ReturnType<typeof management>, 'workspaces'>,
  signal?: AbortSignal,
) {
  return (await memberWorkspaces(api, signal)).filter((w) => w.state === WorkspaceState.ACTIVE)
}

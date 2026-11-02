import { ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/approvals-history'
import { settledApprovals } from '@/lib/activity.server'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'

/** The Approvals page's next page of settled approvals, after the `after` that the last page
 * returned. The page's loader reads the newest page itself. A page Coral cannot read comes
 * back as `unavailable`, so the page keeps the rows it shows and offers a retry. */
export async function loader({ request, context, params }: Route.LoaderArgs) {
  const after = new URL(request.url).searchParams.get('after')
  if (!after) throw new Response('Bad request', { status: 400, headers: privateHeaders })
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    if (workspace.state === 'archived')
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    return data(await settledApprovals(api, workspace.name, after, request.signal), {
      headers: privateHeaders,
    })
  } catch (error) {
    if (error instanceof ConnectError)
      return data({ unavailable: true as const }, { status: 503, headers: privateHeaders })
    managementFailure(error)
  }
}

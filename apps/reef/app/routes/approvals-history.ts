import { data } from 'react-router'
import type { Route } from './+types/approvals-history'
import { settledApprovals, transient } from '@/lib/activity.server'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'

/** The Approvals page's next page of settled approvals, after the `after` that the last page
 * returned. The page's loader reads the newest page itself. A page that a passing Coral fault
 * keeps from loading comes back as `unavailable`, so the page keeps its rows and offers a
 * retry. A refusal, such as a lost membership, fails as any read does. */
export async function loader({ request, context, params }: Route.LoaderArgs) {
  const after = new URL(request.url).searchParams.get('after')
  if (!after) throw new Response('Bad request', { status: 400, headers: privateHeaders })
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    if (workspace.state === 'archived')
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    try {
      return data(await settledApprovals(api, workspace.name, after, request.signal), {
        headers: privateHeaders,
      })
    } catch (error) {
      if (!transient(error)) throw error
      return data({ unavailable: true as const }, { status: 503, headers: privateHeaders })
    }
  } catch (error) {
    managementFailure(error)
  }
}

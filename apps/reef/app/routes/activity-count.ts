import type { Route } from './+types/activity-count'
import { requestAuthContext } from '@/auth/server-context'
import { assertManagementOrigin, management, privateHeaders } from '@/lib/management.server'
import { activityStream } from '@/lib/activity-stream.server'
import { loadActionCounts } from '@/lib/action-count.server'
import { activeWorkspaces } from '@/lib/workspace-route.server'

export function action({ request, context }: Route.ActionArgs) {
  if (request.method !== 'POST')
    throw new Response('Method Not Allowed', { status: 405, headers: privateHeaders })
  assertManagementOrigin(request, context)
  const api = management(context)
  const auth = context.get(requestAuthContext)
  return activityStream(
    request,
    api,
    // Each change rereads the Workspace list, so a new or archived Workspace updates too.
    async (signal) => ({
      kind: 'count',
      counts: await loadActionCounts(api, await activeWorkspaces(api, signal), signal),
    }),
    auth.mode === 'required' ? auth.session.expiresAt : Infinity,
  )
}

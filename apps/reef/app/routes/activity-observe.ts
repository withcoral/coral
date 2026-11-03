import type { Route } from './+types/activity-observe'
import { requestAuthContext } from '@/auth/server-context'
import { assertManagementOrigin, management, privateHeaders } from '@/lib/management.server'
import { observeActivity } from '@/lib/activity-observation.server'

export async function action({ request, context }: Route.ActionArgs) {
  // This POST only observes. Same-origin browser requests prevent cross-site stream allocation.
  if (request.method !== 'POST')
    throw new Response('Method Not Allowed', { status: 405, headers: privateHeaders })
  assertManagementOrigin(request, context)
  const auth = context.get(requestAuthContext)
  return observeActivity(
    request,
    management(context),
    auth.mode === 'required' ? auth.session.expiresAt : Infinity,
  )
}

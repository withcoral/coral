import type { Route } from './+types/activity-summary'
import { requestAuthContext } from '@/auth/server-context'
import {
  assertManagementOrigin,
  management,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { activityStream } from '@/lib/activity-stream.server'
import { summaryDays } from '@/lib/activity-summary'
import { loadActivitySummary } from '@/lib/activity-summary.server'

const invalid = () => new Response('Bad Request', { status: 400, headers: privateHeaders })

export async function action({ request, context }: Route.ActionArgs) {
  if (request.method !== 'POST')
    throw new Response('Method Not Allowed', { status: 405, headers: privateHeaders })
  assertManagementOrigin(request, context)
  const text = await request.text()
  if (text.length > 1024) throw invalid()
  let input: { workspace?: unknown; days?: unknown } | null
  try {
    input = JSON.parse(text) as typeof input
  } catch {
    throw invalid()
  }
  const days = summaryDays(input?.days)
  if (typeof input?.workspace !== 'string' || days === undefined) throw invalid()
  const workspace = workspaceName(input.workspace)
  const api = management(request, context)
  const auth = context.get(requestAuthContext)
  return activityStream(
    request,
    api,
    async (signal) => ({
      kind: 'summary',
      summary: await loadActivitySummary(api, workspace, days, signal),
    }),
    auth.mode === 'required' ? auth.session.expiresAt : Infinity,
  )
}

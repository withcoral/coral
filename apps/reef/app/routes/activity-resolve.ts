import { ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import type { Route } from './+types/activity-resolve'
import { ActivityRunState } from '@/generated/coral/management/v1/activity_pb'
import { connectionState } from '@/lib/accounts.server'
import {
  actionFailure,
  field,
  management,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'

/**
 * Sends a cancellation or a decision, and sends it once more if Coral has not recorded it yet.
 * Coral waits for the record before it answers, so the second send follows at once. Coral keys
 * the request by its Run or Approval Request, and the first one wins, so the second send is
 * safe. Undefined means Coral still holds it: the live read shows the outcome.
 */
async function untilRecorded<Result>(send: () => Promise<Result>) {
  const attempt = () =>
    send().catch((error: unknown) => {
      if (error instanceof ConnectError && error.rawMessage === 'RUN_REQUEST_NOT_YET_RECORDED')
        return undefined
      throw error
    })
  return (await attempt()) ?? (await attempt())
}
const recording = () => data({ recording: true as const }, { status: 202, headers: privateHeaders })

export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    const api = management(request, context).activity
    const name = field(form, 'name')
    const intent = field(form, 'intent')
    if (intent === 'cancel') {
      const result = await untilRecorded(() => api.cancelActivityRun({ name }))
      if (!result) return recording()
      const messages: Partial<Record<ActivityRunState, string>> = {
        [ActivityRunState.RUNNING]: 'Cancellation requested. Check the current run state.',
        [ActivityRunState.CANCELED]: 'The run is canceled.',
        [ActivityRunState.SUCCEEDED]: 'The run already succeeded.',
        [ActivityRunState.FAILED]: 'The run already failed.',
        [ActivityRunState.TIMED_OUT]: 'The run timed out.',
        [ActivityRunState.INTERRUPTED]: 'The run was interrupted.',
      }
      return data(
        { message: messages[result.state] ?? 'Check the current run state.' },
        { headers: privateHeaders },
      )
    }
    if (intent === 'decline_connection') {
      // Only the invoking User can decline; Coral conceals anyone else's request.
      const result = await management(request, context).accounts.resolveAccountConnectionRequest({
        name,
        decision: { case: 'decline', value: {} },
      })
      return data({ connection: connectionState(result.state) }, { headers: privateHeaders })
    }
    if (intent !== 'approve' && intent !== 'decline')
      throw new Response('Invalid action', { status: 400, headers: privateHeaders })
    // Queue rows decide without a comment field; review forms always send one.
    const comment = (form.has('comment') && field(form, 'comment')) || undefined
    const result = await untilRecorded(() =>
      api.decideApproval({ name, decision: intent, comment }),
    )
    if (!result) return recording()
    // The view words the state. Another decision may have won, so this is not the request echoed.
    return data({ state: result.state }, { headers: privateHeaders })
  } catch (error) {
    return actionFailure(error)
  }
}

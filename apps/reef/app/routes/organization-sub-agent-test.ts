import type { Route } from './+types/organization-sub-agent-test'
import { SubAgentModelTestOutcome } from '@/generated/coral/management/v1/sub_agent_model_pb'
import { failureReason, management, mutationForm, privateHeaders } from '@/lib/management.server'
import { enteredModel, settingRefusal } from '@/lib/sub-agent-model.server'
import type { ModelTestResult } from '@/views/sub-agent/types'

/** Coral waits up to 45 seconds for the model. */
const TEST_TIMEOUT_MS = 60_000

/**
 * Tests what is in the page's form. Only an organization owner can; Coral denies anyone else.
 * The form posts here into a frame of the page, so the answer is a document that holds only the
 * result, and never a value of the form.
 */
export async function action({ request, context }: Route.ActionArgs) {
  const { settings, apiKey, refusal } = enteredModel(await mutationForm(request, context))
  if (refusal) return answer({ outcome: 'invalid', ...refusal }, 400)
  try {
    const { outcome, httpStatus } = await management(context).subAgentModel.testSubAgentModel(
      { settings, apiKey },
      { timeoutMs: TEST_TIMEOUT_MS, signal: request.signal },
    )
    return answer(
      outcome === SubAgentModelTestOutcome.REFUSED && httpStatus !== undefined
        ? { outcome: 'refused', status: httpStatus }
        : { outcome: outcomes[outcome] ?? 'unknown' },
    )
  } catch (error) {
    const [status, message] = failureReason(error, '')
    const refused = settingRefusal(error)
    return answer(
      refused ? { outcome: 'invalid', ...refused } : { outcome: 'failed', message },
      status,
    )
  }
}

const outcomes: Partial<
  Record<SubAgentModelTestOutcome, 'answered' | 'unreachable' | 'forbidden' | 'unreadable'>
> = {
  [SubAgentModelTestOutcome.ANSWERED]: 'answered',
  [SubAgentModelTestOutcome.UNREACHABLE]: 'unreachable',
  [SubAgentModelTestOutcome.FORBIDDEN]: 'forbidden',
  [SubAgentModelTestOutcome.UNREADABLE]: 'unreadable',
}

// As text, since a browser can show JSON in a viewer of its own that the page cannot read.
function answer(result: ModelTestResult, status = 200) {
  return new Response(JSON.stringify(result), {
    status,
    headers: {
      ...privateHeaders,
      'Content-Type': 'text/plain; charset=utf-8',
      'X-Content-Type-Options': 'nosniff',
    },
  })
}

import { data, redirect } from 'react-router'

import { COMPLETE_ONBOARDING_INTENT, type CompleteGuiOnboardingError } from '@/lib/gui-onboarding'
import { errorMessage } from '@/lib/utils'
import { impliedWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

/**
 * Lands on the workspace traces once onboarding is walked through. Onboarding is
 * disabled, so nothing is recorded here. A missing workspace is a route
 * `Response`, so it keeps its own status instead of becoming a completion error.
 */
export async function runCompleteOnboardingAction(request: Request, accessToken: string | null) {
  try {
    const workspace = await impliedWorkspaceForRequest(request, accessToken)
    return redirect(routePath('workspaceTraces', { workspaceId: workspace.name }))
  } catch (error) {
    if (error instanceof Response) throw error

    return data(
      {
        intent: COMPLETE_ONBOARDING_INTENT,
        message: errorMessage(error),
        status: 'error',
      } satisfies CompleteGuiOnboardingError,
      { status: 500 },
    )
  }
}

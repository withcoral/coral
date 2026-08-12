import { data, redirect } from 'react-router'

import { completeGuiOnboarding } from '@/lib/gui-onboarding.server'
import { COMPLETE_ONBOARDING_INTENT, type CompleteGuiOnboardingError } from '@/lib/gui-onboarding'
import { errorMessage } from '@/lib/utils'
import { firstWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

/**
 * Marks onboarding complete, then lands on the workspace traces. A missing
 * workspace is a route `Response`, so it keeps its own status instead of
 * becoming a completion error.
 */
export async function runCompleteOnboardingAction(request: Request, accessToken: string | null) {
  try {
    const workspace = await firstWorkspaceForRequest(request, accessToken)
    await completeGuiOnboarding(request, accessToken)
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

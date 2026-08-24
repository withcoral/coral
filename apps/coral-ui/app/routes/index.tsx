import type { Route } from './+types/index'

import { replace } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import { rethrowAsCoralUnavailableRouteError } from '@/lib/coral-unavailable.server'
import { getGuiOnboardingCompleted } from '@/lib/gui-onboarding.server'
import { redirectToFirstWorkspaceTraces } from '@/lib/workspace-redirect.server'

export async function loader({ context, request }: Route.LoaderArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  try {
    if (!(await getGuiOnboardingCompleted(request, accessToken))) return replace('/onboarding')

    return await redirectToFirstWorkspaceTraces(request, accessToken)
  } catch (error) {
    rethrowAsCoralUnavailableRouteError(request, error)
  }
}

export default function AppIndex() {
  return null
}

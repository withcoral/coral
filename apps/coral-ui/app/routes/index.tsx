import type { Route } from './+types/index'

import { replace } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import { getGuiOnboardingCompleted } from '@/lib/gui-onboarding.server'
import { redirectToImpliedWorkspaceTraces } from '@/lib/workspace-redirect.server'

export async function loader({ context, request }: Route.LoaderArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  if (!(await getGuiOnboardingCompleted(request, accessToken))) return replace('/onboarding')

  return redirectToImpliedWorkspaceTraces(request, accessToken)
}

export default function AppIndex() {
  return null
}

import type { Route } from './+types/onboarding'
import type { SourcesActionData } from './sources-action'

import { requestAuthContext } from '@/auth/server-context'
import { getOnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { loadOnboardingSampleQuery } from '@/lib/onboarding-query.server'
import { firstWorkspaceForRequest } from '@/lib/workspaces.server'
import { OnboardingView } from '@/views/onboarding/onboarding'

import { runSourcesAction } from './sources-action'
import { loadSourcesRouteData } from './sources-loader'

export async function loader({ context, request }: Route.LoaderArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  const workspace = await firstWorkspaceForRequest(request, accessToken)
  const sources = await loadSourcesRouteData(request, workspace, accessToken)
  const step = getOnboardingStepState(new URL(request.url).searchParams.get('step'))
  const shouldRunSampleQuery =
    step.step === 'query' &&
    sources.loadError === null &&
    sources.entries.some((entry) => entry.installed)

  return {
    ...sources,
    sampleQuery: shouldRunSampleQuery
      ? loadOnboardingSampleQuery(request, accessToken, workspace.name)
      : null,
    step,
    workspaceId: workspace.name,
  }
}

export async function action({ context, request }: Route.ActionArgs): Promise<SourcesActionData> {
  const accessToken = context.get(requestAuthContext).accessToken
  return runSourcesAction(
    request,
    await firstWorkspaceForRequest(request, accessToken),
    accessToken,
  )
}

export default function OnboardingRoute({ actionData, loaderData }: Route.ComponentProps) {
  return <OnboardingView actionData={actionData} loaderData={loaderData} />
}

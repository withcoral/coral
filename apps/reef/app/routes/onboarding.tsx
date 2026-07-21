import type { Route } from './+types/onboarding'
import type { SourcesActionData } from './sources-action'

import { getOnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { loadOnboardingSampleQuery } from '@/lib/onboarding-query.server'
import { firstWorkspaceForRequest } from '@/lib/workspaces.server'
import { OnboardingView } from '@/views/onboarding/onboarding'

import { runSourcesAction } from './sources-action'
import { loadSourcesRouteData } from './sources-loader'

export async function loader({ request }: Route.LoaderArgs) {
  const workspace = await firstWorkspaceForRequest(request)
  const sources = await loadSourcesRouteData(request, workspace)
  const step = getOnboardingStepState(new URL(request.url).searchParams.get('step'))
  const shouldRunSampleQuery =
    step.step === 'query' &&
    sources.loadError === null &&
    sources.entries.some((entry) => entry.installed)

  return {
    ...sources,
    runtime: isCoralDesktopBuild() ? ('desktop' as const) : ('web' as const),
    sampleQuery: shouldRunSampleQuery ? loadOnboardingSampleQuery(request, workspace.name) : null,
    step,
    workspaceId: workspace.name,
  }
}

export async function action({ request }: Route.ActionArgs): Promise<SourcesActionData> {
  return runSourcesAction(request, await firstWorkspaceForRequest(request))
}

export default function OnboardingRoute({ actionData, loaderData }: Route.ComponentProps) {
  return <OnboardingView actionData={actionData} loaderData={loaderData} />
}

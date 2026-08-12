import type { Route } from './+types/onboarding'
import type { SourcesActionData } from './sources-action'

import { requestAuthContext } from '@/auth/server-context'
import { getOnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { loadOnboardingSampleQuery } from '@/lib/onboarding-query.server'
import { firstWorkspaceForRequest, listWorkspacesForRequest } from '@/lib/workspaces.server'
import { OnboardingView } from '@/views/onboarding/onboarding'

import { runSourcesAction } from './sources-action'
import { loadSourcesRouteData } from './sources-loader'

export async function loader({ context, request }: Route.LoaderArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  const workspaces = await listWorkspacesForRequest(request, accessToken)
  const [workspace] = workspaces
  if (!workspace) {
    throw new Response('No Coral workspace is configured.', {
      status: 404,
      statusText: 'Workspace Not Found',
    })
  }

  const sources = await loadSourcesRouteData(request, workspace, accessToken)
  const step = getOnboardingStepState(new URL(request.url).searchParams.get('step'))
  const shouldRunSampleQuery =
    step.step === 'query' &&
    sources.loadError === null &&
    sources.entries.some((entry) => entry.installed)

  return {
    ...sources,
    runtime: isCoralDesktopBuild() ? ('desktop' as const) : ('web' as const),
    sampleQuery: shouldRunSampleQuery
      ? loadOnboardingSampleQuery(request, accessToken, workspace.name)
      : null,
    step,
    workspaceId: workspace.name,
    workspaces: workspaces.map(({ name }) => ({ name })),
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

import type { Route } from './+types/onboarding'

import { useFetcher } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import { getOnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { COMPLETE_ONBOARDING_INTENT } from '@/lib/gui-onboarding'
import { loadOnboardingSampleQuery } from '@/lib/onboarding-query.server'
import { firstWorkspaceForRequest, listWorkspacesForRequest } from '@/lib/workspaces.server'
import { OnboardingView } from '@/views/onboarding/onboarding'
import { addToast } from '@/wax/components/toast'

import { runCompleteOnboardingAction } from './onboarding-action'
import { runSourcesAction } from './sources-action'
import { loadSourcesRouteData } from './sources-loader'
import {
  loadDesktopMcpClients,
  updateDesktopMcpClient,
  type DesktopMcpClientData,
} from './settings-loader'

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

export async function action({ context, request }: Route.ActionArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  const intent = (await request.clone().formData()).get('intent')

  if (intent === COMPLETE_ONBOARDING_INTENT) {
    return runCompleteOnboardingAction(request, accessToken)
  }

  return runSourcesAction(
    request,
    await firstWorkspaceForRequest(request, accessToken),
    accessToken,
  )
}

export async function clientLoader({ serverLoader }: Route.ClientLoaderArgs) {
  const loaderData = await serverLoader()
  if (loaderData.runtime !== 'desktop') return loaderData

  return {
    ...loaderData,
    mcpClients: await loadDesktopMcpClients(),
  }
}

clientLoader.hydrate = true as const

export async function clientAction({ request, serverAction }: Route.ClientActionArgs) {
  const formData = await request.clone().formData()
  if (formData.get('_intent') === 'update-mcp-client') {
    await updateDesktopMcpClient(formData)
    return null
  }

  const result = await serverAction()

  if (result?.intent === COMPLETE_ONBOARDING_INTENT && result.status === 'error') {
    addToast('error', { description: result.message, title: "Couldn't finish setup" })
  }

  return result
}

export default function OnboardingRoute({ actionData, loaderData }: Route.ComponentProps) {
  const fetcher = useFetcher()
  const pendingClientId = fetcher.formData?.get('clientId')
  const mcpClients: DesktopMcpClientData =
    loaderData.runtime === 'desktop' && 'mcpClients' in loaderData
      ? loaderData.mcpClients
      : { clients: [] }

  return (
    <OnboardingView
      actionData={actionData ?? undefined}
      loaderData={loaderData}
      mcpClients={{
        ...mcpClients,
        loading: false,
        onWorkspaceChange: (clientId, workspace) => {
          fetcher.submit(
            { _intent: 'update-mcp-client', clientId, workspace: workspace ?? '' },
            { method: 'post' },
          )
        },
        pendingClientIds: typeof pendingClientId === 'string' ? [pendingClientId] : [],
      }}
    />
  )
}

export function HydrateFallback() {
  return (
    <main>
      <p role="status">Loading onboarding…</p>
    </main>
  )
}

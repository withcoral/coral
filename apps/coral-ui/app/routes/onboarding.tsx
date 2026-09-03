import type { Route } from './+types/onboarding'

import { useFetcher } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import { getOnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { CreateFirstWorkspace, CREATE_FIRST_WORKSPACE_FETCHER_KEY } from '@/components/workspaces'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { COMPLETE_ONBOARDING_INTENT } from '@/lib/gui-onboarding'
import { loadOnboardingSampleQuery } from '@/lib/onboarding-query.server'
import type { CreateWorkspaceActionData } from '@/lib/workspace-name'
import {
  impliedWorkspaceForRequest,
  listWorkspacesForRequest,
  pickImpliedWorkspace,
} from '@/lib/workspaces.server'
import { OnboardingView } from '@/views/onboarding/onboarding'
import { addToast } from '@/wax/components/toast'

import { runCompleteOnboardingAction } from './onboarding-action'
import { runSourcesAction } from './sources-action'
import { loadSourcesRouteData } from './sources-loader'
import {
  loadDesktopMcpClients,
  loadDesktopMcpLaunchConfig,
  updateDesktopMcpClient,
  type DesktopMcpClientData,
} from './settings-loader'

export async function loader({ context, request }: Route.LoaderArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  const workspaces = await listWorkspacesForRequest(request, accessToken)
  const workspace = pickImpliedWorkspace(workspaces)
  if (!workspace) {
    // A caller with no workspace used to dead-end on a 404 here, with the only
    // creation dialog unreachable behind the app shell it gates. Any human may
    // create a workspace — creation makes them its owner — so offer that.
    return {
      mode: 'create-workspace' as const,
      runtime: isCoralDesktopBuild() ? ('desktop' as const) : ('web' as const),
    }
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
    await impliedWorkspaceForRequest(request, accessToken),
    accessToken,
  )
}

export async function clientLoader({ serverLoader }: Route.ClientLoaderArgs) {
  const loaderData = await serverLoader()
  if (loaderData.runtime !== 'desktop') return loaderData

  const [mcpClients, mcpLaunchConfig] = await Promise.all([
    loadDesktopMcpClients(),
    loadDesktopMcpLaunchConfig(),
  ])
  return { ...loaderData, mcpClients, mcpLaunchConfig }
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
  if ('mode' in loaderData) return <CreateFirstWorkspaceStep />

  return <OnboardingExperience actionData={actionData} loaderData={loaderData} />
}

function OnboardingExperience({
  actionData,
  loaderData,
}: {
  actionData: Route.ComponentProps['actionData']
  loaderData: Exclude<Route.ComponentProps['loaderData'], { mode: 'create-workspace' }>
}) {
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
      mcpLaunchConfig={'mcpLaunchConfig' in loaderData ? loaderData.mcpLaunchConfig : undefined}
    />
  )
}

function CreateFirstWorkspaceStep() {
  const fetcher = useFetcher<CreateWorkspaceActionData>({
    key: CREATE_FIRST_WORKSPACE_FETCHER_KEY,
  })

  return <CreateFirstWorkspace error={fetcher.data?.error} pending={fetcher.state !== 'idle'} />
}

export function HydrateFallback() {
  return (
    <main>
      <p role="status">Loading onboarding…</p>
    </main>
  )
}

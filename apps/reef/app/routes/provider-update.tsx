import { useAuthorizationProgress } from '@/lib/use-authorization-progress'
import { data, redirect, useNavigation } from 'react-router'
import type { Route } from './+types/provider-update'
import {
  formContext,
  field,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { providerName, providerPath } from '@/lib/providers.server'
import { callbackRedirectUri } from '@/lib/provider-publication-attempt/setup.server'
import { attemptPath } from '@/lib/provider-publication-attempt/model'
import {
  loadAttempt,
  attemptAction,
  requestAttempt,
} from '@/lib/provider-publication-attempt/requests.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { UpdatePage } from '@/views/providers/update-page'
import {
  CredentialRoute_Kind,
  ProviderSetupInputKind,
} from '@/generated/coral/management/v1/provider_pb'
import type { AccountChoice } from '@/views/providers/types'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export function shouldRevalidate() {
  return true
}

/** Resolve the exact Provider and reauthorize every navigation, including replay. */
async function resolveTarget(
  request: Request,
  context: Route.LoaderArgs['context'],
  params: Route.LoaderArgs['params'],
) {
  const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace, {
    ownerOnly: true,
  })
  const name = providerName(w.name, params.provider)
  const provider = await api.providers.getProvider({ name })
  const path = providerPath(w.path, name)
  return {
    api,
    workspace: w,
    parent: w.name,
    provider,
    basePath: `${path}/update`,
    cancelPath: path,
    successPath: path,
  }
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace, parent, provider, basePath, cancelPath } = await resolveTarget(
      request,
      context,
      params,
    )
    const parsed = requestAttempt('update', new URL(request.url).searchParams, provider.etag)
    const { setup, attempt } = await loadAttempt(api, parent, parsed)
    const application = await api.providers.getProviderOAuthClient({ name: provider.name })
    const source = await api.providers.getProviderSource({ name: provider.name })
    return data(
      {
        workspace,
        displayName: provider.displayName,
        replacingSource: new URL(request.url).searchParams.get('replaceSource') === '1',
        source: {
          url: source.url,
          kind: source.kind === ProviderSetupInputKind.MCP ? ('mcp' as const) : ('url' as const),
        },
        currentAccount: (provider.credentialRoute?.kind === CredentialRoute_Kind.FIXED
          ? 'fixed'
          : provider.credentialRoute?.kind === CredentialRoute_Kind.CALLER
            ? 'caller'
            : 'none') as AccountChoice,
        basePath,
        cancelPath,
        providersPath: `${workspace.path}/providers`,
        setup,
        callbackUrl: callbackRedirectUri(request, context),
        attempt,
        form: formContext(context),
        applicationProblem: Boolean(application.failureCode),
        application:
          application.application?.mode.case === 'clientId'
            ? {
                clientId: application.application.mode.value,
                secretConfigured: application.secretConfigured,
                problem: Boolean(application.failureCode),
              }
            : undefined,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  let target: Awaited<ReturnType<typeof resolveTarget>>
  try {
    target = await resolveTarget(request, context, params)
  } catch (error) {
    managementFailure(error)
  }
  const { api, parent, provider, basePath, successPath } = target
  if (field(form, 'intent') === 'rotate-client-secret') {
    try {
      await api.providers.rotateProviderOAuthClientSecret({
        name: provider.name,
        clientId: field(form, 'clientId'),
        clientSecret: field(form, 'clientSecret'),
      })
      return redirect(`${basePath}?secretRotated=1`, { headers: privateHeaders })
    } catch (error) {
      managementFailure(error)
    }
  }
  return attemptAction(api, request, context, form, {
    kind: 'update',
    parent,
    provider: provider.name,
    successPath,
    basePath,
  })
}

export default function ProviderUpdateRoute({ loaderData, actionData }: Route.ComponentProps) {
  const authorization = loaderData.setup?.authorization
  useAuthorizationProgress(
    !(actionData && 'configurationErrors' in actionData) &&
      (authorization?.state === 'pending' || authorization?.state === 'exchanging'),
  )
  return (
    <UpdatePage
      key={attemptPath(loaderData.basePath, loaderData.attempt)}
      {...loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

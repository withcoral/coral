import { data, useNavigation } from 'react-router'
import { useAuthorizationProgress } from '@/lib/use-authorization-progress'
import type { Route } from './+types/provider-new'
import {
  field,
  formContext,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import { callbackRedirectUri } from '@/lib/provider-publication-attempt/setup.server'
import { adHocSource, RECIPE_PARAM, SOURCE_PARAM, URL_PARAM } from '@/lib/provider-recipes/choice'
import { RECIPE_CHOICES, recipeById, recipeChoiceById } from '@/lib/provider-recipes/library.server'
import { listAllProviders } from '@/lib/providers.server'
import {
  recipeConfiguration,
  recipeDocumentationUrl,
} from '@/lib/provider-recipes/configuration.server'
import {
  loadAttempt,
  attemptAction,
  requestAttempt,
} from '@/lib/provider-publication-attempt/requests.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { StepperPage } from '@/views/providers/stepper-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export function shouldRevalidate() {
  return true
}

async function resolveScope(
  context: Route.LoaderArgs['context'],
  workspace: string,
  options: { recovery?: boolean } = {},
) {
  const { api, workspace: w } = await authorizedWorkspace(context, workspace, {
    ownerOnly: true,
  })
  if (w.state !== 'active' && !options.recovery)
    throw new Response('Restore the workspace before you add a provider.', {
      status: 409,
      headers: privateHeaders,
    })
  return {
    api,
    workspace: w,
    parent: w.name,
    basePath: `${w.path}/providers/new`,
    cancelPath: `${w.path}/providers`,
  }
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const url = new URL(request.url)
    const recipe =
      recipeChoiceById(url.searchParams.get(RECIPE_PARAM)) ??
      adHocSource(url.searchParams.get(SOURCE_PARAM), url.searchParams.get(URL_PARAM))
    const parsed = requestAttempt('workspace', url.searchParams)
    const { api, workspace, parent, basePath, cancelPath } = await resolveScope(
      context,
      params.workspace,
      { recovery: parsed.state === 'frozen' },
    )
    const { setup, attempt } = await loadAttempt(api, parent, parsed)
    const callbackUrl = callbackRedirectUri(request, context)
    const chosen = recipeById(attempt.recipe)
    // Configuration refuses a namespace or name another provider has, before the person goes on to sign in.
    const providers = setup ? await listAllProviders(api, parent) : []
    const takenNamespaces = providers.map((provider) => provider.namespace)
    const takenDisplayNames = providers.map((provider) => provider.displayName)
    return data(
      {
        // Recipes are offered only before a source is read.
        recipes: setup ? [] : RECIPE_CHOICES,
        recipe,
        // The chosen recipe fills in a first configuration, checked against what Coral read.
        recipeConfiguration:
          setup && chosen ? recipeConfiguration(chosen, setup, { callbackUrl }) : null,
        // Where to find the credentials the account step asks for.
        recipeDocumentationUrl:
          setup && chosen ? recipeDocumentationUrl(chosen, setup.credentialMethod) : null,
        takenNamespaces,
        takenDisplayNames,
        // The shell reads a top-level Workspace to keep the sidebar in place.
        workspace,
        basePath,
        cancelPath,
        setup,
        callbackUrl,
        attempt,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const intent = field(form, 'intent')
  let resolved: Awaited<ReturnType<typeof resolveScope>>
  try {
    resolved = await resolveScope(context, params.workspace, {
      recovery: intent === 'publish',
    })
  } catch (error) {
    managementFailure(error)
  }
  const { api, workspace, parent, basePath } = resolved
  return attemptAction(api, request, context, form, {
    kind: 'workspace',
    parent,
    workspacePath: workspace.path,
    basePath,
  })
}

export default function ProviderNewRoute({ loaderData, actionData }: Route.ComponentProps) {
  const authorization = loaderData.setup?.authorization
  useAuthorizationProgress(
    !(actionData && 'configurationErrors' in actionData) &&
      (authorization?.state === 'pending' || authorization?.state === 'exchanging'),
  )
  return (
    <StepperPage
      key={loaderData.basePath}
      {...loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

import { data, Outlet, redirect, type ShouldRevalidateFunctionArgs } from 'react-router'
import type { Route } from './+types/providers'
import {
  field,
  formContext,
  managementFailure,
  mutationForm,
  privateHeaders,
} from '@/lib/management.server'
import type { ManagementApi } from '@/lib/management.server'
import {
  builtinCatalogUpdate,
  providerName,
  providerView,
  listAllProviders,
  providerFailure,
  uuidOr404,
} from '@/lib/providers.server'
import { personalAccountStates } from '@/lib/accounts.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { RECIPE_CHOICES } from '@/lib/provider-recipes/library.server'
import { OperationAccessState } from '@/generated/coral/management/v1/policy_pb'
import {
  decodeSelectedOperations,
  selectionKey,
  type SelectedOperation,
} from '@/views/providers/selection'
import { countLabel } from '@/views/providers/configuration-details'
import type { OperationOutcome } from '@/views/providers/types'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

// Selection changes on the page (provider, operation) reuse the
// tree: they are one table, and the address only names what is revealed. A
// child loader still checks Workspace authority on each. Refreshes, mutations,
// query changes, and leaving the page reload it.
export function shouldRevalidate({
  currentUrl,
  nextUrl,
  formMethod,
  actionStatus,
}: ShouldRevalidateFunctionArgs) {
  if ((formMethod && formMethod.toUpperCase() !== 'GET') || actionStatus !== undefined) return true
  if (currentUrl.search !== nextUrl.search || currentUrl.pathname === nextUrl.pathname) return true
  // `new` is the Add provider page, whose result must reload the catalog.
  const pagePath = /^(\/workspaces\/[^/]+\/providers)(?:\/(?!new(?:\/|$))[^/]+(?:\/ops\/[^/]+)?)?$/
  const current = pagePath.exec(currentUrl.pathname)
  const next = pagePath.exec(nextUrl.pathname)
  return !current || !next || current[1] !== next[1]
}

/** Loads the Workspace tree independently of operation selection. */
export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace)
    const [listed, personalAccounts] = await Promise.all([
      listAllProviders(api, w.name),
      personalAccountStates(api, w.name),
    ])
    const providers = await Promise.all(
      listed.map((provider) => providerView(api, provider, w.path, personalAccounts)),
    )
    // Only an owner can update Coral, so only an owner reads its update.
    const builtin = providers.find((provider) => provider.builtin)
    const builtinUpdate =
      builtin && w.effectiveRole === 'owner'
        ? await builtinCatalogUpdate(api, providerName(w.name, builtin.id))
        : null
    const selectedProvider = params.provider ? uuidOr404(params.provider) : null
    if (selectedProvider && !providers.some((i) => i.id === selectedProvider))
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    const selectedOperation = params.operation ?? null
    if (
      selectedOperation &&
      !providers
        .find((i) => i.id === selectedProvider)
        ?.operations.some((o) => o.relativePath === selectedOperation)
    )
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    return data(
      {
        workspace: w,
        providers,
        builtinUpdate,
        recipes: RECIPE_CHOICES,
        selectedProvider,
        selectedOperation,
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
  try {
    const { api, workspace: w } = await authorizedWorkspace(request, context, params.workspace, {
      ownerOnly: true,
    })
    const providersPath = `${w.path}/providers`
    switch (intent) {
      case 'renameProvider': {
        await api.providers.updateProvider({
          name: providerName(w.name, field(form, 'provider')),
          etag: field(form, 'etag'),
          displayName: field(form, 'displayName'),
        })
        return data(
          { error: null, credential: null, message: 'Provider name saved.' },
          { headers: privateHeaders },
        )
      }
      case 'disableProvider':
      case 'enableProvider': {
        const name = `${w.name}/providers/${uuidOr404(field(form, 'provider'))}`
        const etag = field(form, 'etag')
        if (intent === 'disableProvider') await api.providers.disableProvider({ name, etag })
        else await api.providers.restoreProvider({ name, etag })
        return data(
          {
            error: null,
            credential: null,
            message:
              intent === 'disableProvider'
                ? 'Provider disabled. Coral blocks unsent operation calls. Calls already sent can finish.'
                : 'Provider enabled. Other access restrictions still apply.',
          },
          { headers: privateHeaders },
        )
      }
      case 'updateBuiltinCatalog': {
        await api.providers.updateBuiltinCatalog({
          name: providerName(w.name, field(form, 'provider')),
          etag: field(form, 'etag'),
        })
        return data(
          { error: null, credential: null, message: 'Coral is updated.' },
          { headers: privateHeaders },
        )
      }
      case 'removeProvider': {
        await api.providers.deleteProvider({
          name: `${w.name}/providers/${uuidOr404(field(form, 'provider'))}`,
          etag: field(form, 'etag'),
        })
        return redirect(providersPath, { headers: privateHeaders })
      }
      case 'policy': {
        const access = field(form, 'access')
        const states: Record<string, OperationAccessState> = {
          enabled: OperationAccessState.ENABLED,
          approval_required: OperationAccessState.APPROVAL_REQUIRED,
          disabled: OperationAccessState.DISABLED,
        }
        const selected = decodeSelectedOperations(form)
        if (
          !Object.hasOwn(states, access) ||
          !selected ||
          selected.length === 0 ||
          selected.some((target) => target.operation.length > 4096) ||
          new Set(selected.map((target) => selectionKey(target.provider, target.operation)))
            .size !== selected.length
        )
          throw new Response('Select distinct operations and a policy.', {
            status: 400,
            headers: privateHeaders,
          })
        const byProvider = new Map<string, string[]>()
        for (const target of selected) {
          const provider = uuidOr404(target.provider)
          byProvider.set(provider, [...(byProvider.get(provider) ?? []), target.operation])
        }
        const outcomes = await applyPolicyBatches(
          [...byProvider].flatMap(([provider, operations]) =>
            chunk(operations, POLICY_BATCH_SIZE).map((batch) => ({ provider, operations: batch })),
          ),
          { api, workspace: w.name, accessState: states[access]! },
        )
        const failed = outcomes.filter((outcome) => outcome.status === 'failed')
        const message =
          failed.length === 0
            ? `Operation policy saved for ${countLabel(selected.length, 'operation')}.`
            : `Operation policy saved for ${countLabel(selected.length - failed.length, 'operation')}. ${countLabel(failed.length, 'operation')} not saved: ${failed[0]!.reason}`
        return data(
          {
            error: null,
            message,
            credential: null,
            operations: outcomes,
          },
          { headers: privateHeaders },
        )
      }
      default:
        throw new Response('Invalid action', { status: 400, headers: privateHeaders })
    }
  } catch (error) {
    const failures: Record<string, string> = {
      renameProvider: 'Coral could not change the provider name. ',
      disableProvider: 'Coral could not disable the provider. ',
      enableProvider: 'Coral could not enable the provider. ',
      removeProvider: 'Coral could not remove the provider. ',
      updateBuiltinCatalog: 'Coral could not update the Coral provider. ',
      policy: 'Coral could not save operation policy. ',
    }
    return providerFailure(error, failures[intent])
  }
}

// Coral accepts this many policies in one update call.
const POLICY_BATCH_SIZE = 100
// This many provider batches run at once. Coral serialises writes per provider,
// so wider concurrency only queues on its side.
const POLICY_BATCH_CONCURRENCY = 4

function chunk<T>(items: T[], size: number) {
  const chunks: T[][] = []
  for (let index = 0; index < items.length; index += size)
    chunks.push(items.slice(index, index + size))
  return chunks
}

type PolicyBatch = { provider: string; operations: string[] }

/**
 * Runs every batch with bounded concurrency and reports each operation on its
 * own, so a failure in one batch neither hides the batches that saved nor
 * masquerades as a failure of them.
 */
async function applyPolicyBatches(
  batches: PolicyBatch[],
  {
    api,
    workspace,
    accessState,
  }: { api: ManagementApi; workspace: string; accessState: OperationAccessState },
): Promise<OperationOutcome[]> {
  const outcomes: OperationOutcome[] = []
  let next = 0
  const worker = async () => {
    while (next < batches.length) {
      const batch = batches[next]!
      next += 1
      const targets: SelectedOperation[] = batch.operations.map((operation) => ({
        provider: batch.provider,
        operation,
      }))
      try {
        await api.policies.updateOperationPolicies({
          parent: providerName(workspace, batch.provider),
          policies: batch.operations.map((operation) => ({ operation, accessState })),
        })
        for (const target of targets) outcomes.push({ ...target, status: 'updated' })
      } catch (error) {
        const reason = failureReason(error)
        for (const target of targets) outcomes.push({ ...target, status: 'failed', reason })
      }
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(POLICY_BATCH_CONCURRENCY, batches.length) }, worker),
  )
  return outcomes
}

function failureReason(error: unknown) {
  const failure = providerFailure(error, 'Coral could not save operation policy. ')
  return failure.data.error ?? 'Coral could not save operation policy.'
}

export type ProvidersData = Route.ComponentProps['loaderData']

export default function ProvidersRoute({ loaderData }: Route.ComponentProps) {
  return <Outlet context={loaderData} />
}

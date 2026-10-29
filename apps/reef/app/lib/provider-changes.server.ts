import { data, redirect, type RouterContextProvider } from 'react-router'
import { field, privateHeaders } from './management.server'
import type { ManagementApi } from './management.server'
import { providerName, providerFailure, uuidOr404 } from './providers.server'
import { authorizedWorkspace } from './workspace-route.server'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { decodeSelectedTools, selectionKey, type SelectedTool } from '@/views/providers/selection'
import { countLabel } from '@/utils/count-label'
import type { ToolOutcome } from '@/views/providers/types'

/**
 * One owner change to a workspace's providers, from a form the route already
 * parsed and checked: rename, enable or disable, remove, or tool policy.
 */
export async function applyProviderChange(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string },
  form: FormData,
) {
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
                ? 'Provider disabled. Coral blocks unsent tool calls. Calls already sent can finish.'
                : 'Provider enabled. Other access restrictions still apply.',
          },
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
        const states: Record<string, ToolAccessState> = {
          enabled: ToolAccessState.ENABLED,
          approval_required: ToolAccessState.APPROVAL_REQUIRED,
          disabled: ToolAccessState.DISABLED,
        }
        const selected = decodeSelectedTools(form)
        if (
          !Object.hasOwn(states, access) ||
          !selected ||
          selected.length === 0 ||
          selected.some((target) => target.tool.length > 4096) ||
          new Set(selected.map((target) => selectionKey(target.provider, target.tool))).size !==
            selected.length
        )
          throw new Response('Select distinct tools and a policy.', {
            status: 400,
            headers: privateHeaders,
          })
        const byProvider = new Map<string, string[]>()
        for (const target of selected) {
          const provider = uuidOr404(target.provider)
          byProvider.set(provider, [...(byProvider.get(provider) ?? []), target.tool])
        }
        const outcomes = await applyPolicyBatches(
          [...byProvider].flatMap(([provider, tools]) =>
            chunk(tools, POLICY_BATCH_SIZE).map((batch) => ({ provider, tools: batch })),
          ),
          { api, workspace: w.name, accessState: states[access]! },
        )
        const failed = outcomes.filter((outcome) => outcome.status === 'failed')
        const message =
          failed.length === 0
            ? `Tool policy saved for ${countLabel(selected.length, 'tool')}.`
            : `Tool policy saved for ${countLabel(selected.length - failed.length, 'tool')}. ${countLabel(failed.length, 'tool')} not saved: ${failed[0]!.reason}`
        return data(
          {
            error: null,
            message,
            credential: null,
            tools: outcomes,
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
      policy: 'Coral could not save tool policy. ',
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

type PolicyBatch = { provider: string; tools: string[] }

/**
 * Runs every batch with bounded concurrency and reports each tool on its
 * own, so a failure in one batch neither hides the batches that saved nor
 * masquerades as a failure of them.
 */
async function applyPolicyBatches(
  batches: PolicyBatch[],
  {
    api,
    workspace,
    accessState,
  }: { api: ManagementApi; workspace: string; accessState: ToolAccessState },
): Promise<ToolOutcome[]> {
  const outcomes: ToolOutcome[] = []
  let next = 0
  const worker = async () => {
    while (next < batches.length) {
      const batch = batches[next]!
      next += 1
      const targets: SelectedTool[] = batch.tools.map((tool) => ({
        provider: batch.provider,
        tool,
      }))
      try {
        await api.policies.updateToolPolicies({
          parent: providerName(workspace, batch.provider),
          policies: batch.tools.map((tool) => ({ tool, accessState })),
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
  const failure = providerFailure(error, 'Coral could not save tool policy. ')
  return failure.data.error ?? 'Coral could not save tool policy.'
}

import { Code, ConnectError } from '@connectrpc/connect'
import { data, redirect, type RouterContextProvider } from 'react-router'
import { field, privateHeaders } from './management.server'
import type { ManagementApi } from './management.server'
import {
  blockerLabel,
  providerName,
  providerFailure,
  toolName,
  uuidOr404,
} from './providers.server'
import { authorizedWorkspace } from './workspace-route.server'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { decodeSelectedTools, selectionKey, type SelectedTool } from '@/views/providers/selection'
import { countLabel } from '@/utils/count-label'
import type { ToolOutcome } from '@/views/providers/types'
import { PROVIDER_ICON_MAX_BYTES, PROVIDER_ICON_RULE } from './provider-input-limits'

/**
 * One owner change to a workspace's providers, from a form the route already
 * parsed and checked: rename, enable or disable, remove, or tool policy.
 */
export async function applyProviderChange(
  context: Readonly<RouterContextProvider>,
  params: { workspace: string },
  form: FormData,
) {
  const intent = field(form, 'intent')
  try {
    const { api, workspace: w } = await authorizedWorkspace(context, params.workspace, {
      ownerOnly: true,
    })
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
      case 'setProviderIcon': {
        const icon = form.get('icon')
        if (!(icon instanceof File) || icon.size === 0)
          return data(
            { error: `Choose an icon file. ${PROVIDER_ICON_RULE}`, credential: null },
            { status: 400, headers: privateHeaders },
          )
        if (icon.size > PROVIDER_ICON_MAX_BYTES)
          return data(
            { error: `This file is too large. ${PROVIDER_ICON_RULE}`, credential: null },
            { status: 400, headers: privateHeaders },
          )
        // Coral reads the image's type from its bytes, not from the browser.
        await api.providers.setProviderIcon({
          name: providerName(w.name, field(form, 'provider')),
          etag: field(form, 'etag'),
          content: new Uint8Array(await icon.arrayBuffer()),
        })
        return data(
          { error: null, credential: null, message: 'Icon saved.' },
          { headers: privateHeaders },
        )
      }
      case 'deleteProviderIcon': {
        await api.providers.deleteProviderIcon({
          name: providerName(w.name, field(form, 'provider')),
          etag: field(form, 'etag'),
        })
        return data(
          { error: null, credential: null, message: 'Icon removed.' },
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
        return redirect(`${w.path}/tools`, { headers: privateHeaders })
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
        const blockers = await currentBlockers(api, w.name, [...byProvider.keys()])
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
            blockers,
          },
          { headers: privateHeaders },
        )
      }
      default:
        throw new Response('Invalid action', { status: 400, headers: privateHeaders })
    }
  } catch (error) {
    // Coral refuses an image it cannot read as one of the accepted types.
    if (
      intent === 'setProviderIcon' &&
      error instanceof ConnectError &&
      error.code === Code.InvalidArgument &&
      error.rawMessage === 'INVALID_PROVIDER_INPUT'
    )
      return data(
        { error: `Coral cannot use this file as an icon. ${PROVIDER_ICON_RULE}`, credential: null },
        { status: 400, headers: privateHeaders },
      )
    const failures: Record<string, string> = {
      renameProvider: 'Coral could not change the provider name. ',
      setProviderIcon: 'Coral could not save the icon. ',
      deleteProviderIcon: 'Coral could not remove the icon. ',
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
        const parent = providerName(workspace, batch.provider)
        await api.policies.updateToolPolicies({
          parent,
          policies: batch.tools.map((key) => ({ tool: toolName(parent, key), accessState })),
        })
        for (const target of targets) outcomes.push({ ...target, status: 'updated' })
      } catch (error) {
        // A lapsed sign-in fails the whole change, so the protected layout
        // signs the reader in again. No later batch starts.
        if (error instanceof ConnectError && error.code === Code.Unauthenticated) {
          next = batches.length
          throw error
        }
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

/**
 * Each provider's blockers, which a policy change can move: a provider whose
 * tools are all disabled has none ready. Undefined when a read fails, which
 * leaves the page to reload them, as the policy change itself succeeded.
 */
async function currentBlockers(api: ManagementApi, workspace: string, providers: string[]) {
  try {
    const entries: (readonly [string, string[]])[] = []
    for (const batch of chunk(providers, POLICY_BATCH_CONCURRENCY)) {
      entries.push(
        ...(await Promise.all(
          batch.map(async (id) => {
            const provider = await api.providers.getProvider({ name: providerName(workspace, id) })
            return [id, provider.blockers.map(blockerLabel)] as const
          }),
        )),
      )
    }
    return Object.fromEntries(entries)
  } catch {
    return undefined
  }
}

function failureReason(error: unknown) {
  const failure = providerFailure(error, 'Coral could not save tool policy. ')
  return failure.data.error ?? 'Coral could not save tool policy.'
}

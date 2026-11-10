import { data, useNavigation, useOutletContext } from 'react-router'
import type { Route } from './+types/tools-selection'
import type { ToolsData } from './tools'
import { agentName, managementFailure, privateHeaders } from '@/lib/management.server'
import { privateToolDetail } from '@/lib/private-tools.server'
import { sharedToolDetail } from '@/lib/shared-tools.server'
import { providerName, toolDetail, uuidOr404 } from '@/lib/providers.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { addressedTool, PRIVATE_GROUP, SHARED_GROUP } from '@/views/tools/addresses'
import { ToolsPage } from '@/views/tools/tools-page'

export { action } from './tools'

export function shouldRevalidate() {
  return true
}

/**
 * Selection reveals no new catalog data but the selected tool's detail, which
 * the tree leaves out, and always rechecks current access. A provider address
 * names a provider's tool. A group address names a shared tool, one of your
 * own private tools, or an agent's private tool when it also names the agent.
 */
export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    const { group } = params
    if (
      (group !== undefined && group !== PRIVATE_GROUP && group !== SHARED_GROUP) ||
      (params.agent !== undefined && group !== PRIVATE_GROUP)
    )
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    const selectedProvider = params.provider ? uuidOr404(params.provider) : null
    const selectedTool = params.tool ?? null
    const selectedToolAgent = params.agent ?? null
    return data(
      {
        workspace,
        group: (group ?? null) as typeof PRIVATE_GROUP | typeof SHARED_GROUP | null,
        selectedProvider,
        selectedTool,
        selectedToolAgent,
        selectedToolDetail: !selectedTool
          ? null
          : group === SHARED_GROUP
            ? await sharedToolDetail(api, workspace.name, selectedTool)
            : group === PRIVATE_GROUP
              ? await privateToolDetail(
                  api,
                  workspace.name,
                  selectedTool,
                  selectedToolAgent ? agentName(params.workspace, selectedToolAgent) : '',
                )
              : selectedProvider
                ? await toolDetail(
                    api,
                    providerName(workspace.name, selectedProvider),
                    selectedTool,
                  )
                : null,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export default function ToolsSelectionRoute({ loaderData, actionData }: Route.ComponentProps) {
  const tree = useOutletContext<ToolsData>()
  const pending = useNavigation().state !== 'idle'
  const { group, selectedTool, selectedToolAgent } = loaderData
  // The shared and the private group's address does not carry its provider's identifier.
  const provider = group
    ? tree.providers.find((i) => i.kind === group)
    : tree.providers.find((i) => i.id === loaderData.selectedProvider)
  if (
    ((group || loaderData.selectedProvider) && !provider) ||
    (selectedTool && !(provider && addressedTool(provider, selectedTool, selectedToolAgent)))
  )
    throw new Response('Not found', { status: 404 })
  return (
    <ToolsPage
      key={loaderData.workspace.name}
      {...tree}
      {...loaderData}
      selectedProvider={provider?.id ?? null}
      result={actionData}
      pending={pending}
    />
  )
}

import { data, useNavigation, useOutletContext } from 'react-router'
import type { Route } from './+types/tools-selection'
import type { ToolsData } from './tools'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { privateToolDetail } from '@/lib/private-tools.server'
import { providerName, toolDetail, uuidOr404 } from '@/lib/providers.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { addressedTool, PRIVATE_GROUP } from '@/views/tools/addresses'
import { ToolsPage } from '@/views/tools/tools-page'

export { action } from './tools'

export function shouldRevalidate() {
  return true
}

/**
 * Selection reveals no new catalog data but the selected tool's detail, which
 * the tree leaves out, and always rechecks current access. A provider address
 * names a provider's tool; a group address names one of your own private tools.
 */
export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    if (params.group !== undefined && params.group !== PRIVATE_GROUP)
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    const privateGroup = params.group !== undefined
    const selectedProvider = params.provider ? uuidOr404(params.provider) : null
    const selectedTool = params.tool ?? null
    return data(
      {
        workspace,
        privateGroup,
        selectedProvider,
        selectedTool,
        selectedToolDetail: !selectedTool
          ? null
          : privateGroup
            ? await privateToolDetail(api, workspace.name, selectedTool)
            : selectedProvider
              ? await toolDetail(api, providerName(workspace.name, selectedProvider), selectedTool)
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
  const { privateGroup, selectedTool } = loaderData
  // The private group's address does not carry its provider's identifier.
  const provider = privateGroup
    ? tree.providers.find((i) => i.kind === 'private')
    : tree.providers.find((i) => i.id === loaderData.selectedProvider)
  if (
    ((privateGroup || loaderData.selectedProvider) && !provider) ||
    (selectedTool && !(provider && addressedTool(provider, selectedTool)))
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

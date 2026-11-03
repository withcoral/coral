import { data, useNavigation, useOutletContext } from 'react-router'
import type { Route } from './+types/providers-selection'
import type { ProvidersData } from './providers'
import { managementFailure, privateHeaders } from '@/lib/management.server'
import { uuidOr404 } from '@/lib/providers.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { WorkspaceProvidersPage } from '@/views/providers/workspace-providers-page'

export { action } from './providers'

export function shouldRevalidate() {
  return true
}

/** Selection reveals no new catalog data, but always rechecks current access. */
export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { workspace } = await authorizedWorkspace(context, params.workspace)
    return data(
      {
        workspace,
        selectedProvider: params.provider ? uuidOr404(params.provider) : null,
        selectedTool: params.tool ?? null,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export default function ProvidersSelectionRoute({ loaderData, actionData }: Route.ComponentProps) {
  const tree = useOutletContext<ProvidersData>()
  const pending = useNavigation().state !== 'idle'
  const { selectedProvider, selectedTool } = loaderData
  const provider = tree.providers.find((i) => i.id === selectedProvider)
  if (
    (selectedProvider && !provider) ||
    (selectedTool && !provider?.tools.some((o) => o.relativePath === selectedTool))
  )
    throw new Response('Not found', { status: 404 })
  return (
    <WorkspaceProvidersPage
      key={loaderData.workspace.name}
      {...tree}
      {...loaderData}
      result={actionData}
      pending={pending}
    />
  )
}

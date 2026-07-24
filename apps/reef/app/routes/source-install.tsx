import type { Route } from './+types/source-install'
import { redirect, useNavigate } from 'react-router'

import type { SourcesActionData } from './sources-action'
import { runSourcesAction } from './sources-action'

import { addToast } from '@/wax/components/toast'
import { SourceCreateDialog } from '@/views/sources/source-create'
import { routePath } from '@/routing/routemap'
import { workspaceFromParams } from '@/lib/workspace-routing'

export async function action({ params, request }: Route.ActionArgs): Promise<SourcesActionData> {
  return runSourcesAction(request, workspaceFromParams(params))
}

export async function clientAction({
  params,
  serverAction,
}: Route.ClientActionArgs): Promise<SourcesActionData | Response> {
  const result = await serverAction()
  if (result?.status !== 'success') return result

  addToast('success', {
    title: `Created ${result.name}`,
    description: 'The source was validated and installed.',
  })
  return redirect(routePath('workspaceSources', { workspaceId: params.workspaceId }))
}

export default function SourceInstallRoute({ actionData, params }: Route.ComponentProps) {
  const navigate = useNavigate()
  const sourcesPath = routePath('workspaceSources', { workspaceId: params.workspaceId })

  return (
    <SourceCreateDialog
      actionData={actionData}
      discoveryPath={routePath('workspaceSourceDiscovery', { workspaceId: params.workspaceId })}
      open
      onOpenChange={(open) => {
        if (!open) navigate(sourcesPath)
      }}
    />
  )
}

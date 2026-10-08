import type { Route } from './+types/source-install'
import { redirect, useNavigate, useRevalidator } from 'react-router'

import type { SourcesActionData } from './sources-action'
import { runSourcesAction } from './sources-action'

import { requestAuthContext } from '@/auth/server-context'
import { addToast } from '@/wax/components/toast'
import { IMPORT_ERROR_TOAST_MS, SourceAddDialog } from '@/views/sources/source-add'
import { routePath } from '@/routing/routemap'
import { workspaceFromParams } from '@/lib/workspace-routing'

export async function action({
  context,
  params,
  request,
}: Route.ActionArgs): Promise<SourcesActionData> {
  return runSourcesAction(
    request,
    workspaceFromParams(params),
    context.get(requestAuthContext).accessToken,
  )
}

export async function clientAction({
  params,
  serverAction,
}: Route.ClientActionArgs): Promise<SourcesActionData | Response> {
  const result = await serverAction()
  if (result?.status === 'error') {
    // Manifest errors carry parse positions, so they need the longer toast
    // whether the manifest was written here or handed in whole.
    addToast('error', {
      description: result.message,
      durationMs: IMPORT_ERROR_TOAST_MS,
      title: result.name ? `Could not add ${result.name}` : 'Could not add that source',
    })
    return result
  }
  if (result?.status !== 'success') return result

  addToast('success', {
    title: `Added ${result.name}`,
    description: 'The source was validated and installed.',
  })
  return redirect(routePath('workspaceSources', { workspaceId: params.workspaceId }))
}

export default function SourceInstallRoute({ params }: Route.ComponentProps) {
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const sourcesPath = routePath('workspaceSources', { workspaceId: params.workspaceId })

  return (
    <SourceAddDialog
      describePath={routePath('workspaceSourceDescribe', { workspaceId: params.workspaceId })}
      discoveryPath={routePath('workspaceSourceDiscovery', { workspaceId: params.workspaceId })}
      oauthImportPath={`${sourcesPath}/oauth-import`}
      onOAuthImportComplete={async (name, signal) => {
        addToast('success', {
          title: `Added ${name}`,
          description: 'The source was validated and installed.',
        })
        await revalidator.revalidate()
        if (signal.aborted) return
        await navigate(sourcesPath)
      }}
      open
      onOpenChange={(open) => {
        if (!open) navigate(sourcesPath)
      }}
    />
  )
}

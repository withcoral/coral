import type { Route } from './+types/source-import'
import { redirect, useNavigate, useRevalidator } from 'react-router'

import type { SourcesActionData } from './sources-action'
import { runSourcesAction } from './sources-action'

import { requestAuthContext } from '@/auth/server-context'
import { addToast } from '@/wax/components/toast'
import { IMPORT_ERROR_TOAST_MS, SourceImportDialog } from '@/views/sources/source-import'
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
    addToast('error', {
      description: result.message,
      durationMs: IMPORT_ERROR_TOAST_MS,
      title: result.name ? `Could not import ${result.name}` : 'Could not import that source',
    })
    return result
  }
  if (result?.status !== 'success') return result

  addToast('success', {
    title: `Imported ${result.name}`,
    description: 'The source was validated and installed.',
  })
  return redirect(routePath('workspaceSources', { workspaceId: params.workspaceId }))
}

export default function SourceImportRoute({ params }: Route.ComponentProps) {
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const sourcesPath = routePath('workspaceSources', { workspaceId: params.workspaceId })

  return (
    <SourceImportDialog
      describePath={routePath('workspaceSourceDescribe', { workspaceId: params.workspaceId })}
      oauthImportPath={`${sourcesPath}/oauth-import`}
      onOAuthImportComplete={async (name, signal) => {
        addToast('success', {
          title: `Imported ${name}`,
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

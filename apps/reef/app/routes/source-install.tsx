import type { Route } from './+types/source-install'
import { redirect, useLocation, useNavigate, useRevalidator } from 'react-router'

import type { SourcesActionData } from './sources-action'
import { runSourcesAction } from './sources-action'

import { requestAuthContext } from '@/auth/server-context'
import { addToast } from '@/wax/components/toast'
import { SourceCreateDialog } from '@/views/sources/source-create'
import { routePath } from '@/routing/routemap'
import { sourceCreatePrefill } from '@/lib/source-presets'
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
  if (result?.status !== 'success') return result

  addToast('success', {
    title: `Created ${result.name}`,
    description: 'The source was validated and installed.',
  })
  return redirect(routePath('workspaceSources', { workspaceId: params.workspaceId }))
}

export default function SourceInstallRoute({ actionData, params }: Route.ComponentProps) {
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const sourcesPath = routePath('workspaceSources', { workspaceId: params.workspaceId })
  // Parsed here rather than in a loader: the prefill comes from the query string
  // alone, and a server loader would make every navigation to this route wait on a
  // `.data` request just to read it. useLocation gives the same value under SSR.
  const prefill = sourceCreatePrefill(new URLSearchParams(useLocation().search))

  return (
    <SourceCreateDialog
      actionData={actionData}
      discoveryPath={routePath('workspaceSourceDiscovery', { workspaceId: params.workspaceId })}
      oauthImportPath={`${sourcesPath}/oauth-import`}
      onOAuthImportComplete={async (name, signal) => {
        addToast('success', {
          title: `Created ${name}`,
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
      prefill={prefill}
    />
  )
}

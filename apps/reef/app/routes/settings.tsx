import { data, useLocation, useNavigate, useNavigation } from 'react-router'
import type { Route } from './+types/settings'
import {
  actionFailure,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  workspaceName,
} from '@/lib/management.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { SettingsDialog } from '@/views/management/settings-dialog'

// No ErrorBoundary: a settings failure reaches the Workspace route's boundary,
// so the error page replaces the Overview instead of rendering under it.

export function shouldRevalidate() {
  return true
}

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const { workspace } = await authorizedWorkspace(request, context, params.workspace, {
      ownerOnly: true,
    })
    return data({ workspace, form: formContext(context) }, { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}

export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const api = management(request, context)
  const name = workspaceName(params.workspace)
  const intent = field(form, 'intent')
  try {
    switch (intent) {
      case 'rename':
        await api.workspaces.updateWorkspace({
          workspace: { name, etag: field(form, 'etag'), displayName: field(form, 'displayName') },
          updateMask: { paths: ['display_name'] },
        })
        break
      case 'archive':
        await api.workspaces.archiveWorkspace({ name, etag: field(form, 'etag') })
        break
      case 'restore':
        await api.workspaces.restoreWorkspace({ name, etag: field(form, 'etag') })
        break
      default:
        throw new Response('Invalid action', { status: 400 })
    }
    const messages: Record<string, string> = {
      rename: 'Workspace name saved.',
      archive: 'Workspace archived.',
      restore: 'Workspace restored. Other access restrictions still apply.',
    }
    return data(
      { error: null, credential: null, message: messages[intent] },
      { headers: privateHeaders },
    )
  } catch (error) {
    const failures: Record<string, string> = {
      rename: 'Coral could not save the workspace name. ',
      archive: 'Coral could not archive the workspace. ',
      restore: 'Coral could not restore the workspace. ',
    }
    return actionFailure(error, failures[intent])
  }
}

export default function SettingsRoute({ loaderData, actionData }: Route.ComponentProps) {
  const navigate = useNavigate()
  return (
    <SettingsDialog
      key={useLocation().key}
      {...loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
      onClose={() => navigate(loaderData.workspace.path, { replace: true })}
    />
  )
}

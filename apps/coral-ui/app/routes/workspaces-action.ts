import { data, redirect } from 'react-router'

import type { Route } from './+types/workspaces-action'

import { requestAuthContext } from '@/auth/server-context'
import { errorMessage } from '@/lib/utils'
import { type CreateWorkspaceActionData, validateWorkspaceName } from '@/lib/workspace-name'
import { createWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

export async function action({ context, request }: Route.ActionArgs) {
  const formData = await request.formData()
  const intent = formData.get('intent')

  switch (intent) {
    case 'create': {
      const nameValue = formData.get('name')
      const name = typeof nameValue === 'string' ? nameValue : ''
      const validationError = validateWorkspaceName(name)

      if (validationError) return actionError(name, validationError, 400)

      try {
        const workspace = await createWorkspaceForRequest(
          request,
          context.get(requestAuthContext).accessToken,
          name,
        )
        return redirect(routePath('workspaceSources', { workspaceId: workspace.name }))
      } catch (error) {
        return actionError(name, errorMessage(error), 502)
      }
    }
    default:
      return actionError('', 'Unsupported workspace action.', 400)
  }
}

function actionError(name: string, error: string, status: number) {
  return data<CreateWorkspaceActionData>({ error, name }, { status })
}

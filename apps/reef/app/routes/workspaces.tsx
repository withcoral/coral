import { redirect } from 'react-router'
import type { Route } from './+types/workspaces'
import {
  actionFailure,
  field,
  management,
  mutationForm,
  privateHeaders,
  resourcePath,
} from '@/lib/management.server'

// The sidebar lists Workspaces, so this path only accepts creation. Old links
// to the former directory land on home.
export function loader() {
  return redirect('/', { headers: privateHeaders })
}
export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    if (field(form, 'intent') !== 'createWorkspace')
      throw new Response('Invalid action', { status: 400 })
    const workspace = await management(context).workspaces.createWorkspace({
      displayName: field(form, 'displayName'),
      requestId: field(form, 'requestId'),
    })
    return redirect(resourcePath(workspace.name), { headers: privateHeaders })
  } catch (error) {
    return actionFailure(error, 'Coral could not create the workspace. ')
  }
}

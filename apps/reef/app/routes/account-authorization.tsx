import { useAuthorizationProgress } from '@/lib/use-authorization-progress'
import { data, useNavigation } from 'react-router'
import type { Route } from './+types/account-authorization'
import { mutationForm } from '@/lib/management.server'
import {
  accountFailure,
  accountHeaders,
  authorizationAction,
  loadAuthorization,
} from '@/lib/accounts.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { AuthorizationPage } from '@/views/accounts/authorization-page'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    const view = await loadAuthorization(request, context, params)
    const workspace = params.workspace
      ? (await authorizedWorkspace(context, params.workspace)).workspace
      : undefined
    return data({ ...view, workspace }, { headers: accountHeaders })
  } catch (error) {
    accountFailure(error)
  }
}
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    return await authorizationAction(request, context, params, form)
  } catch (error) {
    accountFailure(error)
  }
}
export default function AccountAuthorizationRoute({ loaderData }: Route.ComponentProps) {
  useAuthorizationProgress(loaderData.state === 'pending' || loaderData.state === 'exchanging')
  return <AuthorizationPage view={loaderData} pending={useNavigation().state !== 'idle'} />
}

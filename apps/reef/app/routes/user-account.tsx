import { data, useNavigation } from 'react-router'
import type { Route } from './+types/user-account'
import { field, mutationForm } from '@/lib/management.server'
import {
  accountFailure,
  accountHeaders,
  connectYourAccountAction,
  disconnectUserAccountAction,
  loadUserAccount,
  userAccountAction,
} from '@/lib/accounts.server'
import { UserAccountPage } from '@/views/accounts/user-account-page'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    return data(await loadUserAccount(request, context, params), { headers: accountHeaders })
  } catch (error) {
    accountFailure(error)
  }
}
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const intent = field(form, 'intent')
  // A connect dialog posts here from any page, and its `returnTo` names that page.
  if (intent === 'connectAccount') return connectYourAccountAction(request, context, params, form)
  try {
    return intent === 'disconnect'
      ? await disconnectUserAccountAction(request, context, params)
      : await userAccountAction(request, context, params, form)
  } catch (error) {
    accountFailure(error)
  }
}
export default function UserAccountRoute({ loaderData }: Route.ComponentProps) {
  return (
    <UserAccountPage
      key={loaderData.path}
      view={loaderData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

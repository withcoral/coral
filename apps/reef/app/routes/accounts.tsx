import { data, useNavigation } from 'react-router'
import type { Route } from './+types/accounts'
import { mutationForm } from '@/lib/management.server'
import {
  accountFailure,
  accountHeaders,
  loadUserAccounts,
  userAccountsAction,
} from '@/lib/accounts.server'
import { UserAccountsPage } from '@/views/accounts/user-accounts-page'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    return data(await loadUserAccounts(context, params), { headers: accountHeaders })
  } catch (error) {
    accountFailure(error)
  }
}
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    return await userAccountsAction(context, params, form)
  } catch (error) {
    accountFailure(error)
  }
}
export default function AccountsRoute({ loaderData }: Route.ComponentProps) {
  return (
    <UserAccountsPage
      key={loaderData.path}
      view={loaderData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

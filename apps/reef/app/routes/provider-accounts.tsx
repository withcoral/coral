import { data, useNavigation } from 'react-router'
import type { Route } from './+types/provider-accounts'
import { mutationForm } from '@/lib/management.server'
import { accountFailure, accountHeaders, accountsAction, loadAccounts } from '@/lib/accounts.server'
import { AccountsPage } from '@/views/accounts/accounts-page'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    return data(await loadAccounts(request, context, params), { headers: accountHeaders })
  } catch (error) {
    accountFailure(error)
  }
}
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    return await accountsAction(request, context, params, form)
  } catch (error) {
    accountFailure(error)
  }
}
export default function ProviderAccountsRoute({ loaderData, actionData }: Route.ComponentProps) {
  return (
    <AccountsPage
      key={loaderData.path}
      view={loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

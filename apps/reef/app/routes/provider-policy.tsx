import { data, useLocation, useNavigation } from 'react-router'
import type { Route } from './+types/provider-policy'
import {
  mutationForm,
  managementFailure,
  actionFailure,
  privateHeaders,
} from '@/lib/management.server'
import { loadPolicy, policyAction } from '@/lib/policies.server'
import { PolicyPage } from '@/views/accounts/policy-page'
export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'
export async function loader({ request, context, params }: Route.LoaderArgs) {
  try {
    return data(await loadPolicy(request, context, params), { headers: privateHeaders })
  } catch (error) {
    managementFailure(error)
  }
}
export async function action({ request, context, params }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  try {
    return await policyAction(request, context, params, form)
  } catch (error) {
    return actionFailure(error)
  }
}
export default function ProviderPolicyRoute({ loaderData, actionData }: Route.ComponentProps) {
  return (
    <PolicyPage
      key={useLocation().key}
      view={loaderData}
      result={actionData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

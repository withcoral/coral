import { data } from 'react-router'
import type { Route } from './+types/organization-secret-stores'
import { management, managementFailure, privateHeaders } from '@/lib/management.server'
import { secretStoreBlockers } from '@/lib/secret-store-state'
import { OrganizationSecretStoresPage } from '@/views/management/organization-secret-stores-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

/** Only an organization owner can list the secret stores; Coral denies anyone else. */
export async function loader({ context }: Route.LoaderArgs) {
  try {
    const { secretStores } = await management(context).secretStores.listSecretStores({})
    return data(
      {
        stores: secretStores.map((store) => ({
          name: store.name.replace(/^secretStores\//, ''),
          kind: store.kindLabel,
          blockers: secretStoreBlockers(store.state),
        })),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

export default function OrganizationSecretStoresRoute({ loaderData }: Route.ComponentProps) {
  return <OrganizationSecretStoresPage {...loaderData} />
}

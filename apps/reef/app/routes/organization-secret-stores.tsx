import { data } from 'react-router'
import type { Route } from './+types/organization-secret-stores'
import { SecretStoreKind } from '@/generated/coral/management/v1/secret_store_pb'
import { management, managementFailure, privateHeaders } from '@/lib/management.server'
import { OrganizationSecretStoresPage } from '@/views/management/organization-secret-stores-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

const kindLabel = (kind: SecretStoreKind) =>
  kind === SecretStoreKind.KV_V2 ? 'Vault / OpenBao KV v2' : 'Unknown'

/** Only an organization owner can list the secret stores; Coral denies anyone else. */
export async function loader({ context }: Route.LoaderArgs) {
  try {
    const { secretStores } = await management(context).secretStores.listSecretStores({})
    return data(
      {
        stores: secretStores.map((store) => ({
          name: store.name.replace(/^secretStores\//, ''),
          kind: kindLabel(store.kind),
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

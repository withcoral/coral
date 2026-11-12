import { data } from 'react-router'
import type { Route } from './+types/organization-secret-stores'
import { SecretStoreState } from '@/generated/coral/management/v1/secret_store_pb'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import {
  actionFailure,
  field,
  formContext,
  management,
  managementFailure,
  mutationForm,
  privateHeaders,
  requestIdFor,
} from '@/lib/management.server'
import { organizationWorkspaces, secretStoreId } from '@/lib/organization.server'
import { secretStoreBlockers } from '@/lib/secret-store-state'
import {
  OrganizationSecretStoresPage,
  type SecretStoreRow,
} from '@/views/management/organization-secret-stores-page'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'

/** Only an organization owner can list the secret stores; Coral denies anyone else. */
export async function loader({ context }: Route.LoaderArgs) {
  try {
    const api = management(context)
    const [{ secretStores }, { grants }, workspaces] = await Promise.all([
      api.secretStores.listSecretStores({}),
      api.secretStores.listSecretStoreGrants({}),
      organizationWorkspaces(api),
    ])
    const stores = new Map<string, SecretStoreRow>()
    for (const store of secretStores) {
      const name = secretStoreId(store.name)
      stores.set(name, {
        name,
        kind: store.kindLabel,
        blockers: secretStoreBlockers(store.state),
        workspaces: [],
      })
    }
    for (const { workspace, workspaceDisplayName, workspaceState, secretStore } of grants) {
      const name = secretStoreId(secretStore?.name ?? '')
      // A grant can outlive its store in config. It stays on the page so you can revoke it.
      const row = stores.get(name) ?? {
        name,
        kind: null,
        blockers: secretStoreBlockers(secretStore?.state ?? SecretStoreState.REMOVED),
        workspaces: [],
      }
      row.workspaces.push({
        name: workspace,
        displayName: workspaceDisplayName,
        archived: workspaceState === WorkspaceState.ARCHIVED,
      })
      stores.set(name, row)
    }
    return data(
      {
        stores: [...stores.values()].toSorted((a, b) => a.name.localeCompare(b.name)),
        workspaces: workspaces
          .filter((w) => w.state === WorkspaceState.ACTIVE)
          .map((w) => ({ name: w.name, displayName: w.displayName })),
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

const done: Record<string, string> = {
  grant: 'Secret store granted.',
  revoke: 'Grant revoked.',
}
const failed: Record<string, string> = {
  grant: 'Coral could not grant this secret store. ',
  revoke: 'Coral could not revoke this grant. ',
}

export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  const intent = field(form, 'intent')
  if (!Object.hasOwn(done, intent)) throw new Response('Invalid action', { status: 400 })
  const secretStore = field(form, 'name')
  const workspace = field(form, 'workspace')
  if (!/^secretStores\/[^/]+$/.test(secretStore) || !/^workspaces\/[^/]+$/.test(workspace))
    throw new Response('Invalid target', { status: 400 })
  // Every row on the page shares the form's key, so the change is part of this one.
  const requestId = requestIdFor(
    field(form, 'requestId'),
    `${intent}\0${secretStore}\0${workspace}`,
  )
  const api = management(context).secretStores
  try {
    if (intent === 'grant') await api.grantSecretStore({ workspace, secretStore, requestId })
    else await api.revokeSecretStore({ workspace, secretStore, requestId })
    return data(
      { error: null, credential: null, message: done[intent] },
      { headers: privateHeaders },
    )
  } catch (error) {
    return actionFailure(error, failed[intent])
  }
}

export default function OrganizationSecretStoresRoute({ loaderData }: Route.ComponentProps) {
  return <OrganizationSecretStoresPage {...loaderData} />
}

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

/**
 * Only an organization owner can list the secret stores; Coral denies anyone else.
 * The page does not exist where the deployment turned them off.
 */
export async function loader({ context }: Route.LoaderArgs) {
  try {
    const api = management(context)
    const [{ secretStores, disabled }, { grants }, workspaces] = await Promise.all([
      api.secretStores.listSecretStores({}),
      api.secretStores.listSecretStoreGrants({}),
      organizationWorkspaces(api),
    ])
    if (disabled) throw new Response('Not found', { status: 404, headers: privateHeaders })
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

/** The workspaces a form names in one field, each checked to be a workspace. */
function workspacesIn(form: FormData, name: 'grant' | 'revoke') {
  return form.getAll(name).map((value) => {
    if (typeof value !== 'string' || !/^workspaces\/[^/]+$/.test(value))
      throw new Response('Invalid target', { status: 400, headers: privateHeaders })
    return value
  })
}

/** Saves one edit of a store's workspaces: its grants first, then its revocations. */
export async function action({ request, context }: Route.ActionArgs) {
  const form = await mutationForm(request, context)
  if (field(form, 'intent') !== 'update') throw new Response('Invalid action', { status: 400 })
  const secretStore = field(form, 'name')
  if (!/^secretStores\/[^/]+$/.test(secretStore))
    throw new Response('Invalid target', { status: 400 })
  const changes = [
    ...workspacesIn(form, 'grant').map((workspace) => ({ intent: 'grant' as const, workspace })),
    ...workspacesIn(form, 'revoke').map((workspace) => ({ intent: 'revoke' as const, workspace })),
  ]
  if (changes.length === 0) throw new Response('Invalid form', { status: 400 })
  const api = management(context).secretStores
  for (const { intent, workspace } of changes) {
    // Every row on the page shares the form's key, so each change is part of this one.
    const requestId = requestIdFor(
      field(form, 'requestId'),
      `${intent}\0${secretStore}\0${workspace}`,
    )
    try {
      if (intent === 'grant') await api.grantSecretStore({ workspace, secretStore, requestId })
      else await api.revokeSecretStore({ workspace, secretStore, requestId })
    } catch (error) {
      // The page reloads, so the dialog offers only the changes that did not save.
      return actionFailure(
        error,
        intent === 'grant'
          ? 'Coral could not grant this secret store. '
          : 'Coral could not revoke this grant. ',
      )
    }
  }
  return data(
    { error: null, credential: null, message: 'Workspaces saved.' },
    { headers: privateHeaders },
  )
}

export default function OrganizationSecretStoresRoute({ loaderData }: Route.ComponentProps) {
  return <OrganizationSecretStoresPage {...loaderData} />
}

import { ConnectError } from '@connectrpc/connect'
import { data, useNavigation } from 'react-router'
import type { Route } from './+types/provider-settings'
import {
  formContext,
  managementFailure,
  mutationForm,
  privateHeaders,
  type ManagementApi,
} from '@/lib/management.server'
import { providerName, providerSettingsView } from '@/lib/providers.server'
import { applyProviderChange } from '@/lib/provider-changes.server'
import { authorizedWorkspace } from '@/lib/workspace-route.server'
import { ProviderAccountScope } from '@/generated/coral/management/v1/account_pb'
import { accountConnectionState } from '@/lib/accounts.server'
import { ProviderSetupInputKind } from '@/generated/coral/management/v1/provider_pb'
import { ProviderSettingsPage } from '@/views/providers/settings-page'
import { redactSourceUrl } from '@/views/providers/source-url'
import type { ProviderSettingsView, ProviderSourceView } from '@/views/providers/types'

export { ManagementErrorBoundary as ErrorBoundary } from '@/views/management/management-error'
// Rename and removal are the provider list's own mutations.
/**
 * Every change here is an owner's, as on the providers page. Your own account
 * connects through your account page. The form is parsed and checked once.
 */
export async function action({ request, context, params }: Route.ActionArgs) {
  return applyProviderChange(context, params, await mutationForm(request, context))
}

/**
 * Settings show only what existing reads return. Opening or revalidating the
 * page never prepares an update: only an explicit edit creates a Setup.
 */
export async function loader({ context, params }: Route.LoaderArgs) {
  try {
    const { api, workspace } = await authorizedWorkspace(context, params.workspace)
    const name = providerName(workspace.name, params.provider)
    const stored = await api.providers.getProvider({ name })
    const provider = providerSettingsView(stored, workspace.path)
    const owner = workspace.effectiveRole === 'owner'
    const [source, connection] = await Promise.all([
      // Source metadata is owner-only; members never reach the read. Coral
      // defines the Built-in Provider's catalog, so it has no source.
      owner && !provider.builtin ? readSource(api, name) : undefined,
      // Calls that use no account have no account to read.
      provider.account === 'none'
        ? {}
        : readConnection(api, workspace.name, name, provider.account, owner),
    ])
    return data(
      {
        workspace,
        provider: { ...provider, ...connection },
        source,
        form: formContext(context),
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    managementFailure(error)
  }
}

/**
 * The account calls use, read from the account itself: yours when each caller
 * uses their own, the shared one for owners. What it returns replaces what the
 * provider's blockers say. Members cannot inspect the shared account, and a
 * failed read has nothing to add, so both keep the blockers' reading.
 */
async function readConnection(
  api: ManagementApi,
  parent: string,
  provider: string,
  account: 'caller' | 'fixed',
  owner: boolean,
): Promise<Partial<Pick<ProviderSettingsView, 'connection' | 'canConnect' | 'canReconnect'>>> {
  if (account === 'fixed' && !owner) return { canConnect: false, canReconnect: false }
  try {
    const current = await api.accounts.getProviderAccount({
      parent,
      provider,
      scope: account === 'fixed' ? ProviderAccountScope.SHARED : ProviderAccountScope.USER,
      agent: '',
    })
    return {
      connection: accountConnectionState(current.account),
      canConnect: current.canConnect,
      canReconnect: current.canReconnect,
    }
  } catch (error) {
    // A state this Reef does not know reads as a failed read, not a failed page.
    if (!(error instanceof ConnectError) && !(error instanceof Response)) throw error
    return {}
  }
}

/** A failed source read leaves the rest of settings readable. */
async function readSource(api: ManagementApi, name: string): Promise<ProviderSourceView> {
  try {
    const source = await api.providers.getProviderSource({ name })
    const url = source.url ? redactSourceUrl(source.url) : null
    return {
      state: 'loaded',
      kind: source.kind === ProviderSetupInputKind.MCP ? 'mcp' : 'openapi',
      location: !source.url
        ? { type: 'upload' }
        : url
          ? { type: 'url', ...url }
          : { type: 'unreadable' },
    }
  } catch (error) {
    if (!(error instanceof ConnectError)) throw error
    return { state: 'failed' }
  }
}

export default function ProviderSettingsRoute({ loaderData }: Route.ComponentProps) {
  return (
    <ProviderSettingsPage
      key={loaderData.provider.id}
      {...loaderData}
      pending={useNavigation().state !== 'idle'}
    />
  )
}

import { Code, ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import {
  CredentialRoute_Kind,
  ProviderBlocker,
  ProviderState,
  SearchIndexFailureReason,
  SearchIndexState,
  type CredentialRoute,
  type ToolHints,
  type Provider,
  type ProviderTool,
} from '@/generated/coral/management/v1/provider_pb'
import { ToolAccessState, type ToolAccess } from '@/generated/coral/management/v1/policy_pb'
import type {
  AccountChoice,
  AccountLabel,
  ToolDetailView,
  ToolHintsView,
  ToolView,
  ProviderSettingsView,
  ProviderView,
  SearchIndexView,
} from '@/views/providers/types'
import type { PersonalAccountState } from '@/views/accounts/types'
import { accountLabels } from '@/views/providers/account-labels'
import { privateHeaders, type ManagementApi } from './management.server'
import { OPENAPI_DOCUMENT_SIZE_LABEL } from './provider-input-limits'

// A malformed identifier names nothing, so it reads as not found.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
export function uuidOr404(id: string) {
  if (!UUID.test(id)) throw new Response('Not found', { status: 404, headers: privateHeaders })
  return id.toLowerCase()
}
export function providerName(workspace: string, id: string) {
  return `${workspace}/providers/${uuidOr404(id)}`
}
export function providerPath(workspacePath: string, name: string) {
  return `${workspacePath}/providers/${name.split('/').at(-1)}`
}

/** Follows page tokens to the end; the tree shows every Provider. */
export async function listAllProviders(api: Pick<ManagementApi, 'providers'>, parent: string) {
  const providers: Provider[] = []
  let pageToken = ''
  do {
    const page = await api.providers.listProviders({
      parent,
      pageSize: 100,
      pageToken,
    })
    providers.push(...page.providers)
    pageToken = page.nextPageToken
  } while (pageToken)
  return providers
}
export async function listAllTools(api: Pick<ManagementApi, 'providers'>, parent: string) {
  const tools: ProviderTool[] = []
  let pageToken = ''
  do {
    const page = await api.providers.listTools({
      parent,
      pageSize: 100,
      pageToken,
    })
    tools.push(...page.tools)
    pageToken = page.nextPageToken
  } while (pageToken)
  return tools
}
export function accountLabel(route: CredentialRoute | undefined): AccountLabel {
  switch (route?.kind) {
    case CredentialRoute_Kind.FIXED:
      return accountLabels.fixed
    case CredentialRoute_Kind.CALLER:
      return accountLabels.caller
    case CredentialRoute_Kind.NONE:
      return accountLabels.none
    default:
      return accountLabels.none
  }
}
/** The account setting a Credential Route names; null for one this Reef does not know. */
export function routeChoice(route: CredentialRoute | undefined): AccountChoice | null {
  switch (route?.kind) {
    case CredentialRoute_Kind.NONE:
      return 'none'
    case CredentialRoute_Kind.FIXED:
      return 'fixed'
    case CredentialRoute_Kind.CALLER:
      return 'caller'
    default:
      return null
  }
}
export function routeFromLabel(value: string): CredentialRoute {
  switch (value) {
    case 'none':
      return { kind: CredentialRoute_Kind.NONE } as CredentialRoute
    case 'caller':
      return { kind: CredentialRoute_Kind.CALLER } as CredentialRoute
    case 'fixed':
      return { kind: CredentialRoute_Kind.FIXED } as CredentialRoute
    default:
      throw new Response('Invalid sign-in choice', {
        status: 400,
        headers: privateHeaders,
      })
  }
}

const blockerLabels: Partial<Record<ProviderBlocker, string>> = {
  [ProviderBlocker.WORKSPACE_ARCHIVED]: 'Workspace archived',
  [ProviderBlocker.PROVIDER_DISABLED]: 'Provider disabled',
  [ProviderBlocker.CALLER_ACCOUNT_REQUIRED]: 'Connect an account for this agent',
  [ProviderBlocker.SHARED_ACCOUNT_REQUIRED]: 'Connect the shared account',
  [ProviderBlocker.ACCOUNT_SETTING_INCOMPATIBLE]: 'Change the account setting',
  [ProviderBlocker.NO_TOOLS_READY]: 'No tools ready',
}
export function blockerLabel(blocker: ProviderBlocker) {
  const label = blockerLabels[blocker]
  return label ?? 'Unavailable'
}

const personalAccountActions: Record<PersonalAccountState, string | null> = {
  connected: null,
  not_connected: 'Connect your account',
  needs_reconnect: 'Reconnect your account',
}
/**
 * Coral leaves your own account out of provider readiness, so it stays apart from
 * the blockers. The account setting decides whether you have one: a missing row
 * for a separate-account provider means you never connected it.
 */
export function personalAccountAction(
  provider: Provider,
  personalAccounts: ReadonlyMap<string, PersonalAccountState>,
): string | null {
  if (accountChoice(provider.credentialRoute) !== 'caller') return null
  return personalAccountActions[personalAccounts.get(provider.name) ?? 'not_connected']
}

/** Only the hints the provider declared; Coral never assumes a default. */
export function toolHints(hints: ToolHints | undefined): ToolHintsView {
  const view: ToolHintsView = {}
  if (hints?.readOnly !== undefined) view.readOnly = hints.readOnly
  if (hints?.destructive !== undefined) view.destructive = hints.destructive
  if (hints?.idempotent !== undefined) view.idempotent = hints.idempotent
  if (hints?.openWorld !== undefined) view.openWorld = hints.openWorld
  return view
}

function accessState(value: ToolAccessState): ToolView['policy'] {
  switch (value) {
    case ToolAccessState.ENABLED:
      return 'enabled'
    case ToolAccessState.APPROVAL_REQUIRED:
      return 'approval_required'
    case ToolAccessState.DISABLED:
      return 'disabled'
    default:
      return 'approval_required'
  }
}

/**
 * Groups by the first tag, else the dotted prefix; a flat list otherwise. The
 * setup review groups its own preview by the same rule, so it lives here.
 */
export function toolGroup(o: { tags: readonly string[]; relativePath: string }) {
  if (o.tags[0]) return o.tags[0]
  const dot = o.relativePath.lastIndexOf('.')
  return dot > 0 ? o.relativePath.slice(0, dot) : ''
}

export function toolView(o: ToolAccess): ToolView {
  return {
    name: o.tool,
    relativePath: o.relativePath,
    hints: toolHints(o.hints),
    policy: accessState(o.accessState),
    available: o.available,
    reasons: o.diagnostics.map((d) => d.message),
    group: toolGroup(o),
  }
}

/** Reads the prose of the one tool the drawer shows. */
export async function toolDetail(
  api: Pick<ManagementApi, 'providers'>,
  provider: string,
  relativePath: string,
): Promise<ToolDetailView> {
  const { summary, description } = await api.providers.getTool({ parent: provider, relativePath })
  return { relativePath, summary, description }
}

// Coral bounds a page of tools with their decisions to this many.
const TOOL_ACCESS_PAGE_SIZE = 1000

function enabled(state: ProviderState) {
  switch (state) {
    case ProviderState.ACTIVE:
      return true
    case ProviderState.DISABLED:
      return false
    default:
      return false
  }
}

// Each kind of indexing error, never its message, which can hold internal detail.
const searchIndexFailureReasons: Partial<Record<SearchIndexFailureReason, string>> = {
  [SearchIndexFailureReason.INVALID_ENTRY]: 'One of the tools cannot become a search entry.',
  [SearchIndexFailureReason.TIMED_OUT]: 'Indexing took too long.',
  [SearchIndexFailureReason.CLOCK]: 'The server clock is wrong.',
  [SearchIndexFailureReason.CORRUPT]: 'The search index is damaged.',
  [SearchIndexFailureReason.PROVIDER]: "Search cannot read the provider's tools.",
  [SearchIndexFailureReason.DATABASE]: 'Search cannot use its database.',
}

/** Whether search can find the provider's tools; null when it can, or Coral sent no state. */
export function searchIndexView(provider: Provider): SearchIndexView | null {
  switch (provider.searchIndexState) {
    case SearchIndexState.PENDING:
      return { state: 'pending' }
    case SearchIndexState.FAILED:
      return {
        state: 'failed',
        reason:
          searchIndexFailureReasons[provider.searchIndexFailureReason] ?? 'No reason recorded.',
      }
    default:
      return null
  }
}

export async function providerView(
  api: ManagementApi,
  provider: Provider,
  workspacePath: string,
  /** Your own account's state for each separate-account provider, by provider name. */
  personalAccounts: ReadonlyMap<string, PersonalAccountState>,
): Promise<ProviderView> {
  const path = providerPath(workspacePath, provider.name)
  const toolViews: ToolView[] = []
  let pageToken = ''
  do {
    const page = await api.policies.listToolAccess({
      parent: provider.name,
      pageSize: TOOL_ACCESS_PAGE_SIZE,
      pageToken,
    })
    for (const tool of page.tools) toolViews.push(toolView(tool))
    pageToken = page.nextPageToken
  } while (pageToken)
  const id = provider.name.split('/').at(-1)!
  const isEnabled = enabled(provider.state)
  return {
    id,
    displayName: provider.displayName,
    namespace: provider.namespace,
    callRoot: provider.callRoot,
    builtin: provider.builtin,
    account: accountLabel(provider.credentialRoute),
    blockers: provider.blockers.map(blockerLabel),
    personalAccountAction: personalAccountAction(provider, personalAccounts),
    searchIndex: searchIndexView(provider),
    readyCount: toolViews.filter((tool) => tool.available).length,
    totalCount: toolViews.length,
    etag: provider.etag,
    path,
    enabled: isEnabled,
    tools: toolViews,
  }
}

/** Whose account a provider's calls use, from its credential route. */
export function accountChoice(route: CredentialRoute | undefined): AccountChoice {
  return routeChoice(route) ?? 'none'
}

/**
 * Settings from the provider read alone; it holds no configuration or source.
 * The shared account's state comes from its blocker; your own account's is
 * unknown here. The settings loader replaces both with the account read where
 * the reader may make one.
 */
export function providerSettingsView(
  provider: Provider,
  workspacePath: string,
): ProviderSettingsView {
  const account = accountChoice(provider.credentialRoute)
  return {
    id: provider.name.split('/').at(-1)!,
    displayName: provider.displayName,
    namespace: provider.namespace,
    callRoot: provider.callRoot,
    builtin: provider.builtin,
    etag: provider.etag,
    path: providerPath(workspacePath, provider.name),
    enabled: enabled(provider.state),
    toolsReady: !provider.blockers.includes(ProviderBlocker.NO_TOOLS_READY),
    account,
    // Blockers report the shared account, but not a User's own account.
    connection:
      account === 'caller'
        ? 'unknown'
        : provider.blockers.includes(ProviderBlocker.SHARED_ACCOUNT_REQUIRED)
          ? 'not_connected'
          : 'connected',
    canConnect: true,
    canReconnect: true,
    accountSettingIncompatible: provider.blockers.includes(
      ProviderBlocker.ACCOUNT_SETTING_INCOMPATIBLE,
    ),
  }
}

const reasons: Record<string, string> = {
  ETAG_MISMATCH: 'This provider changed. Reload the page. Review the changes before you try again.',
  PROVIDER_CHANGED:
    'This provider changed. Reload the page. Review the changes before you try again.',
  NAMESPACE_IN_USE: 'This program name is in use in this workspace. Select a different name.',
  ACCOUNT_REQUIRED: 'This choice needs an account. Sign in first.',
  WORKSPACE_ARCHIVED: 'This workspace is archived. Restore it before you make changes.',
  INVALID_LIFECYCLE_STATE: 'The provider is already in that state. Reload the page.',
  BUILTIN_PROVIDER_MANAGED:
    'You cannot make this change to a provider Coral manages. Reload the page.',
  REQUEST_ID_CONFLICT:
    'Coral used this request for a different change. Reload the page. Start the change again.',
  SETUP_CONSUMED: 'This setup is complete. Start again to continue.',
  CONSENT_DESTINATION_REJECTED:
    'Coral cannot open this sign-in address. Use HTTPS or a local or private HTTP address.',
  // One message for both a configured denial and the fixed blocklist: the
  // rejection names neither, and the copy must not reveal the address class.
  OUTBOUND_DESTINATION_REJECTED:
    "Coral's outbound settings do not allow this address. Check the deployment's outbound settings before you try again.",
  SETUP_NOT_READY: 'Complete the previous steps before you add or update the provider.',
  SETUP_NOT_APPLICABLE: 'That step does not apply now. Reload the page.',
  INPUT_UNAVAILABLE: 'Coral could not reach this address. Check it and try again.',
  OPENAPI_DOCUMENT_TOO_LARGE: `This OpenAPI document is larger than ${OPENAPI_DOCUMENT_SIZE_LABEL}. Select a smaller document.`,
  OPENAPI_DOCUMENT_NOT_TEXT: 'This document is not text. Select a JSON or YAML OpenAPI document.',
  CLIENT_BOOTSTRAP_UNAVAILABLE:
    'This server needs an OAuth client ID. Register Coral with the server to get one.',
  CLIENT_METADATA_NOT_PUBLIC:
    "Coral cannot confirm that this server can read Coral's client metadata document. The document address resolves to a non-public address on Coral's host. Enter a client ID. If you run Coral, you can also publish the document on a public address. Or set the MCP client bootstrap policy to prefer the document.",
  CLIENT_METADATA_UNRESOLVED:
    "Coral cannot confirm that this server can read Coral's client metadata document. Coral's host could not resolve the document address. Enter a client ID. If you run Coral, check the document address. Or set the MCP client bootstrap policy to prefer the document.",
  INVALID_PROVIDER_INPUT: 'Coral cannot use this document. Check the fields.',
}
const statuses: Partial<Record<Code, number>> = {
  [Code.PermissionDenied]: 403,
  [Code.NotFound]: 404,
  [Code.InvalidArgument]: 400,
  [Code.FailedPrecondition]: 409,
  [Code.AlreadyExists]: 409,
  [Code.Aborted]: 409,
}
/** Maps a failed Provider mutation to one safe sentence and a status the form can show. */
export function providerFailure(error: unknown, tool = 'Coral could not apply this change. ') {
  if (error instanceof Response) throw error
  if (!(error instanceof ConnectError)) console.error('Unexpected failure in a Reef action:', error)
  const status = error instanceof ConnectError ? (statuses[error.code] ?? 503) : 503
  const reason =
    error instanceof ConnectError && error.rawMessage.startsWith('AUTHORIZATION_DISCOVERY_')
      ? "Coral does not support this server's sign-in requirements."
      : error instanceof ConnectError
        ? reasons[error.rawMessage]
        : undefined
  const message =
    reason ??
    (status === 503
      ? 'Reload the page to check whether Coral applied the change before you submit it again.'
      : status === 403
        ? 'You do not have permission for this action.'
        : status === 404
          ? 'Coral cannot find this item. Reload the page.'
          : 'Check the form fields. Correct any errors before you try again.')
  return data(
    {
      error: reason ? message : `${tool}${message}`,
      credential: null,
    },
    { status, headers: privateHeaders },
  )
}

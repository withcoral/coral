import type { Crumb, FormContext, ActionResult, WorkspaceView } from '@/views/management/types'
import type { CredentialFieldView } from '@/views/providers/types'
export type { FormContext, ActionResult }

/** Safe Account metadata. The configuring Coral User is not an upstream identity. */
export interface AccountView {
  id: string
  canManage: boolean
  state: 'ready' | 'reconnect' | 'disconnected'
  automaticRefresh?: boolean
  /** The OAuth grant exactly as the service reported it; empty for secrets. */
  grantedScope: string
  connectedAt: string
  configuredByUser: string
}
export interface AccountsView {
  workspace?: WorkspaceView
  parents: Crumb[]
  title: string
  path: string
  /** Your own account lives under Accounts, never here. */
  scope: 'shared' | 'agent'
  account?: AccountView
  scopes: string[]
  needsCredential: boolean
  usesScope: boolean
  canConnect: boolean
  canReconnect: boolean
  active: boolean
  route: 'none' | 'caller' | 'fixed'
  form: FormContext
}
export interface AuthorizationView {
  providerName: string
  /** Validated root-relative navigation only; it grants no authority. */
  returnTo: string
  scope: 'user' | AccountsView['scope']
  path: string
  parents: Crumb[]
  state: 'pending' | 'exchanging' | 'succeeded' | 'failed' | 'canceled' | 'expired'
  /** Your own account page, where a new attempt starts after this one ends. */
  retryPath?: string
  oauth: boolean
  userCode?: string
  applicationFailure?: boolean
  failure?: string
  automaticRefresh?: boolean
  /** The OAuth grant exactly as the service reported it; empty for secrets. */
  grantedScope: string
  /** The Credential Fields to enter for secret entry, in name order. */
  credentialFields: CredentialFieldView[]
  form: FormContext
}
/** Durable Account Connection Request state, as the Run shows it. */
export type ConnectionState =
  | 'pending'
  | 'connected'
  | 'declined'
  | 'expired'
  | 'route_changed'
  | 'authorization_changed'
/** Your own account state for one separate-account provider. */
export type PersonalAccountState = 'connected' | 'not_connected' | 'needs_reconnect'
/** One row of your Accounts page. */
export interface UserAccountRow {
  provider: string
  name: string
  state: PersonalAccountState
  accountId?: string
  /** OAuth signs in at the provider in one click; secrets open a dialog here. */
  oauth: boolean
  /** You can start a connection or a reconnect now. */
  canConnect: boolean
  canDisconnect: boolean
  /** Deadline of your pending connection request, if any. */
  waitingUntil?: string
  connectPath: string
  /** Account detail: granted permissions, reconnect, and disconnect. */
  detailPath: string
}
export interface UserAccountsView {
  workspace: WorkspaceView
  path: string
  accounts: UserAccountRow[]
  form: FormContext
}
/** Your own account for one provider: connect in one click, inspect, or disconnect. */
export interface UserAccountView {
  workspace: WorkspaceView
  providerName: string
  /** Form action, carrying the validated return target. */
  path: string
  /** Validated root-relative navigation only; it grants no authority. */
  returnTo: string | null
  accountsPath: string
  state: PersonalAccountState
  account?: AccountView
  scopes: string[]
  canConnect: boolean
  canDisconnect: boolean
  oauth: boolean
  credentialFields: CredentialFieldView[]
  form: FormContext
}

import type { PolicyState } from '@/components/policy-toggle'
import type { ActionResult, FormContext, WorkspaceView } from '@/views/management/types'
import type {
  AccountChoice,
  CreationAttempt,
  UpdateAttempt,
} from '@/lib/provider-publication-attempt/model'
import type { SourceChoice } from '@/lib/provider-recipes/choice'
import type { PersonalAccountState } from '@/views/accounts/types'
import type { AccountLabel } from './account-labels'
import type { SelectedTool } from './selection'
export type { AccountChoice } from '@/lib/provider-publication-attempt/model'

export type { ActionResult, FormContext, WorkspaceView }

/** What happened to one tool in a bulk policy update. `unconfirmed` is
 * the page's own reading of a lost response; the action never sends it. */
export type ToolOutcome = SelectedTool &
  ({ status: 'updated' } | { status: 'unconfirmed' } | { status: 'failed'; reason: string })

/** A bulk policy update's result: the shared shape plus one outcome per tool. */
export interface PolicyActionResult extends ActionResult {
  tools?: ToolOutcome[]
}

/**
 * The least a Tool needs to appear in the tree. A Setup that has not been
 * published yet can say this much; only an installed Provider can say more.
 */
export interface ToolNodeView {
  /** Tool-relative path, unique within its Provider. */
  relativePath: string
  /** Provider-authored summary; empty when the Provider gave none. */
  summary: string
  available: boolean
  /** Coral-authored reasons the Tool cannot be called; empty when available. */
  reasons: string[]
  /** The first OpenAPI tag, else the dotted prefix, else empty. */
  group: string
  /** Where selecting it navigates; empty before the Provider is published. */
  path: string
}

/** Effect hints the provider declared. An absent hint is undeclared, not false. */
export interface ToolHintsView {
  readOnly?: boolean
  destructive?: boolean
  idempotent?: boolean
  openWorld?: boolean
}

/** One Tool as the tree and its detail pane show it. */
export interface ToolView extends ToolNodeView {
  /** Full management API resource name, used for policy writes. */
  name: string
  /** The full call path Programs use; the relative path when not installed. */
  callPath: string
  /** Safe Provider-authored prose; may contain Markdown. */
  description: string
  hints: ToolHintsView
  policy: PolicyState
}

/** One tool the live provider has, as a refresh compares it. */
export type LiveToolView = Pick<ToolNodeView, 'relativePath' | 'summary'>

/** One previewed Tool, as the tools step groups and lists it. */
export interface ReviewToolView extends ToolNodeView {
  /** Tool Key, which the destination map and the unavailable list are keyed by. */
  key: string
}

/** How a Provider authenticates with its Upstream Service; the Reef spelling of Coral's CredentialMethod. */
export type CredentialMethod = 'none' | 'bearer' | 'header' | 'basic' | 'query' | 'oauth' | 'device'

/** The Credential Route as Reef names it: "Whose account does it use?" */
export type { AccountLabel } from './account-labels'

/** Search has not indexed the provider's tools yet, or could not, and why. */
export type SearchIndexView = { state: 'pending' } | { state: 'failed'; reason: string }

export interface ProviderView {
  id: string
  displayName: string
  namespace: string
  /** Coral defines and manages this provider. */
  builtin: boolean
  account: AccountLabel
  /** Coral's provider blockers: provider health, not your own account. */
  blockers: string[]
  /** What you must do with your own account before you call the provider, if anything. */
  personalAccountAction: string | null
  /** Whether search can find the provider's tools; null when it can, or when Coral sent no state. Calls still work. */
  searchIndex: SearchIndexView | null
  readyCount: number
  totalCount: number
  etag: string
  path: string
  enabled: boolean
  tools: ToolView[]
}

/** What the settings page can read about one provider without owner-only or write calls. */
export interface ProviderSettingsView {
  id: string
  displayName: string
  namespace: string
  /** The provider's complete Call Root, such as `coral.providers.github`. */
  callRoot: string
  /** Coral defines and manages this provider; owners cannot change it. */
  builtin: boolean
  etag: string
  path: string
  /** Disabled providers keep their settings but Coral refuses their calls. */
  enabled: boolean
  /** False when Coral can call none of the provider's tools. */
  toolsReady: boolean
  account: AccountChoice
  /** The state of the account calls use: yours, or the shared one. Unknown when unread. */
  connection: PersonalAccountState | 'unknown'
  /** Coral lets the reader start a connection for that account. */
  canConnect: boolean
  /** Coral lets the reader replace that account's authorization. */
  canReconnect: boolean
  /** Coral reports that the saved account setting no longer suits the provider. */
  accountSettingIncompatible: boolean
}

/** The owner-only source read: its kind and a redacted address, or a failed read. */
export type ProviderSourceView =
  | {
      state: 'loaded'
      kind: 'openapi' | 'mcp'
      location:
        | { type: 'url'; url: string; host: string }
        | { type: 'upload' }
        | { type: 'unreadable' }
    }
  | { state: 'failed' }

export interface ProviderSettingsPageProps {
  workspace: WorkspaceView
  provider: ProviderSettingsView
  /** Absent for members: they cannot read the source. */
  source?: ProviderSourceView
  form: FormContext
  pending?: boolean
}

export interface WorkspaceProvidersPageProps {
  workspace: WorkspaceView
  recipes?: readonly SourceChoice[]
  providers: ProviderView[]
  selectedProvider: string | null
  selectedTool: string | null
  form: FormContext
  result?: ActionResult
  pending?: boolean
}

/** Safe progress of one Add or Update attempt; never a navigation URL, secret, or code. */
/**
 * What Coral sends in one credential header or query parameter: a Credential
 * Field, or a constant. Only a header puts text before the value.
 */
export type CredentialBindingValue =
  | { name: string; source: 'field'; field: string; prefix?: string }
  | { name: string; source: 'literal'; value: string }

/** Where one part of a credential comes from: a Credential Field, or a constant. */
export type CredentialSourceValue =
  | { source: 'field'; field: string }
  | { source: 'literal'; value: string }

/** The two parts Coral encodes as HTTP Basic. */
export interface BasicAuthValue {
  username: CredentialSourceValue
  password: CredentialSourceValue
}

/** How the owner asks for one Credential Field; none of it reaches the service. */
export interface CredentialFieldPresentation {
  label: string
  description: string
  /** Masks the value while it is typed. */
  sensitive: boolean
}

/** One Credential Field a person enters when connecting an account. */
export interface CredentialFieldView extends CredentialFieldPresentation {
  name: string
}

export interface ConfigurationValues {
  /** The provider's name in Reef; the source's title unless the owner changes it. */
  displayName: string
  /** The confirmed method; empty until the owner chooses one. */
  authentication: CredentialMethod | ''
  /** What Coral sends in each credential header, when the method is 'header'. */
  headers: CredentialBindingValue[]
  /** The username and password, when the method is 'basic'. */
  basic: BasicAuthValue
  /** What Coral sends in each credential query parameter, when the method is 'query'. */
  parameters: CredentialBindingValue[]
  /** How each Credential Field is asked for, by field name, when the method sends fields. */
  credentialFields: Record<string, CredentialFieldPresentation>
  authorizationUrl: string
  tokenUrl: string
  scopes: string[]
  /** Non-secret service headers, independent of credential headers. */
  requestHeaders?: { name: string; value: string }[]
  scopeFormat?: 'space' | 'comma'
  servers: {
    declaration: string
    url: string
    custom: boolean
    variables: Record<string, string>
  }[]
}

export interface ConfigurationReviewView {
  application?: {
    mode: 'automatic' | 'manual'
    clientId: string
    authentication?: 'none' | 'basic' | 'post'
    pkce?: 's256' | 'disabled'
    /** Editable query text; rejected drafts retain duplicates for correction. */
    parameters?: string
  }
  revision: string
  confirmed: boolean
  authenticationMethods: CredentialMethod[]
  discoveryAuthorizationRequired: boolean
  automaticClientAvailable: boolean
  issuer: string
  resource: string
  deviceAuthorizationUrl?: string
  differences: { field: string; saved?: string; document?: string }[]
  /** null means compatibility has not been evaluated, not that the update is compatible. */
  interruptsAccess: boolean | null
  updating: boolean
  values: ConfigurationValues
  authenticationStatus: string
  /** The title the document or server gives itself; empty when it has none. */
  documentDisplayName: string
  servers: {
    declaration: string
    tools: string[]
    options: {
      url: string
      variables: { name: string; defaultValue: string; enumerated: boolean; values: string[] }[]
    }[]
  }[]
  destinations: Record<string, string>
  /** Exact preview count, including a known empty catalog; null when not established. */
  toolCount: number | null
  /** Every previewed Tool in catalog order; empty until a catalog is read. */
  tools: ReviewToolView[]
  unavailableTools: { key: string; reasons: string[] }[]
  diagnostic: string
  /** False when Coral cannot read the source; the diagnostic says why. */
  readable: boolean
  errors: Record<string, string>
}

/** One recipe input the configuration step asks for. Inputs are never credentials. */
export interface RecipeInputView {
  name: string
  label: string
  description: string
  type: 'string' | 'url' | 'hostname'
  /** The recipe's default answer; empty when the person must enter one. */
  defaultValue: string
  /** The allowed answers; empty when any answer is accepted. */
  choices: string[]
}

/** A server variable: text that can refer to inputs, or a value chosen by an input. */
export type RecipeVariableView = string | { input: string; values: Record<string, string> }

export interface RecipeServerView {
  declaration: string
  url: string
  custom: boolean
  variables: Record<string, RecipeVariableView>
}

/**
 * What a recipe fills in on a first configuration, already checked against what
 * Coral read. Text can refer to inputs as {{input.name}}; the page fills them in.
 */
export interface RecipeConfigurationView {
  id: string
  inputs: RecipeInputView[]
  /** The offered sign-in methods the recipe names, listed first as suggested. */
  suggestedMethods: CredentialMethod[]
  values: Partial<
    Pick<
      ConfigurationValues,
      | 'authentication'
      | 'authorizationUrl'
      | 'tokenUrl'
      | 'scopes'
      | 'scopeFormat'
      | 'requestHeaders'
      | 'headers'
      | 'basic'
      | 'parameters'
      | 'credentialFields'
    >
  > & { servers?: RecipeServerView[] }
  application?: NonNullable<ConfigurationReviewView['application']>
  /** Help below the client ID field, as Markdown, such as where to create the application. */
  clientConfigHintMarkdown?: string
}

export interface SetupView {
  configuration?: ConfigurationReviewView
  id: string
  kind: 'openapi' | 'mcp'
  state:
    | 'awaiting_configuration'
    | 'awaiting_authorization'
    | 'awaiting_discovery'
    | 'ready'
    | 'consumed'
    | 'expired'
    | 'canceled'
  /** Null when a newer Coral server chose a method this Reef does not know. */
  credentialMethod: CredentialMethod | null
  /** The Credential Fields a person enters to sign in, in name order; empty for OAuth. */
  credentialFields: CredentialFieldView[]
  /** The inspecting User holds a live sign-in for this attempt. */
  authorized: boolean
  /** The provisional account offered by this Setup, as a bare UUID; never a credential. */
  provisionalGrantId?: string
  clientSecretConfigured?: boolean
  automaticRefresh?: boolean
  /** The OAuth grant exactly as the service reported it; empty for secrets. */
  grantedScope: string
  authorization: {
    userCode?: string
    state: 'pending' | 'exchanging' | 'succeeded' | 'failed' | 'canceled' | 'expired'
    requestedScopes: string[]
    bootstrap: 'unknown' | 'document' | 'registration' | 'manual'
    /** Stable failure code, empty unless failed. */
    failure: string
    /** The public client identifier the sign-in runs through; never a secret. */
    clientId: string
    expiresAt: string
  } | null
  /** Why the last attempt to list Tools after sign-in did not complete; empty otherwise. */
  discoveryFailure: string
  /** When this attempt expires; empty when it does not. */
  expiresAt: string
  /** The URL entered, or a label for the uploaded document. */
  sourceLabel: string
  /** The host the source was read from; empty for an uploaded document. */
  sourceHost: string
  /**
   * The host of the service a sign-in authorizes, never an OpenAPI document's
   * own host. Empty when that is unknown or split across hosts.
   */
  serverName: string
  /**
   * What a Program's addProvider call asked this Setup to publish. Absent
   * when a person started the Setup. The person can change either. The route
   * is null when a newer Coral asked for one this Reef does not know.
   */
  request?: { namespace: string; route: AccountChoice | null }
  /**
   * The routes Coral says suit the confirmed configuration. Empty until it is
   * confirmed. A route this Reef does not know is left out.
   */
  allowedRoutes: AccountChoice[]
  /** Coral says the inspecting user can sign in, or enter a credential, now. */
  connectAvailable: boolean
  /** Coral says each person can connect their own account after an add with no sign-in. */
  connectLaterAvailable: boolean
}

export interface StepperResult extends ActionResult {
  /** Safe domain field errors for the submitted configuration, not persisted settings. */
  configurationErrors?: Record<string, string>
  /** Submitted non-secret fields, so document submissions can restore rejected edits. */
  configurationDraft?: Pick<ConfigurationReviewView, 'values' | 'application'> & {
    namespace?: string
    /** Recipe input answers, so a rejected draft keeps them. */
    recipeInputs?: Record<string, string>
  }
  /** The attempt finished with a failure Coral recorded. */
  failed?: string
}

export interface StepperPageProps {
  workspace: WorkspaceView
  /** Namespaces other providers in this workspace have; Configuration refuses them. */
  takenNamespaces?: readonly string[]
  /** Display names other providers in this workspace have; Configuration refuses them. */
  takenDisplayNames?: readonly string[]
  /** The recipe this attempt was read from; it suggests the name and namespace. */
  recipe?: SourceChoice | null
  /** Recipes the catalog offers, from the bundled library. */
  recipes?: readonly SourceChoice[]
  /** What the chosen recipe fills in on a first configuration. */
  recipeConfiguration?: RecipeConfigurationView | null
  /** Where the chosen recipe says to find the credentials to enter. */
  recipeDocumentationUrl?: string | null
  /** The stepper's own URL, without query. */
  basePath: string
  cancelPath: string
  setup: SetupView | null
  attempt: CreationAttempt
  /** Reef's exact OAuth callback, to register with an OAuth application; empty when not needed. */
  callbackUrl: string
  form: FormContext
  result?: StepperResult
  pending?: boolean
}

export interface UpdatePageProps {
  replacingSource?: boolean
  source?: { url: string; kind: 'mcp' | 'url' }
  currentAccount?: AccountChoice
  applicationProblem?: boolean
  application?: { clientId: string; secretConfigured: boolean; problem: boolean }
  callbackUrl?: string
  displayName: string
  /** The provider's version now. Start again opens a new Setup against it. */
  etag?: string
  basePath: string
  cancelPath: string
  /** The provider's settings, where Edit configuration opens from. */
  settingsPath?: string
  /** The live provider's tools; a refresh groups the new and removed ones. */
  currentTools?: readonly LiveToolView[]
  providersPath: string
  setup: SetupView | null
  attempt: UpdateAttempt
  form: FormContext
  result?: StepperResult
  pending?: boolean
}

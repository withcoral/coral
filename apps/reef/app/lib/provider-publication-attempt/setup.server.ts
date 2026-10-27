import { redirectToProviderConsent } from '../provider-consent.server'
import { createHash } from 'node:crypto'
import { create, type MessageInitShape } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { createCookie, data, redirect, type RouterContextProvider } from 'react-router'
import { providerSetupRedirectUri } from '@/auth/config.server'
import type { AuthConfig } from '@/auth/types'
import { isExplicitLoopbackUrl } from '@/lib/loopback.server'
import { runtimeConfigContext } from '@/lib/runtime-config.server'
import {
  type BasicAuthentication,
  ClientBootstrap,
  type QueryCredentialsAuthentication,
  type CredentialSource,
  type CredentialSourceSchema,
  OAuthClientAuthenticationSchema,
  OAuthPkcePolicy,
  OAuthScopeFormat,
  CredentialMethod,
  ProviderSetupInputKind,
  ProviderSetupState,
  SetupAuthorizationState,
  type ProviderSetup,
  type ProviderConfiguration,
  ProviderConfigurationSchema,
} from '@/generated/coral/management/v1/provider_pb'
import type {
  AccountChoice,
  BasicAuthValue,
  ConfigurationValues,
  CredentialSourceValue,
  ConfigurationReviewView,
  CredentialMethod as ReefCredentialMethod,
  CredentialBindingValue,
  SetupView,
  StepperResult,
} from '@/views/providers/types'
import { credentialFieldViews, credentialValuesFrom } from '../credential-fields.server'
import { field, privateHeaders, type ManagementApi } from '../management.server'
import { safeInternalPath } from '@/auth/safe-path.server'
import { toolGroup, providerFailure, routeChoice, uuidOr404 } from '../providers.server'
import { OPENAPI_DOCUMENT_MAX_BYTES, OPENAPI_DOCUMENT_SIZE_LABEL } from '../provider-input-limits'
import { recipeDocument } from '../provider-recipes/library.server'
import {
  accountChoice,
  attemptPath,
  freezeAttempt,
  isCredentialFree,
  parseAttempt,
  reviseAttempt,
  savesDirectly,
  type FrozenAttempt,
  type PublicationAttempt,
} from './model'

function isoTime(time: { seconds: bigint; nanos: number } | undefined) {
  return time ? new Date(Number(time.seconds) * 1000).toISOString() : ''
}

function enumLabel<T extends string>(value: number, labels: Partial<Record<number, T>>): T {
  const label = labels[value]
  if (label === undefined)
    throw new Response('Unrecognized management classification', {
      status: 503,
    })
  return label
}

type WireMethod = Exclude<CredentialMethod, CredentialMethod.UNSPECIFIED>
type AuthenticationCase = NonNullable<ProviderConfiguration['authentication']['case']>
type MethodRow = { method: WireMethod; case: AuthenticationCase }

/**
 * Accepts a table only when its rows name every wire method and every
 * authentication case, so a method Coral adds fails to build until Reef names it.
 */
function completeMethods<const T extends Record<ReefCredentialMethod, MethodRow>>(
  table: T &
    (WireMethod extends T[ReefCredentialMethod]['method']
      ? AuthenticationCase extends T[ReefCredentialMethod]['case']
        ? unknown
        : { missingCase: Exclude<AuthenticationCase, T[ReefCredentialMethod]['case']> }
      : { missingMethod: Exclude<WireMethod, T[ReefCredentialMethod]['method']> }),
): T {
  return table
}

/**
 * Each of Reef's method names with its two wire spellings: the enum Coral uses in
 * lists, and the case of a configuration's authentication.
 */
const METHODS = completeMethods({
  none: { method: CredentialMethod.NONE, case: 'none' },
  bearer: { method: CredentialMethod.BEARER, case: 'bearer' },
  header: { method: CredentialMethod.HEADER_CREDENTIALS, case: 'headerCredentials' },
  basic: { method: CredentialMethod.BASIC_AUTH, case: 'basicAuth' },
  query: { method: CredentialMethod.QUERY_CREDENTIALS, case: 'queryCredentials' },
  device: { method: CredentialMethod.OAUTH_DEVICE_CODE, case: 'oauthDeviceCode' },
  oauth: { method: CredentialMethod.OAUTH_AUTHORIZATION_CODE, case: 'oauthAuthorizationCode' },
})

const REEF_METHODS = Object.keys(METHODS) as ReefCredentialMethod[]

/** Coral's credential method, in the words the views use. */
const CREDENTIAL_METHODS = new Map(REEF_METHODS.map((name) => [METHODS[name].method, name]))

/** Each authentication case Coral sends, in the words the views use. */
const AUTHENTICATION_CASES = Object.fromEntries(
  REEF_METHODS.map((name) => [METHODS[name].case, name]),
) as Record<AuthenticationCase, ReefCredentialMethod>

/**
 * A newer Coral server can offer methods this Reef was built without (ADR 0004).
 * Reef leaves those out rather than failing the page. An unspecified method is
 * also null, without a warning.
 */
export function knownCredentialMethod(value: CredentialMethod): ReefCredentialMethod | null {
  if (value === CredentialMethod.UNSPECIFIED) return null
  const label = CREDENTIAL_METHODS.get(value)
  if (label === undefined) {
    console.warn(`Coral sent credential method ${value}, which this Reef does not know`)
    return null
  }
  return label
}

/** Safe Setup progress for the stepper. Never the navigation URL, a secret, or a code. */
export function setupView(s: ProviderSetup): SetupView {
  const authorization = s.authorization
  return {
    id: s.name.split('/').at(-1)!,
    kind: enumLabel(s.inputKind, {
      [ProviderSetupInputKind.OPENAPI]: 'openapi',
      [ProviderSetupInputKind.MCP]: 'mcp',
    }),
    configuration: s.configuration ? configurationView(s.configuration, s.inputKind) : undefined,
    state: enumLabel(s.state, {
      [ProviderSetupState.AWAITING_CONFIGURATION]: 'awaiting_configuration',
      [ProviderSetupState.AWAITING_AUTHORIZATION]: 'awaiting_authorization',
      [ProviderSetupState.AWAITING_DISCOVERY]: 'awaiting_discovery',
      [ProviderSetupState.READY]: 'ready',
      [ProviderSetupState.CONSUMED]: 'consumed',
      [ProviderSetupState.EXPIRED]: 'expired',
      [ProviderSetupState.CANCELED]: 'canceled',
    }),
    credentialMethod: knownCredentialMethod(s.credentialMethod),
    credentialFields: credentialFieldViews(s.credentialFields),
    authorized: s.authorized,
    provisionalGrantId: s.provisionalGrantId || undefined,
    clientSecretConfigured: s.clientSecretConfigured,
    automaticRefresh: s.automaticRefresh,
    grantedScope: s.grantedScope,
    authorization: authorization
      ? {
          state: enumLabel(authorization.state, {
            [SetupAuthorizationState.PENDING]: 'pending',
            [SetupAuthorizationState.EXCHANGING]: 'exchanging',
            [SetupAuthorizationState.SUCCEEDED]: 'succeeded',
            [SetupAuthorizationState.FAILED]: 'failed',
            [SetupAuthorizationState.CANCELED]: 'canceled',
            [SetupAuthorizationState.EXPIRED]: 'expired',
          }),
          userCode: authorization.userCode,
          requestedScopes: authorization.requestedScopes,
          bootstrap: enumLabel(authorization.clientBootstrap, {
            [ClientBootstrap.UNSPECIFIED]: 'unknown',
            [ClientBootstrap.CLIENT_ID_METADATA_DOCUMENT]: 'document',
            [ClientBootstrap.DYNAMIC_CLIENT_REGISTRATION]: 'registration',
            [ClientBootstrap.MANUAL]: 'manual',
          }),
          failure: authorization.failureCode,
          clientId: authorization.clientId,
          expiresAt: isoTime(authorization.expireTime),
        }
      : null,
    discoveryFailure: s.discoveryFailure?.message ?? '',
    expiresAt: isoTime(s.expireTime),
    sourceLabel: s.inputUrl || 'Uploaded document',
    sourceHost: s.inputUrl ? safeHost(s.inputUrl) : '',
    serverName: serviceHost(s),
    ...setupRequest(s),
    allowedRoutes: allowedRoutes(s),
    connectAvailable: s.connectAvailable,
    connectLaterAvailable: s.connectLaterAvailable,
  }
}

/** The routes Coral says suit the confirmed configuration, without any this Reef does not know. */
function allowedRoutes(s: ProviderSetup): AccountChoice[] {
  return s.allowedCredentialRoutes
    .map(routeChoice)
    .filter((route): route is AccountChoice => route !== null)
}

/**
 * What a Program asked the Setup to publish. A route this Reef does not know
 * leaves the account question to the person, and the Setup is still the Program's.
 */
function setupRequest(s: ProviderSetup): Pick<SetupView, 'request'> {
  return s.request
    ? { request: { namespace: s.request.namespace, route: routeChoice(s.request.credentialRoute) } }
    : {}
}

/**
 * The host that receives authorized calls. An MCP input URL is the server
 * itself; an OpenAPI document can live anywhere, so its API destinations
 * decide. Several hosts, or none yet, name nothing rather than guess.
 */
function serviceHost(s: ProviderSetup) {
  if (s.inputKind === ProviderSetupInputKind.MCP) return s.inputUrl ? safeHost(s.inputUrl) : ''
  const hosts = new Set(
    Object.values(s.configuration?.destinations ?? {}).flatMap((url) => hostOf(url) ?? []),
  )
  return hosts.size === 1 ? [...hosts][0]! : ''
}

/** Whitelist display fields: application authentication can contain a write-only secret. */
function configurationDraft(
  value: ProviderConfiguration,
): Pick<ConfigurationReviewView, 'values' | 'application'> {
  const { authentication } = value
  // A case this Reef does not know arrives unset, so the owner chooses again.
  const oauth =
    authentication.case === 'oauthAuthorizationCode' || authentication.case === 'oauthDeviceCode'
      ? authentication.value
      : undefined
  // Both parts must be sources this Reef knows; one it does not leaves the
  // method out, rather than sending a part Coral never saved (ADR 0004).
  const basic = authentication.case === 'basicAuth' ? basicAuth(authentication.value) : null
  const parameters =
    authentication.case === 'queryCredentials'
      ? queryParameters(authentication.value.parameters)
      : null
  // The method reads only when every part of it does.
  const unreadable =
    (authentication.case === 'basicAuth' && !basic) ||
    (authentication.case === 'queryCredentials' && !parameters)
  const application = oauth?.application
  return {
    application: application?.mode.case
      ? {
          mode: application.mode.case === 'clientId' ? 'manual' : 'automatic',
          clientId:
            application.mode.case === 'clientId' ? application.mode.value : oauth!.oauthClientId,
          authentication:
            application.authentication?.method.case === 'clientSecretBasic'
              ? 'basic'
              : application.authentication?.method.case === 'clientSecretPost'
                ? 'post'
                : 'none',
          pkce: application.pkce === OAuthPkcePolicy.DISABLED ? 'disabled' : 's256',
          parameters: new URLSearchParams(application.authorizationParameters).toString(),
        }
      : undefined,
    values: {
      displayName: value.displayName,
      requestHeaders: Object.entries(value.requestHeaders).map(([name, headerValue]) => ({
        name,
        value: headerValue,
      })),
      authentication:
        authentication.case && !unreadable ? AUTHENTICATION_CASES[authentication.case] : '',
      headers:
        authentication.case === 'headerCredentials'
          ? Object.entries(authentication.value.headers)
              .toSorted(([left], [right]) => left.localeCompare(right))
              // A binding with a source this Reef does not know is left out.
              .flatMap(([name, { source }]): CredentialBindingValue[] => {
                if (source.case === 'field')
                  return [
                    {
                      name,
                      source: 'field',
                      field: source.value.field,
                      prefix: source.value.prefix,
                    },
                  ]
                if (source.case === 'literal')
                  return [{ name, source: 'literal', value: source.value }]
                return []
              })
          : [],
      basic: basic ?? NO_BASIC_AUTH,
      parameters: parameters ?? [],
      credentialFields:
        authentication.case === 'headerCredentials' ||
        authentication.case === 'basicAuth' ||
        authentication.case === 'queryCredentials'
          ? Object.fromEntries(
              Object.entries(value.credentialFields).map(
                ([name, { label, description, sensitive }]) => [
                  name,
                  { label, description, sensitive },
                ],
              ),
            )
          : {},
      authorizationUrl: oauth
        ? 'deviceAuthorizationUrl' in oauth
          ? oauth.deviceAuthorizationUrl
          : oauth.authorizationUrl
        : '',
      tokenUrl: oauth?.tokenUrl ?? '',
      scopes: oauth?.scopes ?? [],
      scopeFormat: oauth?.scopeFormat === OAuthScopeFormat.COMMA ? 'comma' : 'space',
      servers: value.servers.map(({ declaration, url, custom, variables }) => ({
        declaration,
        url,
        custom,
        variables,
      })),
    },
  }
}

const NO_BASIC_AUTH: BasicAuthValue = {
  username: { source: 'literal', value: '' },
  password: { source: 'literal', value: '' },
}

/** The username and password, or null when either has a source this Reef does not know. */
function basicAuth(value: BasicAuthentication): BasicAuthValue | null {
  const username = credentialSource(value.username)
  const password = credentialSource(value.password)
  return username && password ? { username, password } : null
}

/** The parameters in name order, or null when a source is one this Reef does not know. */
function queryParameters(
  wire: QueryCredentialsAuthentication['parameters'],
): CredentialBindingValue[] | null {
  const parameters = Object.entries(wire)
    .toSorted(([left], [right]) => left.localeCompare(right))
    .map(([name, value]) => {
      const source = credentialSource(value)
      return source && { name, ...source }
    })
  return parameters.every((parameter) => parameter !== null) ? parameters : null
}

/** One part, or null when its source is one this Reef does not know. */
function credentialSource(value: CredentialSource | undefined): CredentialSourceValue | null {
  const source = value?.source
  if (source?.case === 'field') return { source: 'field', field: source.value }
  if (source?.case === 'literal') return { source: 'literal', value: source.value }
  return null
}

function wireCredentialSource(
  value: CredentialSourceValue,
): MessageInitShape<typeof CredentialSourceSchema> {
  return {
    source:
      value.source === 'field'
        ? { case: 'field', value: value.field }
        : { case: 'literal', value: value.value },
  }
}

function configurationView(
  review: NonNullable<ProviderSetup['configuration']>,
  kind: ProviderSetupInputKind,
): ConfigurationReviewView {
  const value = review.confirmed ?? review.suggested ?? create(ProviderConfigurationSchema)
  const draft = configurationDraft(value)
  if (kind === ProviderSetupInputKind.MCP) {
    const discovered = configurationDraft(
      review.suggested ?? create(ProviderConfigurationSchema),
    ).values
    draft.values = {
      ...draft.values,
      authorizationUrl: discovered.authorizationUrl,
      tokenUrl: discovered.tokenUrl,
      servers: discovered.servers,
    }
  }
  // Joined into the list below, so a large catalog does not rescan the failures
  // once per Tool.
  const unavailableByKey = new Map(
    review.unavailableTools.map(({ key, reasons }) => [key, reasons]),
  )
  return {
    ...draft,
    revision: review.revision.toString(),
    confirmed: review.configurationConfirmed,
    authenticationMethods: review.authenticationMethods.flatMap((method) => {
      const label = knownCredentialMethod(method)
      return label ? [label] : []
    }),
    discoveryAuthorizationRequired: review.discoveryAuthorizationRequired,
    automaticClientAvailable: review.automaticClientAvailable,
    issuer: review.issuer ?? '',
    resource: review.resource ?? '',
    deviceAuthorizationUrl: review.deviceAuthorizationUrl || undefined,
    differences: review.differences.map(({ field: path, saved, document }) => ({
      field: path,
      saved,
      document,
    })),
    interruptsAccess: review.interruptsAccess ?? null,
    updating: review.updating,
    authenticationStatus: review.authenticationStatus,
    documentDisplayName: review.suggested?.displayName ?? '',
    servers: review.servers.map((group) => ({
      declaration: group.declaration,
      tools: group.tools,
      options: group.options.map((option) => ({
        url: option.url,
        variables: option.variables.map(({ name, defaultValue, enumerated, values }) => ({
          name,
          defaultValue,
          enumerated,
          values,
        })),
      })),
    })),
    destinations: review.destinations,
    // An empty MCP catalog is a completed read, even though it cannot be published.
    // Other rejected previews and pending discovery do not establish an empty catalog.
    toolCount: review.discoveryAuthorizationRequired
      ? null
      : review.diagnostic === 'empty_mcp_catalog'
        ? 0
        : review.configurationConfirmed && !review.diagnostic
          ? review.tools.length
          : null,
    // Availability arrives once, keyed by Tool Key; the list joins it in so
    // the tools step reads one record per Tool.
    tools: review.tools.map((tool) => {
      const unavailable = unavailableByKey.get(tool.key)
      return {
        key: tool.key,
        relativePath: tool.relativePath,
        summary: tool.summary,
        group: toolGroup(tool),
        available: !unavailable,
        reasons: unavailable ?? [],
        // Nothing to select before the Provider is published.
        path: '',
      }
    }),
    unavailableTools: review.unavailableTools.map(({ key, reasons }) => ({
      key,
      reasons,
    })),
    diagnostic: review.diagnostic,
    // A source Coral read suggests a configuration, even one it cannot publish.
    readable: review.suggested !== undefined || !review.diagnostic,
    errors: review.errors,
  }
}

function configurationInput(form: FormData): MessageInitShape<typeof ProviderConfigurationSchema> {
  // This is a transport projection. Coral validates all authentication and server rules.
  // Declaration groups can exceed the short scalar-field limit.
  const serialized = form.get('configuration')
  if (typeof serialized !== 'string' || serialized.length > OPENAPI_DOCUMENT_MAX_BYTES)
    throw new Response('Invalid configuration', { status: 400 })
  const value = JSON.parse(serialized) as ConfigurationValues
  if (
    !value ||
    !Array.isArray(value.servers) ||
    (value.authentication === 'header' && !Array.isArray(value.headers)) ||
    (value.authentication === 'query' && !Array.isArray(value.parameters)) ||
    (value.authentication === 'basic' &&
      (typeof value.basic !== 'object' || !value.basic?.username || !value.basic.password)) ||
    (presentsFields(value) &&
      (typeof value.credentialFields !== 'object' ||
        value.credentialFields === null ||
        Array.isArray(value.credentialFields)))
  )
    throw new Response('Invalid configuration', { status: 400 })
  return {
    displayName: value.displayName,
    requestHeaders: requestHeadersInput(value.requestHeaders),
    authentication: authenticationInput(value),
    // Only methods that send fields present them; the others name their own.
    credentialFields: presentsFields(value) ? value.credentialFields : {},
    servers: value.servers.map((server) => ({
      ...server,
      $typeName: 'coral.management.v1.ProviderServerChoice' as const,
    })),
  }
}

function presentsFields(value: ConfigurationValues) {
  return (
    value.authentication === 'header' ||
    value.authentication === 'basic' ||
    value.authentication === 'query'
  )
}

/** Only the chosen method's settings travel; the form keeps every field for switching back. */
function authenticationInput(
  value: ConfigurationValues,
): MessageInitShape<typeof ProviderConfigurationSchema>['authentication'] {
  switch (value.authentication) {
    case '':
      return undefined
    case 'none':
      return { case: 'none', value: {} }
    case 'bearer':
      return { case: 'bearer', value: {} }
    case 'header':
      return {
        case: 'headerCredentials',
        value: {
          headers: Object.fromEntries(
            value.headers.map((binding) => [
              binding.name,
              {
                source:
                  binding.source === 'field'
                    ? { case: 'field', value: { field: binding.field, prefix: binding.prefix } }
                    : { case: 'literal', value: binding.value },
              },
            ]),
          ),
        },
      }
    case 'query':
      return {
        case: 'queryCredentials',
        value: {
          parameters: Object.fromEntries(
            value.parameters.map(({ name, ...source }) => [name, wireCredentialSource(source)]),
          ),
        },
      }
    case 'basic':
      return {
        case: 'basicAuth',
        value: {
          username: wireCredentialSource(value.basic.username),
          password: wireCredentialSource(value.basic.password),
        },
      }
    case 'device':
      return {
        case: 'oauthDeviceCode',
        value: {
          deviceAuthorizationUrl: value.authorizationUrl,
          tokenUrl: value.tokenUrl,
          scopes: value.scopes,
          scopeFormat: scopeFormatInput(value.scopeFormat),
        },
      }
    case 'oauth':
      return {
        case: 'oauthAuthorizationCode',
        value: {
          authorizationUrl: value.authorizationUrl,
          tokenUrl: value.tokenUrl,
          scopes: value.scopes,
          scopeFormat: scopeFormatInput(value.scopeFormat),
        },
      }
    default:
      // The form value is untrusted JSON, so an unknown name is refused at run time;
      // the type check makes a Reef method with no case fail to build.
      value.authentication satisfies never
      throw new Response('Invalid configuration', { status: 400 })
  }
}

function safeHost(url: string) {
  return hostOf(url) ?? 'This server'
}

function hostOf(url: string) {
  try {
    return new URL(url).host
  } catch {
    return undefined
  }
}

export function setupName(parent: string, id: string) {
  return `${parent}/providerSetups/${uuidOr404(id)}`
}

export function stepperPathFor(name: string, account: AccountChoice | null = null) {
  const [scope, workspace, resource, id] = name.split('/')
  if (scope !== 'workspaces' || resource !== 'providerSetups' || !id)
    throw new Response('Not found', { status: 404, headers: privateHeaders })
  const base = `/workspaces/${uuidOr404(workspace ?? '')}/providers/new`
  const attempt = parseAttempt('workspace', new URLSearchParams({ setup: uuidOr404(id) }))
  return attemptPath(base, { ...attempt, account })
}

/**
 * Step 1: pins one exact input without account credentials. Validation answers inline.
 */
/** The pinned Setup, or one inline message for the field the person filled in. */
export type Prepared = { setup: ProviderSetup } | { problem: string }
export async function prepareSetup(
  api: ManagementApi,
  parent: string,
  form: FormData,
  provider = '',
): Promise<Prepared> {
  const source = field(form, 'source')
  if (source === 'current' && provider)
    return {
      setup: await api.setups.createProviderSetup({
        parent,
        provider,
        input: { case: 'current', value: {} },
      }),
    }
  if (source === 'file') {
    const file = form.get('document')
    if (!(file instanceof File) || file.size === 0)
      return {
        problem: `Choose an OpenAPI document up to ${OPENAPI_DOCUMENT_SIZE_LABEL}.`,
      }
    if (file.size > OPENAPI_DOCUMENT_MAX_BYTES)
      return { problem: `Choose a file up to ${OPENAPI_DOCUMENT_SIZE_LABEL}.` }
    // Coral reads the file's bytes, so it can unzip a gzipped document.
    return {
      setup: await api.setups.createProviderSetup({
        parent,
        provider,
        input: { case: 'openapiDocument', value: new Uint8Array(await file.arrayBuffer()) },
        discoveryCredential: '',
      }),
    }
  }
  // A recipe's bundled document comes from Reef's own library, so the browser
  // names only the recipe and never supplies the text.
  if (source === 'recipe') {
    const text = recipeDocument(field(form, 'recipe'))
    if (text === null) return { problem: 'Choose the provider again.' }
    return {
      setup: await api.setups.createProviderSetup({
        parent,
        provider,
        input: { case: 'openapiDocument', value: new TextEncoder().encode(text) },
        discoveryCredential: '',
      }),
    }
  }
  const url = field(form, 'sourceUrl').trim()
  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    return {
      problem: source === 'mcp' ? 'Enter the MCP server URL.' : 'Enter the source URL.',
    }
  }
  // Coral resolves operator policy and guards DNS at connection time. Reef
  // validates the shape, not a second deployment-dependent destination policy.
  if (!['http:', 'https:'].includes(parsed.protocol))
    return { problem: 'Use an http:// or https:// address.' }
  // The person states which kind the URL is, so one call settles it. Coral
  // cannot: a refusal carries no proof of who sent it, and a server holding its
  // catalog behind a token answers a handshake exactly as a protected document
  // host answers a fetch.
  if (source === 'mcp')
    return {
      setup: await api.setups.createProviderSetup({
        parent,
        provider,
        input: { case: 'mcpUrl', value: url },
        requestHeaders: sourceHeaders(form),
      }),
    }
  if (source === 'url')
    return {
      setup: await api.setups.createProviderSetup({
        parent,
        provider,
        input: { case: 'openapiUrl', value: url },
        discoveryCredential: '',
      }),
    }
  throw new Response('Invalid form', { status: 400, headers: privateHeaders })
}

/** Reef's own callback, from its validated origin; never from a browser field. */
export function callbackRedirectUri(request: Request, context: Readonly<RouterContextProvider>) {
  const { auth, upstreamOAuthCallbackUrl } = context.get(runtimeConfigContext)
  if (auth.mode === 'required') return providerSetupRedirectUri(auth)
  const origin = new URL(request.url)
  if (upstreamOAuthCallbackUrl) {
    const callback = new URL(upstreamOAuthCallbackUrl)
    if (
      !isExplicitLoopbackUrl(callback) ||
      callback.pathname !== '/provider-oauth/callback' ||
      callback.search ||
      callback.hash
    )
      throw new Response('Invalid local callback configuration', {
        status: 503,
        headers: privateHeaders,
      })
    return callback.toString()
  }
  if (!isExplicitLoopbackUrl(origin))
    throw new Response('Invalid request origin', {
      status: 403,
      headers: privateHeaders,
    })
  return new URL('/provider-setup/oauth/callback', origin.origin).toString()
}

const CALLBACK_COOKIE = 'coral_provider_setup'
const CALLBACK_MAX_AGE_SECONDS = 15 * 60
export interface CallbackReturn {
  setup: string
  returnTo: string
}
function callbackCookie(workflow: string, context: Readonly<RouterContextProvider>) {
  const auth: AuthConfig = context.get(runtimeConfigContext).auth
  // The name isolates browser return choices, not authority. Coral resolves and
  // authorizes the exact workflow before Reef reads or clears its cookie.
  const key = createHash('sha256').update(workflow).digest('base64url')
  return createCookie(`${CALLBACK_COOKIE}_${key}`, {
    httpOnly: true,
    sameSite: 'lax',
    path: '/provider-setup/oauth/callback',
    maxAge: CALLBACK_MAX_AGE_SECONDS,
    secure: auth.mode === 'required' && new URL(auth.publicUrl).protocol === 'https:',
    secrets: auth.mode === 'required' ? [auth.sessionSecret] : undefined,
  })
}
/** Remembers, for the browser that started consent, which Setup the callback completes. */
export async function commitCallbackReturn(
  value: CallbackReturn,
  context: Readonly<RouterContextProvider>,
) {
  return callbackCookie(value.setup, context).serialize(value)
}
export async function readCallbackReturn(
  request: Request,
  workflow: string,
  context: Readonly<RouterContextProvider>,
): Promise<CallbackReturn | null> {
  const value: unknown = await callbackCookie(workflow, context).parse(
    request.headers.get('cookie'),
  )
  if (
    !value ||
    typeof value !== 'object' ||
    (value as CallbackReturn).setup !== workflow ||
    typeof (value as CallbackReturn).returnTo !== 'string' ||
    !/^workspaces\/[0-9a-f-]{36}\/(providerSetups|accountAuthorizations)\/[0-9a-f-]{36}$/.test(
      (value as CallbackReturn).setup,
    ) ||
    !/^\/[^/\\][^\s]*$/.test((value as CallbackReturn).returnTo)
  )
    return null
  return value as CallbackReturn
}
export async function clearCallbackReturn(
  workflow: string,
  context: Readonly<RouterContextProvider>,
) {
  return callbackCookie(workflow, context).serialize('', { maxAge: 0 })
}

/** Fetch fresh checked issuance; only callback presentation belongs to Reef. */
async function redirectToConsent(
  api: ManagementApi,
  returnTo: CallbackReturn,
  context: Readonly<RouterContextProvider>,
): Promise<Response> {
  const response = await redirectToProviderConsent(api, { kind: 'setup', name: returnTo.setup })
  response.headers.set('Set-Cookie', await commitCallbackReturn(returnTo, context))
  response.headers.set('X-Coral-Authorization-Return-To', returnTo.returnTo)
  return response
}

function optionalField(form: FormData, name: string) {
  return form.has(name) ? field(form, name) : ''
}

type ApplicationFields = NonNullable<ConfigurationReviewView['application']>
type ConfigurationDraft = NonNullable<StepperResult['configurationDraft']>

/** A form error with a host-owned field and message, never submitted values. */
class ConfigurationFormError extends Error {
  constructor(
    readonly path: string,
    message: string,
  ) {
    super(message)
  }
}

/** Read only editable, non-secret fields before any lossy projection or validation. */
function applicationFields(form: FormData): ApplicationFields {
  const mode = field(form, 'application')
  if (mode !== 'automatic' && mode !== 'explicit')
    throw new Response('Invalid form', {
      status: 400,
      headers: privateHeaders,
    })
  const parameters = form.get('authorizationParameters') ?? ''
  if (typeof parameters !== 'string' || parameters.length > 8192)
    throw new Response('Invalid form', {
      status: 400,
      headers: privateHeaders,
    })
  const authentication = optionalField(form, 'clientAuthentication')
  return {
    mode: mode === 'explicit' ? 'manual' : 'automatic',
    clientId: mode === 'explicit' ? field(form, 'clientId') : '',
    authentication:
      authentication === 'basic' || authentication === 'post' ? authentication : 'none',
    pkce: optionalField(form, 'pkce') === 'disabled' ? 'disabled' : 's256',
    parameters,
  }
}

/** Secrets join only the outgoing request; the editable draft never contains them. */
function applicationInput(application: ApplicationFields, secret = '') {
  const parameters = new URLSearchParams(application.parameters)
  const keys = [...parameters.keys()]
  if (new Set(keys.map((key) => key.toLowerCase())).size !== keys.length)
    throw new ConfigurationFormError(
      'application.parameters',
      'Enter each authorization parameter once.',
    )
  if (application.mode === 'automatic')
    return {
      mode: { case: 'automatic' as const, value: {} },
      authorizationParameters: Object.fromEntries(parameters),
    }
  return {
    mode: { case: 'clientId' as const, value: application.clientId },
    authentication: clientAuthenticationFrom(application.authentication, secret),
    pkce: application.pkce === 'disabled' ? OAuthPkcePolicy.DISABLED : OAuthPkcePolicy.S256,
    authorizationParameters: Object.fromEntries(parameters),
  }
}

/** One response shape for returned field errors and thrown configuration rejections. */
function rejectConfiguration(
  draft: ConfigurationDraft,
  errors: Record<string, string>,
  status = 400,
) {
  return data(
    {
      error: null,
      configurationErrors: errors,
      configurationDraft: draft,
      credential: null,
    },
    { status, headers: privateHeaders },
  )
}

function clientAuthenticationFrom(
  authentication: ApplicationFields['authentication'],
  secret: string,
): MessageInitShape<typeof OAuthClientAuthenticationSchema> {
  switch (authentication) {
    case 'basic':
      return { method: { case: 'clientSecretBasic', value: { secret } } }
    case 'post':
      return { method: { case: 'clientSecretPost', value: { secret } } }
    default:
      return { method: { case: 'none', value: {} } }
  }
}

/** Recipe input answers, returned with a rejected draft; they never reach Coral. */
function recipeInputsFrom(form: FormData): Record<string, string> | undefined {
  const serialized = form.get('recipeInputs')
  if (typeof serialized !== 'string' || serialized.length > 16384) return undefined
  try {
    const parsed: unknown = JSON.parse(serialized)
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined
    const answers = Object.entries(parsed).filter(
      (entry): entry is [string, string] =>
        /^[a-z][a-z0-9_]{0,63}$/.test(entry[0]) &&
        typeof entry[1] === 'string' &&
        entry[1].length <= 2048,
    )
    return Object.fromEntries(answers.slice(0, 64))
  } catch {
    return undefined
  }
}

/**
 * Configuration, discovery sign-in, and account choreography, shared by the
 * Add and Update flows. The answer to "Whose account does it use?"
 * travels in the stepper URL; a shared-account sign-in returns to it.
 */
export async function setupAction<Published = never>(
  api: ManagementApi,
  request: Request,
  context: Readonly<RouterContextProvider>,
  form: FormData,
  parent: string,
  basePath: string,
  attempt: PublicationAttempt,
  cancelPath = basePath,
  /** Edit configuration's Save: publish a confirmed configuration that needs nothing more. */
  save?: { account: AccountChoice; publish: (attempt: FrozenAttempt) => Promise<Published> },
) {
  const intent = field(form, 'intent')
  const name = setupName(parent, field(form, 'setup'))
  // Carry local choices through setup; only final publication sends them to Coral.
  const submittedAccount = accountChoice(form.get('account') as string | null)
  const chosen = submittedAccount ?? attempt.account ?? null
  // Only the account question's forms carry an account, so only a sign-in it
  // started answers it. A discovery sign-in leaves the question to the person,
  // even when the page URL (an agent's link, say) already names a route.
  const answering = Boolean(submittedAccount)
  const draft = reviseAttempt(attempt)
  if (draft.kind === 'update') draft.accountGrant = undefined
  // A confirmed configuration relists the tools, so they are accepted again.
  if (intent === 'configure') draft.toolsAccepted = false
  draft.setupId = name.split('/').at(-1)!
  const namespace = form.get('namespace')
  if (draft.kind === 'workspace' && typeof namespace === 'string') draft.namespace = namespace
  const stepper = (
    account: AccountChoice | null,
    returning = draft.kind === 'workspace' && Boolean(draft.returnFromConnect),
  ) =>
    attemptPath(
      basePath,
      draft.kind === 'update'
        ? { ...draft, ...(account ? { account } : {}) }
        : {
            ...draft,
            account,
            returnFromConnect: Boolean(account) && returning,
          },
    )
  try {
    switch (intent) {
      case 'configure': {
        // Preserve the reviewed request exactly. Coral validates read-only MCP facts
        // and the revision; this adapter must not replace them with newer metadata.
        const configuration = configurationInput(form)
        const submitted: ConfigurationDraft = {
          ...configurationDraft(create(ProviderConfigurationSchema, configuration)),
          namespace: draft.kind === 'workspace' ? draft.namespace : undefined,
        }
        const recipeInputs = recipeInputsFrom(form)
        if (recipeInputs) submitted.recipeInputs = recipeInputs
        const oauth =
          configuration.authentication?.case === 'oauthAuthorizationCode' ||
          configuration.authentication?.case === 'oauthDeviceCode'
            ? configuration.authentication.value
            : undefined
        if (oauth) submitted.application = applicationFields(form)
        let setup: ProviderSetup
        try {
          if (oauth && submitted.application)
            oauth.application = applicationInput(
              submitted.application,
              optionalField(form, 'clientSecret'),
            )
          setup = await api.setups.confirmProviderSetupConfiguration({
            name,
            expectedRevision: BigInt(field(form, 'configurationRevision')),
            configuration,
            redirectUri: callbackRedirectUri(request, context),
          })
        } catch (error) {
          if (error instanceof ConfigurationFormError)
            return rejectConfiguration(submitted, {
              [error.path]: error.message,
            })
          if (
            error instanceof ConnectError &&
            [Code.InvalidArgument, Code.FailedPrecondition, Code.Aborted].includes(error.code)
          ) {
            const failure = providerFailure(error, 'Coral could not confirm the configuration. ')
            return rejectConfiguration(
              submitted,
              { configuration: failure.data.error },
              failure.init?.status ?? 400,
            )
          }
          throw error
        }
        const errors = setup.configuration?.errors ?? {}
        if (Object.keys(errors).length) return rejectConfiguration(submitted, errors)
        if (save && draft.kind === 'update' && draft.task === 'configuration') {
          const view = setupView(setup)
          if (savesDirectly(view, save.account))
            return save.publish(freezeAttempt(draft, view, field(form, 'requestId')))
        }
        const returning =
          draft.kind === 'workspace' &&
          Boolean(draft.returnFromConnect) &&
          !isCredentialFree({ allowedRoutes: allowedRoutes(setup) })
        return redirect(stepper(chosen, returning), { headers: privateHeaders })
      }
      case 'account': {
        if (!chosen)
          throw new Response('Invalid form', {
            status: 400,
            headers: privateHeaders,
          })
        const setup = await api.setups.getProviderSetup({ name })
        // Coral reports no routes before the configuration is confirmed, and
        // Publication refuses a misfit route anyway.
        const allowed = allowedRoutes(setup)
        const accountGrant = optionalField(form, 'accountGrant')
        if (
          (allowed.length > 0 && !allowed.includes(chosen)) ||
          (chosen === 'fixed' && !accountGrant) ||
          (accountGrant && (!setup.authorized || accountGrant !== setup.provisionalGrantId)) ||
          (chosen === 'none' && accountGrant)
        )
          throw new Response('Invalid form', {
            status: 400,
            headers: privateHeaders,
          })
        if (draft.kind === 'workspace') draft.accountGrant = accountGrant || undefined
        return redirect(stepper(chosen, false), { headers: privateHeaders })
      }
      case 'secret': {
        const setup = await api.setups.authorizeProviderSetup({
          name,
          configurationRevision: BigInt(field(form, 'configurationRevision')),
          credentialValues: credentialValuesFrom(form),
        })
        // Runtime secret entry explicitly retains this submission's grant for shared or self use.
        // Discovery forms carry no account choice and must not silently retain credentials.
        if (draft.kind === 'workspace')
          draft.accountGrant =
            submittedAccount && submittedAccount !== 'none' && setup.authorized
              ? setup.provisionalGrantId || undefined
              : undefined
        const returning =
          !submittedAccount && draft.kind === 'workspace' && Boolean(draft.returnFromConnect)
        return redirect(stepper(chosen, returning), { headers: privateHeaders })
      }
      case 'oauth-start': {
        if (draft.kind === 'workspace') draft.accountGrant = undefined
        const started = await api.setups.startProviderSetupAuthorization({
          name,
          redirectUri: callbackRedirectUri(request, context),
          configurationRevision: BigInt(field(form, 'configurationRevision')),
          // A confidential application whose secret nobody saved yet takes it here.
          application: applicationInput(
            applicationFields(form),
            optionalField(form, 'clientSecret'),
          ),
          replacementGrant: optionalField(form, 'replacementGrant'),
        })
        if (started.credentialMethod === CredentialMethod.OAUTH_DEVICE_CODE)
          return redirect(stepper(chosen, answering), { headers: privateHeaders })
        return await redirectToConsent(
          api,
          {
            setup: name,
            // Review the resulting grant explicitly before shared or self retention.
            returnTo: stepper(chosen, answering),
          },
          context,
        )
      }
      case 'oauth-resume': {
        if (draft.kind === 'workspace') draft.accountGrant = undefined
        const setup = await api.setups.getProviderSetup({ name })
        if (setup.authorization?.state === SetupAuthorizationState.PENDING)
          return await redirectToConsent(
            api,
            { setup: name, returnTo: stepper(chosen, answering) },
            context,
          )
        return redirect(stepper(chosen, answering), { headers: privateHeaders })
      }
      case 'borrow-account':
        await api.setups.retryProviderSetupDiscovery({
          name,
          useExistingAccount: true,
          configurationRevision: BigInt(field(form, 'configurationRevision')),
        })
        return redirect(stepper(chosen), { headers: privateHeaders })
      case 'oauth-retry':
        await api.setups.retryProviderSetupDiscovery({ name })
        return redirect(stepper(chosen), { headers: privateHeaders })
      case 'cancel': {
        await api.setups.cancelProviderSetup({ name })
        // A discard through a link goes where the link pointed, if that stays on this origin.
        const leaveTo = form.get('leaveTo')
        const target =
          typeof leaveTo === 'string' && leaveTo !== '/' && safeInternalPath(leaveTo) === leaveTo
            ? leaveTo
            : cancelPath
        return redirect(target, { headers: privateHeaders })
      }
      default:
        throw new Response('Invalid action', {
          status: 400,
          headers: privateHeaders,
        })
    }
  } catch (error) {
    if (error instanceof ConfigurationFormError)
      throw new Response(error.message, {
        status: 400,
        headers: privateHeaders,
      })
    const failures: Record<string, string> = {
      configure: 'Coral could not confirm the configuration. ',
      account: 'Coral could not save this choice. ',
      secret: 'Coral could not save the bearer token or API key. ',
      'oauth-start': 'Coral could not start sign-in. ',
      'oauth-resume': 'Coral could not continue sign-in. ',
      'oauth-retry': 'Coral could not list the tools. ',
      'borrow-account':
        'Coral could not use your existing account. Authorize this update separately. ',
      cancel: 'Coral could not start over. ',
    }
    return providerFailure(error, failures[intent])
  }
}

function scopeFormatInput(value: ConfigurationValues['scopeFormat']) {
  if (value === undefined || value === 'space') return OAuthScopeFormat.SPACE
  if (value === 'comma') return OAuthScopeFormat.COMMA
  throw new Response('Invalid scope format', { status: 400 })
}

function requestHeadersInput(
  headers: ConfigurationValues['requestHeaders'],
): Record<string, string> {
  if (headers === undefined) return {}
  if (!Array.isArray(headers)) throw new Response('Invalid request headers', { status: 400 })
  // A row opened and never filled in is no header at all.
  const filled = headers.filter(
    (header) =>
      !header ||
      typeof header.name !== 'string' ||
      typeof header.value !== 'string' ||
      header.name.trim() !== '' ||
      header.value.trim() !== '',
  )
  const names = new Set<string>()
  for (const header of filled) {
    if (
      !header ||
      typeof header.name !== 'string' ||
      typeof header.value !== 'string' ||
      names.has(header.name.toLowerCase())
    )
      throw new Response('Invalid request headers', { status: 400 })
    names.add(header.name.toLowerCase())
  }
  return Object.fromEntries(filled.map(({ name, value }) => [name, value]))
}

function sourceHeaders(form: FormData) {
  try {
    return requestHeadersInput(JSON.parse(String(form.get('requestHeaders') ?? '[]')))
  } catch {
    throw new Response('Invalid request headers', { status: 400 })
  }
}

import type { CredentialMethod, SetupView, StepperResult } from '@/views/providers/types'

export type AccountChoice = 'none' | 'fixed' | 'caller'
/** Navigation context a launcher hands the page; never publication content. */
type InputPresentation = {
  /** A recipe's name, offered as the provider's name until Configuration confirms one. */
  suggestedName?: string
  /** The catalog recipe chosen, whose settings fill in the first configuration. */
  recipe?: string
}
type Draft = InputPresentation & {
  state: 'draft'
  setupId: string | null
  configure: boolean
  /** The listed operations have been accepted; reopening configuration relists them. */
  operationsAccepted: boolean
}
type Frozen = InputPresentation & {
  state: 'frozen'
  setupId: string
  requestId: string
  /** Creation uses this only to invalidate local choices; Update sends it to Coral. */
  configurationRevision: string
}
type WorkspaceChoices = {
  kind: 'workspace'
  namespace: string
  account: AccountChoice | null
  /**
   * The person clicked Connect on the account question and left to sign in to
   * the service; this page is their return. Only `accountStep` decides on it.
   */
  returnFromConnect?: boolean
  /** Exact Setup grant explicitly reviewed for shared or self use. */
  accountGrant?: string
}
export type UpdateTask = 'refresh' | 'configuration' | 'accounts'
type UpdateChoices = {
  kind: 'update'
  /** Presentation only; every task uses the same atomic publication. */
  task?: UpdateTask
  access?: boolean
  review?: boolean
  etag: string
  allowIncompatibleConfiguration: boolean
  account?: AccountChoice
  accountGrant?: string
}
export type CreationAttempt = (Draft | Frozen) & WorkspaceChoices
export type UpdateAttempt = (Draft | Frozen) & UpdateChoices
export type PublicationAttempt = CreationAttempt | UpdateAttempt
export type FrozenAttempt = PublicationAttempt & Frozen
export type AttemptKind = PublicationAttempt['kind']

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const REVISION = /^(0|[1-9][0-9]*)$/
export class InvalidAttempt extends Error {
  constructor() {
    super('Invalid publication attempt')
  }
}

export function accountChoice(value: string | null): AccountChoice | null {
  return value === 'none' || value === 'fixed' || value === 'caller' ? value : null
}

/** URL and POST fields have the same allowlisted, secret-free encoding. */
export function parseAttempt(kind: 'update', params: URLSearchParams, etag?: string): UpdateAttempt
export function parseAttempt(kind: 'workspace', params: URLSearchParams): CreationAttempt
export function parseAttempt(
  kind: AttemptKind,
  params: URLSearchParams,
  etag?: string,
): PublicationAttempt
export function parseAttempt(
  kind: AttemptKind,
  params: URLSearchParams,
  etag = '',
): PublicationAttempt {
  const scalar = (key: string, fallback = '') => {
    const values = params.getAll(key)
    if (values.length > 1 || (values[0]?.length ?? 0) > 4096) throw new InvalidAttempt()
    return values[0] ?? fallback
  }
  const id = (value: string) => {
    if (!UUID.test(value)) throw new InvalidAttempt()
    return value.toLowerCase()
  }
  const setupId = scalar('setup') ? id(scalar('setup')) : null
  const requestId = scalar('requestId')
  const chosen = scalar('account')
  const account = accountChoice(chosen)
  if (chosen && !account) throw new InvalidAttempt()
  const configurationEdit = scalar('configure')
  if (configurationEdit && configurationEdit !== '1') throw new InvalidAttempt()
  const configure = configurationEdit === '1'
  const returnFromConnect = scalar('returnFromConnect')
  if (
    returnFromConnect &&
    (returnFromConnect !== '1' ||
      kind !== 'workspace' ||
      !account ||
      account === 'none' ||
      requestId)
  )
    throw new InvalidAttempt()
  const accountGrant = params.has('accountGrant') ? id(scalar('accountGrant')) : undefined
  if (
    accountGrant &&
    (!['fixed', ...(kind === 'workspace' ? ['caller'] : [])].includes(account ?? '') ||
      !setupId ||
      configure)
  )
    throw new InvalidAttempt()
  const accepted = scalar('operationsAccepted')
  if (accepted && accepted !== '1') throw new InvalidAttempt()
  const operationsAccepted = accepted === '1'
  const suggestedName = scalar('suggestedName')
  if (suggestedName.length > 256 || /[\p{Cc}]/u.test(suggestedName)) throw new InvalidAttempt()
  const recipe = scalar('recipe')
  if (recipe && !/^[a-z][a-z0-9_]{0,63}$/.test(recipe)) throw new InvalidAttempt()
  const input: InputPresentation = {
    ...(suggestedName ? { suggestedName } : {}),
    ...(recipe ? { recipe } : {}),
  }
  let state: Draft | Frozen = { ...input, state: 'draft', setupId, configure, operationsAccepted }
  if (requestId) {
    const revision = scalar('configurationRevision')
    if (
      !setupId ||
      configure ||
      !REVISION.test(revision) ||
      BigInt(revision) > 9223372036854775807n
    )
      throw new InvalidAttempt()
    state = {
      ...input,
      state: 'frozen',
      setupId,
      requestId: id(requestId),
      configurationRevision: revision,
    }
  }
  switch (kind) {
    case 'workspace':
      if (state.state === 'frozen' && (!account || (account === 'fixed' && !accountGrant)))
        throw new InvalidAttempt()
      return {
        ...state,
        kind,
        namespace: scalar('namespace'),
        account,
        accountGrant,
        ...(returnFromConnect ? { returnFromConnect: true } : {}),
      }
    case 'update': {
      const task = scalar('task', 'refresh')
      if (!['refresh', 'configuration', 'accounts'].includes(task)) throw new InvalidAttempt()
      const review = scalar('review')
      const access = scalar('access')
      if ((review && review !== '1') || (access && access !== '1')) throw new InvalidAttempt()
      const consent = scalar('allowIncompatibleConfiguration', 'no')
      if (consent !== 'yes' && consent !== 'no') throw new InvalidAttempt()
      const version = scalar('etag', state.state === 'draft' ? etag : '')
      if (state.state === 'frozen' && !version) throw new InvalidAttempt()
      return {
        ...state,
        kind,
        etag: version,
        task: task as UpdateTask,
        access: access === '1',
        review: review === '1',
        allowIncompatibleConfiguration: consent === 'yes',
        ...(account ? { account } : {}),
        ...(accountGrant ? { accountGrant } : {}),
      }
    }
  }
}

export function attemptParams(attempt: PublicationAttempt): URLSearchParams {
  const params = new URLSearchParams()
  if (attempt.setupId) params.set('setup', attempt.setupId)
  if (attempt.suggestedName) params.set('suggestedName', attempt.suggestedName)
  if (attempt.recipe) params.set('recipe', attempt.recipe)
  if (attempt.state === 'frozen') {
    params.set('requestId', attempt.requestId)
    params.set('configurationRevision', attempt.configurationRevision)
  } else {
    if (attempt.configure) params.set('configure', '1')
    if (attempt.operationsAccepted) params.set('operationsAccepted', '1')
  }
  switch (attempt.kind) {
    case 'workspace':
      params.set('namespace', attempt.namespace)
      if (attempt.account) params.set('account', attempt.account)
      if (attempt.returnFromConnect && attempt.state === 'draft')
        params.set('returnFromConnect', '1')
      if (attempt.accountGrant) params.set('accountGrant', attempt.accountGrant)
      break
    case 'update':
      if (attempt.task) params.set('task', attempt.task)
      if (attempt.review) params.set('review', '1')
      if (attempt.access) params.set('access', '1')
      if (attempt.account) params.set('account', attempt.account)
      if (attempt.accountGrant) params.set('accountGrant', attempt.accountGrant)
      params.set('etag', attempt.etag)
      if (attempt.state === 'frozen')
        params.set(
          'allowIncompatibleConfiguration',
          attempt.allowIncompatibleConfiguration ? 'yes' : 'no',
        )
  }
  return params
}

export function attemptPath(basePath: string, attempt: PublicationAttempt) {
  const params = attemptParams(attempt).toString()
  return params ? `${basePath}?${params}` : basePath
}

/** Edits discard request identity; only account/configuration edits discard the chosen account. */
export function reviseAttempt(
  attempt: CreationAttempt,
  edit?: 'configuration' | 'account',
): CreationAttempt & Draft
export function reviseAttempt(
  attempt: UpdateAttempt,
  edit?: 'configuration' | 'account',
): UpdateAttempt & Draft
export function reviseAttempt(
  attempt: PublicationAttempt,
  edit?: 'configuration' | 'account',
): PublicationAttempt & Draft
export function reviseAttempt(
  attempt: PublicationAttempt,
  edit?: 'configuration' | 'account',
): PublicationAttempt & Draft {
  const state: Draft = {
    state: 'draft',
    suggestedName: attempt.suggestedName,
    recipe: attempt.recipe,
    setupId: attempt.setupId,
    configure: edit === 'configuration',
    // Reopening configuration relists the operations, so the acceptance goes.
    // Every other revision keeps it, and a frozen attempt reached publication,
    // which only a draft that accepted the list can reach.
    operationsAccepted:
      edit !== 'configuration' && (attempt.state === 'frozen' || attempt.operationsAccepted),
  }
  switch (attempt.kind) {
    case 'workspace':
      return {
        ...state,
        kind: attempt.kind,
        namespace: attempt.namespace,
        account: edit === 'account' ? null : attempt.account,
        returnFromConnect:
          edit === 'configuration'
            ? attempt.returnFromConnect || Boolean(attempt.accountGrant)
            : edit
              ? undefined
              : attempt.returnFromConnect,
        accountGrant: edit ? undefined : attempt.accountGrant,
      }
    case 'update':
      return {
        ...state,
        kind: attempt.kind,
        etag: attempt.etag,
        task: attempt.task,
        access: edit ? false : attempt.access,
        review: edit ? false : attempt.review,
        ...(attempt.account ? { account: attempt.account } : {}),
        ...(!edit && attempt.accountGrant ? { accountGrant: attempt.accountGrant } : {}),
        allowIncompatibleConfiguration: false,
      }
  }
}

/**
 * Consumption preserves committed requests; configuration edits discard request identity.
 * A Program's link names only the Setup, so the namespace it asked for comes from the
 * Setup until the person chooses one.
 */
export function reconcileAttempt(attempt: CreationAttempt, setup: SetupView | null): CreationAttempt
export function reconcileAttempt(attempt: UpdateAttempt, setup: SetupView | null): UpdateAttempt
export function reconcileAttempt(
  attempt: PublicationAttempt,
  setup: SetupView | null,
): PublicationAttempt
export function reconcileAttempt(
  attempt: PublicationAttempt,
  setup: SetupView | null,
): PublicationAttempt {
  if (
    attempt.state === 'frozen' &&
    setup?.state !== 'consumed' &&
    attempt.configurationRevision !== setup?.configuration?.revision
  )
    attempt = reviseAttempt(attempt)
  if (attempt.kind === 'workspace' && !attempt.namespace && setup?.request)
    attempt = { ...attempt, namespace: setup.request.namespace }
  return attempt
}

export function freezeAttempt(
  attempt: PublicationAttempt,
  setup: SetupView,
  requestId: string,
): FrozenAttempt {
  if (attempt.state === 'frozen') return attempt
  const frozen = {
    ...attempt,
    state: 'frozen' as const,
    setupId: setup.id,
    requestId,
    configurationRevision: setup.configuration?.revision ?? '0',
  }
  return frozen
}

/** The Setup states that end an attempt. Starting again needs a new Setup. */
export type AttemptEnd = 'expired' | 'canceled' | 'consumed'
const ATTEMPT_ENDS: readonly AttemptEnd[] = ['expired', 'canceled', 'consumed']

/** Whether a person signs in through an OAuth application, in the browser or on a device. */
export function isOAuthMethod(method: CredentialMethod | null) {
  return method === 'oauth' || method === 'device'
}

/** Whether Coral sets up the OAuth application itself. A device sign-in never does. */
export function automaticApplicationAllowed(
  setup: SetupView,
  device = setup.credentialMethod === 'device',
) {
  return !device && setup.configuration?.automaticClientAvailable === true
}

/**
 * Whether Coral says nothing can be connected now for a confirmed configuration
 * that takes a credential: most often because it names no OAuth application
 * and Coral cannot set one up.
 */
export function needsOAuthApplication(setup: SetupView) {
  return setup.allowedRoutes.length > 0 && !isCredentialFree(setup) && !setup.connectAvailable
}

/** Whether Coral says the confirmed configuration takes no credential, so nobody signs in. */
export function isCredentialFree(setup: Pick<SetupView, 'allowedRoutes'>) {
  return setup.allowedRoutes.includes('none')
}

/** The account an attempt has settled, or null while the account question is still open. */
function settledAccount(
  attempt: PublicationAttempt,
  setup: SetupView | null,
  frozen: boolean,
  credentialFree: boolean,
) {
  if (attempt.kind === 'update') return null
  if (frozen) return attempt.account
  if (credentialFree) return 'none'
  if (!attempt.account || attempt.account === 'none') return null
  // A shared account, and an individual one Coral says cannot connect later,
  // are settled only by a sign-in in hand.
  if (attempt.account === 'caller' && setup?.connectLaterAvailable !== false) return attempt.account
  return setup?.authorized && attempt.accountGrant ? attempt.account : null
}

/** One presentation decision from Coral progress plus explicit local choices. No lifecycle validation. */
export function attemptProgress(
  attempt: PublicationAttempt,
  setup: SetupView | null,
  result?: StepperResult,
) {
  const frozen = attempt.state === 'frozen'
  const replaying = frozen && setup?.state === 'consumed'
  const credentialFree = Boolean(setup && isCredentialFree(setup))
  const account = settledAccount(attempt, setup, frozen, credentialFree)
  // A configuration confirmed without an OAuth application has nothing to sign
  // in with. Coral's addProvider now refuses one, but an older Coral server's
  // createProvider can still confirm it. Only Configuration asks for an application.
  const needsApplication = !frozen && Boolean(setup && needsOAuthApplication(setup))
  // Why this Setup can go no further. A replay still reaches its publication.
  const ended: AttemptEnd | null =
    setup && !replaying && ATTEMPT_ENDS.includes(setup.state as AttemptEnd)
      ? (setup.state as AttemptEnd)
      : null
  const stage = !setup
    ? 'source'
    : ended
      ? 'terminal'
      : result?.failed
        ? 'publish'
        : setup.state === 'awaiting_configuration' ||
            (setup.configuration && attempt.state === 'draft' && attempt.configure) ||
            needsApplication
          ? 'configuration'
          : !replaying &&
              (setup.state === 'awaiting_authorization' || setup.state === 'awaiting_discovery')
            ? 'discovery'
            : // Every source shows its operations before anyone is asked about
              // accounts: what the provider can do is the thing worth checking,
              // and on a protected MCP server the branch above signs in first.
              // Only Add requires a separate Operations acceptance step.
              attempt.kind !== 'update' && attempt.state === 'draft' && !attempt.operationsAccepted
              ? 'operations'
              : attempt.kind !== 'update' && (!account || accountStep(attempt, setup).reopens)
                ? 'account'
                : 'publish'
  return {
    stage,
    account,
    ended,
    replaying,
    credentialFree,
    canRevise: !replaying && !result?.failed,
    requiresInterruptionAcceptance: !replaying && setup?.configuration?.interruptsAccess === true,
  }
}

/**
 * One outcome for the account question: which route the provider takes, and
 * what becomes of the sign-in in hand.
 */
export type AccountPlan =
  | 'shared-keep'
  | 'shared-connect'
  | 'own-connect'
  | 'own-keep'
  | 'own-later'

/**
 * The account question's one decision: whether it opens again on the return
 * from Connect, the outcome it opens on, and whether that sign-in adds the
 * provider as soon as it lands.
 *
 * The return reopens it even though the URL names an account, so the sign-in
 * just made can be kept as that account.
 *
 * A sign-in in hand is kept by default, whichever route the attempt names. A
 * route not chosen yet is the one the Program asked for. A Program's own-account
 * Setup opens on Connect: a confidential application takes its secret only at
 * that sign-in, so connecting later could not add the provider. A Program's
 * Setup never adds itself: the person confirms the finished provider with
 * Add provider.
 */
export function accountStep(
  attempt: CreationAttempt,
  setup: SetupView,
): { reopens: boolean; plan: AccountPlan; publishOnReturn: boolean } {
  const held = setup.authorized && Boolean(setup.provisionalGrantId)
  const returning = Boolean(attempt.returnFromConnect)
  const credentialFree = isCredentialFree(setup)
  const route = attempt.account ?? setup.request?.route ?? null
  const plan: AccountPlan =
    route === 'fixed'
      ? held
        ? 'shared-keep'
        : 'shared-connect'
      : route === 'caller'
        ? held
          ? 'own-keep'
          : returning || !attempt.account || !setup.connectLaterAvailable
            ? 'own-connect'
            : 'own-later'
        : held
          ? 'own-keep'
          : 'shared-connect'
  return {
    reopens: returning && !credentialFree,
    plan,
    publishOnReturn: returning && !setup.request,
  }
}

/**
 * Whether Edit configuration's Save can publish a confirmed configuration at
 * once: Coral needs no sign-in, no account choice and no consent for it.
 */
export function savesDirectly(setup: SetupView, currentAccount: AccountChoice) {
  return (
    setup.state === 'ready' &&
    setup.configuration?.interruptsAccess === false &&
    !setup.authorized &&
    !setup.authorization &&
    setup.allowedRoutes.includes(currentAccount)
  )
}

/** Accepts the listed operations, so the attempt moves on to the account question. */
export function acceptOperations(attempt: CreationAttempt): CreationAttempt & Draft {
  return { ...reviseAttempt(attempt), operationsAccepted: true }
}

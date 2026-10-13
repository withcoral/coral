import { Code, ConnectError } from '@connectrpc/connect'
import { data, redirect, type RouterContextProvider } from 'react-router'
import { ProviderPublicationOutcome } from '@/generated/coral/management/v1/provider_pb'
import { field, privateHeaders, type ManagementApi } from '@/lib/management.server'
import { providerPath, providerFailure, routeFromLabel } from '@/lib/providers.server'
import {
  callbackRedirectUri,
  prepareSetup,
  setupAction,
  setupName,
  setupView,
} from './setup.server'
import type { AccountChoice } from '@/views/providers/types'
import {
  InvalidAttempt,
  parseAttempt,
  reconcileAttempt,
  attemptParams,
  attemptPath,
  type AttemptKind,
  type CreationAttempt,
  type UpdateAttempt,
  type PublicationAttempt,
} from './model'

type PublicationTarget =
  | { kind: 'workspace'; parent: string; workspacePath: string }
  | {
      kind: 'update'
      parent: string
      provider: string
      successPath: string
      /** The provider's account setting now, which Save keeps. */
      account: AccountChoice
    }

/** HTTP decoding errors are Reef errors; Coral still owns authorization and validation. */
export function requestAttempt(
  kind: 'update',
  params: URLSearchParams,
  etag?: string,
  status?: number,
): UpdateAttempt
export function requestAttempt(
  kind: 'workspace',
  params: URLSearchParams,
  etag?: string,
  status?: number,
): CreationAttempt
export function requestAttempt(
  kind: AttemptKind,
  params: URLSearchParams,
  etag?: string,
  status?: number,
): PublicationAttempt
export function requestAttempt(
  kind: AttemptKind,
  params: URLSearchParams,
  etag = '',
  status = 404,
) {
  try {
    return parseAttempt(kind, params, etag)
  } catch (error) {
    if (error instanceof InvalidAttempt)
      throw new Response(error.message, { status, headers: privateHeaders })
    throw error
  }
}

type LoadedAttempt<T> = {
  setup: ReturnType<typeof setupView> | null
  attempt: T
}
export function loadAttempt(
  api: ManagementApi,
  parent: string,
  attempt: CreationAttempt,
): Promise<LoadedAttempt<CreationAttempt>>
export function loadAttempt(
  api: ManagementApi,
  parent: string,
  attempt: UpdateAttempt,
): Promise<LoadedAttempt<UpdateAttempt>>
export async function loadAttempt(api: ManagementApi, parent: string, attempt: PublicationAttempt) {
  const setup = attempt.setupId
    ? setupView(
        await api.setups.getProviderSetup({
          name: setupName(parent, attempt.setupId),
        }),
      )
    : null
  return { setup, attempt: reconcileAttempt(attempt, setup) }
}

/** Routes authorize their target; this module owns input preparation and attempt transitions. */
export async function attemptAction(
  api: ManagementApi,
  request: Request,
  context: Readonly<RouterContextProvider>,
  form: FormData,
  target: PublicationTarget & { basePath: string },
) {
  const intent = field(form, 'intent')
  if (intent === 'prepare') {
    try {
      const prepared = await prepareSetup(
        api,
        target.parent,
        form,
        target.kind === 'update' ? target.provider : '',
      )
      if ('problem' in prepared)
        return data({ error: prepared.problem, credential: null }, { headers: privateHeaders })
      const params = new URLSearchParams({
        setup: prepared.setup.name.split('/').at(-1)!,
      })
      if (target.kind === 'update') {
        params.set('task', form.has('task') ? field(form, 'task') : 'refresh')
        // Refresh and account changes retain saved choices, never source defaults.
        // Coral validates them before review or discovery. Only an explicit edit
        // or a rejected saved choice needs the configuration editor.
        if (params.get('task') !== 'configuration') {
          const saved = prepared.setup.configuration?.confirmed
          if (saved) {
            try {
              const confirmed = await api.setups.confirmProviderSetupConfiguration({
                name: prepared.setup.name,
                expectedRevision: prepared.setup.configuration!.revision,
                configuration: saved,
                redirectUri: callbackRedirectUri(request, context),
              })
              if (
                Object.keys(confirmed.configuration?.errors ?? {}).length ||
                confirmed.configuration?.interruptsAccess === true
              )
                params.set('configure', '1')
            } catch {
              // Preparation already succeeded. Keep its identity even if the
              // confirmation response is lost; the loader reauthorizes and reads
              // current state before offering correction, retry or cancellation.
              params.set('configure', '1')
            }
          }
        }
      }
      // The namespace is confirmed in Configuration; a recipe suggests one, a launcher sends none.
      const namespace = form.get('namespace')
      if (target.kind === 'workspace' && typeof namespace === 'string')
        params.set('namespace', namespace)
      const suggestedName = form.get('displayName')
      if (typeof suggestedName === 'string' && suggestedName)
        params.set('suggestedName', suggestedName)
      // A catalog recipe fills in the first configuration; Update never applies one.
      const recipe = form.get('recipe')
      if (target.kind !== 'update' && typeof recipe === 'string' && recipe)
        params.set('recipe', recipe)
      const attempt = parseAttempt(
        target.kind,
        params,
        target.kind === 'update' ? field(form, 'etag') : '',
      )
      return redirect(attemptPath(target.basePath, attempt), {
        headers: privateHeaders,
      })
    } catch (error) {
      return providerFailure(error, 'Coral could not read the document or server. ')
    }
  }
  if (intent === 'publish') return publishAttempt(api, target, form)
  const attempt = requestAttempt(target.kind, new URL(request.url).searchParams, '', 400)
  return setupAction(
    api,
    request,
    context,
    form,
    target.parent,
    target.basePath,
    attempt,
    target.kind === 'update' ? target.successPath : target.basePath,
    target.kind === 'update'
      ? {
          account: target.account,
          // Save sends the request identity of the configuration form, so the
          // same request decoding protects it.
          // Save keeps the account setting: an account the URL still carries
          // from an earlier step never reaches the publication.
          publish: (frozen) =>
            publishFrozen(
              api,
              target,
              requestAttempt(
                target.kind,
                attemptParams(
                  frozen.kind === 'update'
                    ? { ...frozen, account: undefined, accountGrant: undefined }
                    : frozen,
                ),
                '',
                400,
              ),
            ),
        }
      : undefined,
  )
}

/** One terminal submission/replay protocol for creation and Update, using the existing management adapter. */
async function publishAttempt(
  api: ManagementApi,
  target: PublicationTarget & { basePath: string },
  form: FormData,
) {
  const fields = new URLSearchParams()
  for (const [key, value] of form) if (typeof value === 'string') fields.append(key, value)
  return publishFrozen(api, target, requestAttempt(target.kind, fields, '', 400))
}

async function publishFrozen(
  api: ManagementApi,
  target: PublicationTarget & { basePath: string },
  attempt: PublicationAttempt,
) {
  if (attempt.state !== 'frozen')
    throw new Response('Invalid publication attempt', {
      status: 400,
      headers: privateHeaders,
    })
  const setup = setupName(target.parent, attempt.setupId)
  const updating = target.kind === 'update'
  try {
    const common = {
      parent: target.parent,
      setup,
      requestId: attempt.requestId,
      provider: target.kind === 'update' ? target.provider : '',
    }
    const publication = await api.providers.publishProvider(
      attempt.kind === 'update'
        ? {
            ...common,
            etag: attempt.etag,
            allowIncompatibleConfiguration: attempt.allowIncompatibleConfiguration,
            configurationRevision: BigInt(attempt.configurationRevision),
            credentialRoute: attempt.account ? routeFromLabel(attempt.account) : undefined,
            accountGrant: attempt.accountGrant ?? '',
          }
        : {
            ...common,
            namespace: attempt.namespace,
            credentialRoute: routeFromLabel(attempt.account ?? ''),
            accountGrant: attempt.accountGrant ?? '',
          },
    )
    if (publication.outcome === ProviderPublicationOutcome.SUCCEEDED) {
      const destination =
        target.kind === 'update'
          ? target.successPath
          : providerPath(target.workspacePath, publication.provider)
      return redirect(destination, {
        headers: privateHeaders,
      })
    }
    return data(
      {
        error: null,
        credential: null,
        failed:
          publication.diagnostic?.message ||
          `Coral could not ${updating ? 'update' : 'add'} this provider.`,
      },
      { headers: privateHeaders },
    )
  } catch (error) {
    // Action data can leave the browser at the draft URL. Redirect so reload and
    // Retry retain this request even when the Coral response (not Reef HTTP) is lost.
    if (isLostResponse(error))
      return redirect(attemptPath(target.basePath, attempt), {
        headers: privateHeaders,
      })
    return providerFailure(error, `Coral could not ${updating ? 'update' : 'add'} the provider. `)
  }
}

/** A lost response is a transport failure, never a Coral decision. */
function isLostResponse(error: unknown) {
  return (
    !(error instanceof Response) &&
    (!(error instanceof ConnectError) ||
      [
        Code.Unavailable,
        Code.DeadlineExceeded,
        Code.Unknown,
        Code.Internal,
        Code.Canceled,
      ].includes(error.code))
  )
}

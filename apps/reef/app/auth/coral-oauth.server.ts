import { createHash } from 'node:crypto'

import { redirect } from 'react-router'

import { authClientId, authRedirectUri, authResource, isIssuerEndpoint } from './config.server'
import { safeInternalPath } from './safe-path.server'
import {
  clearOAuthTransaction,
  commitOAuthTransaction,
  commitCoralWebSession,
  randomToken,
  readOAuthTransaction,
  type StartedWebSession,
} from './session.server'
import type { RequiredAuthConfig } from './types'

interface AuthorizationServerMetadata {
  authorization_endpoint: string
  coral_web_session_endpoint: string
  issuer: string
  token_endpoint: string
}

interface TokenResponse {
  refresh_token?: unknown
}

interface WebSessionResponse {
  expires_in?: unknown
  session?: unknown
}

interface OAuthErrorResponse {
  error?: unknown
  error_description?: unknown
}

export async function startCoralOAuthLogin(
  request: Request,
  config: RequiredAuthConfig,
): Promise<Response> {
  const metadata = await authorizationServerMetadata(config)
  const url = new URL(request.url)
  const returnTo = safeInternalPath(url.searchParams.get('returnTo'))
  const redirectUri = authRedirectUri(config)
  const clientId = authClientId(config)
  const resource = authResource(config)
  const state = randomToken(32)
  const codeVerifier = randomToken(32)
  const authorizationUrl = new URL(metadata.authorization_endpoint)

  authorizationUrl.searchParams.set('response_type', 'code')
  authorizationUrl.searchParams.set('client_id', clientId)
  authorizationUrl.searchParams.set('redirect_uri', redirectUri)
  authorizationUrl.searchParams.set('resource', resource)
  authorizationUrl.searchParams.set('state', state)
  authorizationUrl.searchParams.set('code_challenge', pkceChallenge(codeVerifier))
  authorizationUrl.searchParams.set('code_challenge_method', 'S256')
  return redirect(authorizationUrl.toString(), {
    headers: {
      'Set-Cookie': await commitOAuthTransaction({ codeVerifier, returnTo, state }, config),
    },
  })
}

export async function completeCoralOAuthLogin(
  request: Request,
  config: RequiredAuthConfig,
): Promise<Response> {
  const url = new URL(request.url)
  const state = url.searchParams.get('state')
  const transaction = await readOAuthTransaction(request, config)
  if (!state || !transaction || state !== transaction.state) {
    throw callbackError('Invalid Coral web auth callback state')
  }

  const providerError = url.searchParams.get('error')
  if (providerError) {
    const description = url.searchParams.get('error_description')
    throw await callbackError(
      description ? `${providerError}: ${description}` : providerError,
      await clearOAuthTransaction(config),
    )
  }

  const code = url.searchParams.get('code')
  if (!code) {
    throw callbackError('Invalid Coral web auth callback code', await clearOAuthTransaction(config))
  }

  const metadata = await authorizationServerMetadata(config)
  let token: TokenResponse
  try {
    token = await exchangeAuthorizationCode(metadata, {
      clientId: authClientId(config),
      code,
      codeVerifier: transaction.codeVerifier,
      redirectUri: authRedirectUri(config),
      resource: authResource(config),
    })
  } catch (error) {
    // Coral currently reports authorize failures through redirects, while its
    // token endpoint returns OAuth JSON directly. Keep the callback route
    // tolerant of both shapes as the server contract evolves.
    if (error instanceof Response) {
      error.headers.append('Set-Cookie', await clearOAuthTransaction(config))
    }
    throw error
  }
  const session = await startWebSession(metadata, token, config)
  const headers = new Headers()
  headers.append('Set-Cookie', await clearOAuthTransaction(config))
  headers.append('Set-Cookie', await commitCoralWebSession(session, config))

  return redirect(transaction.returnTo, { headers })
}

async function authorizationServerMetadata(
  config: RequiredAuthConfig,
): Promise<AuthorizationServerMetadata> {
  const response = await fetch(authorizationServerMetadataUrl(config.issuer), {
    headers: { accept: 'application/json' },
  })
  if (!response.ok) {
    throw new Error(`Coral auth metadata request failed with HTTP ${response.status}`)
  }

  const metadata = (await response.json()) as AuthorizationServerMetadata
  validateAuthorizationServerMetadata(metadata, config.issuer)

  return metadata
}

function validateAuthorizationServerMetadata(
  metadata: AuthorizationServerMetadata,
  issuer: string,
): void {
  if (
    typeof metadata.authorization_endpoint !== 'string' ||
    typeof metadata.issuer !== 'string' ||
    typeof metadata.token_endpoint !== 'string' ||
    typeof metadata.coral_web_session_endpoint !== 'string'
  ) {
    throw new Error('Coral auth metadata is missing required OAuth endpoints')
  }
  if (metadata.issuer !== issuer) {
    throw new Error('Coral auth metadata issuer does not match CORAL_WEB_AUTH_ISSUER')
  }

  for (const name of [
    'authorization_endpoint',
    'token_endpoint',
    'coral_web_session_endpoint',
  ] as const) {
    if (!isIssuerEndpoint(metadata[name], issuer)) throw invalidMetadataEndpoint(name)
  }
}

function invalidMetadataEndpoint(name: string): Error {
  return new Error(
    `Coral auth metadata ${name} must be an absolute same-origin HTTP(S) URL without credentials`,
  )
}

async function exchangeAuthorizationCode(
  metadata: AuthorizationServerMetadata,
  input: {
    clientId: string
    code: string
    codeVerifier: string
    redirectUri: string
    resource: string
  },
): Promise<TokenResponse> {
  const body = new URLSearchParams({
    client_id: input.clientId,
    code: input.code,
    code_verifier: input.codeVerifier,
    grant_type: 'authorization_code',
    coral_audience: 'reef',
    redirect_uri: input.redirectUri,
    resource: input.resource,
  })
  const response = await fetch(metadata.token_endpoint, {
    body,
    headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
    method: 'POST',
    redirect: 'manual',
  })
  if (!response.ok) {
    throw await tokenExchangeError(response)
  }

  return response.json() as Promise<TokenResponse>
}

async function tokenExchangeError(response: Response): Promise<Response> {
  const body = (await response
    .clone()
    .json()
    .catch(() => null)) as OAuthErrorResponse | null
  const error = typeof body?.error === 'string' ? body.error : null
  const description = typeof body?.error_description === 'string' ? body.error_description : null
  const message = error
    ? description
      ? `${error}: ${description}`
      : error
    : `Coral OAuth token exchange failed with HTTP ${response.status}`

  return callbackError(message)
}

/**
 * Hands the refresh token the sign-in returned to Coral, which holds it in a
 * new web session. Reef keeps only the session's handle; the refresh token
 * is spent and never stored.
 */
async function startWebSession(
  metadata: AuthorizationServerMetadata,
  token: TokenResponse,
  config: RequiredAuthConfig,
): Promise<StartedWebSession> {
  if (typeof token.refresh_token !== 'string' || !token.refresh_token) {
    throw callbackError('Sign-in failed: no refresh token. Ask your administrator to update Coral.')
  }
  const response = await fetch(metadata.coral_web_session_endpoint, {
    body: new URLSearchParams({
      client_id: authClientId(config),
      max_age: String(config.sessionMaxAgeSeconds),
      refresh_token: token.refresh_token,
    }),
    headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
    method: 'POST',
    redirect: 'manual',
  })
  if (response.status !== 201) throw await tokenExchangeError(response)
  const body = (await response.json().catch(() => null)) as WebSessionResponse | null
  if (
    typeof body?.session !== 'string' ||
    !body.session ||
    typeof body.expires_in !== 'number' ||
    !Number.isInteger(body.expires_in) ||
    body.expires_in <= 0
  ) {
    throw callbackError('Sign-in failed. Try again.')
  }
  return {
    endpoint: metadata.coral_web_session_endpoint,
    expiresIn: body.expires_in,
    handle: body.session,
  }
}

function pkceChallenge(codeVerifier: string): string {
  return createHash('sha256').update(codeVerifier).digest('base64url')
}

function authorizationServerMetadataUrl(issuer: string): string {
  const url = new URL(issuer)
  const issuerPath = url.pathname.replace(/^\/+/, '')
  url.pathname = `/.well-known/oauth-authorization-server${issuerPath ? `/${issuerPath}` : ''}`
  return url.toString()
}

function callbackError(message: string, clearTransaction?: string): Response {
  const headers = new Headers()
  if (clearTransaction) headers.set('Set-Cookie', clearTransaction)
  return new Response(message, {
    headers,
    status: 400,
  })
}

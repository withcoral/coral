import { createHash } from 'node:crypto'

import { redirect } from 'react-router'

import { authClientId, authRedirectUri, authResource } from './config.server'
import { safeInternalPath } from './safe-path.server'
import {
  clearOAuthTransaction,
  commitOAuthTransaction,
  commitCoralUISession,
  randomToken,
  readOAuthTransaction,
} from './session.server'
import type { AuthSession, RequiredAuthConfig } from './types'

interface AuthorizationServerMetadata {
  authorization_endpoint: string
  issuer: string
  token_endpoint: string
}

interface TokenResponse {
  access_token?: unknown
  expires_in?: unknown
  token_type?: unknown
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
    throw callbackError('Invalid Coral UI auth callback state')
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
    throw callbackError('Invalid Coral UI auth callback code', await clearOAuthTransaction(config))
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
  const session = sessionFromToken(token, config)
  const headers = new Headers()
  headers.append('Set-Cookie', await clearOAuthTransaction(config))
  headers.append('Set-Cookie', await commitCoralUISession(session, config))

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
    typeof metadata.token_endpoint !== 'string'
  ) {
    throw new Error('Coral auth metadata is missing required OAuth endpoints')
  }
  if (metadata.issuer !== issuer) {
    throw new Error('Coral auth metadata issuer does not match CORAL_UI_AUTH_ISSUER')
  }

  const issuerOrigin = new URL(issuer).origin
  validateMetadataEndpoint(metadata.authorization_endpoint, 'authorization_endpoint', issuerOrigin)
  validateMetadataEndpoint(metadata.token_endpoint, 'token_endpoint', issuerOrigin)
}

function validateMetadataEndpoint(endpoint: string, name: string, issuerOrigin: string): void {
  let url: URL
  try {
    url = new URL(endpoint)
  } catch {
    throw invalidMetadataEndpoint(name)
  }
  if (
    (url.protocol !== 'http:' && url.protocol !== 'https:') ||
    url.origin !== issuerOrigin ||
    url.username ||
    url.password
  ) {
    throw invalidMetadataEndpoint(name)
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

function sessionFromToken(token: TokenResponse, config: RequiredAuthConfig): AuthSession {
  if (typeof token.access_token !== 'string' || !token.access_token) {
    throw new Error('Coral OAuth token response did not include access_token')
  }
  if (
    typeof token.expires_in !== 'number' ||
    !Number.isFinite(token.expires_in) ||
    !Number.isInteger(token.expires_in) ||
    token.expires_in <= 0
  ) {
    throw new Error('Coral OAuth token response did not include a positive expires_in')
  }

  return {
    accessToken: token.access_token,
    expiresAt: unixTimestamp() + Math.min(token.expires_in, config.sessionMaxAgeSeconds),
    tokenType:
      typeof token.token_type === 'string' && token.token_type ? token.token_type : 'Bearer',
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

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}

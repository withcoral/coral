import { createHash } from 'node:crypto'

import { redirect } from 'react-router'

import {
  clearOAuthTransaction,
  commitOAuthTransaction,
  commitReefSession,
  randomToken,
  readOAuthTransaction,
} from './session.server'
import type { AuthSession, RequiredAuthConfig } from './types'

const DEFAULT_SCOPE = 'coral:mcp'

interface AuthorizationServerMetadata {
  authorization_endpoint: string
  issuer: string
  registration_endpoint?: string
  resource?: string
  scopes_supported?: string[]
  token_endpoint: string
}

interface TokenResponse {
  access_token?: unknown
  expires_in?: unknown
  token_type?: unknown
}

export async function startCoralOAuthLogin(
  request: Request,
  config: RequiredAuthConfig,
): Promise<Response> {
  const metadata = await authorizationServerMetadata(config)
  const url = new URL(request.url)
  const returnTo = safeReturnTo(url.searchParams.get('returnTo'))
  const redirectUri = callbackUrl(request, config)
  const clientId = await oauthClientId(metadata, redirectUri, config)
  const state = randomToken(32)
  const codeVerifier = randomToken(32)
  const authorizationUrl = new URL(metadata.authorization_endpoint)

  authorizationUrl.searchParams.set('response_type', 'code')
  authorizationUrl.searchParams.set('client_id', clientId)
  authorizationUrl.searchParams.set('redirect_uri', redirectUri)
  authorizationUrl.searchParams.set('scope', oauthScope(metadata, config))
  authorizationUrl.searchParams.set('state', state)
  authorizationUrl.searchParams.set('code_challenge', pkceChallenge(codeVerifier))
  authorizationUrl.searchParams.set('code_challenge_method', 'S256')
  if (metadata.resource) authorizationUrl.searchParams.set('resource', metadata.resource)

  return redirect(authorizationUrl.toString(), {
    headers: {
      'Set-Cookie': await commitOAuthTransaction(
        { clientId, codeVerifier, returnTo, state },
        config,
      ),
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
    throw callbackError('Invalid Reef auth callback state')
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
    throw callbackError('Invalid Reef auth callback code', await clearOAuthTransaction(config))
  }

  const metadata = await authorizationServerMetadata(config)
  const token = await exchangeAuthorizationCode(metadata, {
    clientId: transaction.clientId,
    code,
    codeVerifier: transaction.codeVerifier,
    redirectUri: callbackUrl(request, config),
  })
  const session = sessionFromToken(token, config)
  const headers = new Headers()
  headers.append('Set-Cookie', await clearOAuthTransaction(config))
  headers.append('Set-Cookie', await commitReefSession(session, config))

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
  if (
    typeof metadata.authorization_endpoint !== 'string' ||
    typeof metadata.issuer !== 'string' ||
    typeof metadata.token_endpoint !== 'string'
  ) {
    throw new Error('Coral auth metadata is missing required OAuth endpoints')
  }
  if (metadata.issuer !== config.issuer) {
    throw new Error('Coral auth metadata issuer does not match REEF_AUTH_ISSUER')
  }

  return metadata
}

async function oauthClientId(
  metadata: AuthorizationServerMetadata,
  redirectUri: string,
  config: RequiredAuthConfig,
): Promise<string> {
  if (config.clientId) return config.clientId
  if (!metadata.registration_endpoint) {
    throw new Error('Coral auth metadata did not include registration_endpoint')
  }

  const response = await fetch(metadata.registration_endpoint, {
    body: JSON.stringify({
      client_name: 'Coral Reef',
      grant_types: ['authorization_code'],
      redirect_uris: [redirectUri],
      response_types: ['code'],
      scope: oauthScope(metadata, config),
      token_endpoint_auth_method: 'none',
    }),
    headers: { accept: 'application/json', 'content-type': 'application/json' },
    method: 'POST',
  })
  if (!response.ok) {
    throw new Error(`Coral OAuth registration failed with HTTP ${response.status}`)
  }
  const registration = (await response.json()) as { client_id?: unknown }
  if (typeof registration.client_id !== 'string' || !registration.client_id) {
    throw new Error('Coral OAuth registration response did not include client_id')
  }

  return registration.client_id
}

async function exchangeAuthorizationCode(
  metadata: AuthorizationServerMetadata,
  input: { clientId: string; code: string; codeVerifier: string; redirectUri: string },
): Promise<TokenResponse> {
  const body = new URLSearchParams({
    client_id: input.clientId,
    code: input.code,
    code_verifier: input.codeVerifier,
    grant_type: 'authorization_code',
    redirect_uri: input.redirectUri,
  })
  const response = await fetch(metadata.token_endpoint, {
    body,
    headers: { accept: 'application/json', 'content-type': 'application/x-www-form-urlencoded' },
    method: 'POST',
  })
  if (!response.ok) {
    throw new Error(`Coral OAuth token exchange failed with HTTP ${response.status}`)
  }

  return response.json() as Promise<TokenResponse>
}

function sessionFromToken(token: TokenResponse, config: RequiredAuthConfig): AuthSession {
  if (typeof token.access_token !== 'string' || !token.access_token) {
    throw new Error('Coral OAuth token response did not include access_token')
  }
  if (
    typeof token.expires_in !== 'number' ||
    !Number.isFinite(token.expires_in) ||
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

function callbackUrl(request: Request, config: RequiredAuthConfig): string {
  if (config.redirectUri) return config.redirectUri
  const url = new URL(request.url)
  url.pathname = '/auth/callback'
  url.search = ''
  url.hash = ''
  return url.toString()
}

function oauthScope(metadata: AuthorizationServerMetadata, config: RequiredAuthConfig): string {
  return config.scope ?? metadata.scopes_supported?.[0] ?? DEFAULT_SCOPE
}

function safeReturnTo(value: string | null): string {
  if (!value) return '/'
  try {
    const parsed = new URL(value, 'http://reef.local')
    if (parsed.origin !== 'http://reef.local') return '/'
    return `${parsed.pathname}${parsed.search}${parsed.hash}`
  } catch {
    return '/'
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

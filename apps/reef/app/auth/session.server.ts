import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto'

import { createCookie } from 'react-router'

import { authCookieSecure } from './config.server'
import type { AuthSession, RequiredAuthConfig } from './types'

const OAUTH_COOKIE_NAME = 'reef_oauth'
const OAUTH_MAX_AGE_SECONDS = 10 * 60
const CLOCK_SKEW_SECONDS = 30

export interface OAuthTransaction {
  codeVerifier: string
  returnTo: string
  state: string
}

export async function commitOAuthTransaction(
  transaction: OAuthTransaction,
  config: RequiredAuthConfig,
): Promise<string> {
  return oauthCookie(config).serialize(transaction)
}

export async function readOAuthTransaction(
  request: Request,
  config: RequiredAuthConfig,
): Promise<OAuthTransaction | null> {
  const value = await oauthCookie(config).parse(request.headers.get('cookie'))
  return isOAuthTransaction(value) ? value : null
}

export async function clearOAuthTransaction(config: RequiredAuthConfig): Promise<string> {
  return oauthCookie(config).serialize('', { maxAge: 0 })
}

export async function commitReefSession(
  session: AuthSession,
  config: RequiredAuthConfig,
): Promise<string> {
  return sessionCookie(config).serialize(encryptSession(session, config.sessionSecret))
}

export async function readReefSession(
  request: Request,
  config: RequiredAuthConfig,
): Promise<AuthSession | null> {
  const encrypted = await sessionCookie(config).parse(request.headers.get('cookie'))
  if (typeof encrypted !== 'string' || !encrypted) return null
  const session = decryptSession(encrypted, config.sessionSecret)
  if (!session || session.expiresAt <= unixTimestamp() + CLOCK_SKEW_SECONDS) return null

  return session
}

export async function clearReefSession(config: RequiredAuthConfig): Promise<string> {
  return sessionCookie(config).serialize('', { maxAge: 0 })
}

export function randomToken(byteCount: number): string {
  return randomBytes(byteCount).toString('base64url')
}

function oauthCookie(config: RequiredAuthConfig) {
  return createCookie(OAUTH_COOKIE_NAME, {
    httpOnly: true,
    maxAge: OAUTH_MAX_AGE_SECONDS,
    path: '/',
    sameSite: 'lax',
    secrets: [config.sessionSecret],
    secure: authCookieSecure(config),
  })
}

function sessionCookie(config: RequiredAuthConfig) {
  return createCookie(config.cookieName, {
    httpOnly: true,
    maxAge: config.sessionMaxAgeSeconds,
    path: '/',
    sameSite: 'lax',
    secrets: [config.sessionSecret],
    secure: authCookieSecure(config),
  })
}

function encryptSession(session: AuthSession, secret: string): string {
  const iv = randomBytes(12)
  const cipher = createCipheriv('aes-256-gcm', encryptionKey(secret), iv)
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(session), 'utf8'), cipher.final()])
  const tag = cipher.getAuthTag()

  return [iv, tag, ciphertext].map((part) => part.toString('base64url')).join('.')
}

function decryptSession(value: string, secret: string): AuthSession | null {
  const [ivPart, tagPart, ciphertextPart] = value.split('.')
  if (!ivPart || !tagPart || !ciphertextPart) return null

  try {
    const decipher = createDecipheriv(
      'aes-256-gcm',
      encryptionKey(secret),
      Buffer.from(ivPart, 'base64url'),
    )
    decipher.setAuthTag(Buffer.from(tagPart, 'base64url'))
    const plaintext = Buffer.concat([
      decipher.update(Buffer.from(ciphertextPart, 'base64url')),
      decipher.final(),
    ]).toString('utf8')
    const session = JSON.parse(plaintext) as unknown

    return isAuthSession(session) ? session : null
  } catch {
    return null
  }
}

function encryptionKey(secret: string): Buffer {
  return createHash('sha256').update(secret).digest()
}

function isOAuthTransaction(value: unknown): value is OAuthTransaction {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<OAuthTransaction>

  return (
    typeof candidate.codeVerifier === 'string' &&
    typeof candidate.returnTo === 'string' &&
    typeof candidate.state === 'string'
  )
}

function isAuthSession(value: unknown): value is AuthSession {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<AuthSession>

  return (
    typeof candidate.accessToken === 'string' &&
    candidate.accessToken.length > 0 &&
    typeof candidate.expiresAt === 'number' &&
    Number.isFinite(candidate.expiresAt) &&
    typeof candidate.tokenType === 'string' &&
    candidate.tokenType.length > 0
  )
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}

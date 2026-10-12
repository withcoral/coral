import { createHash, timingSafeEqual } from 'node:crypto'

import { createCookie } from 'react-router'

import { authCookieSecure } from './config.server'
import { randomToken } from './session.server'
import type { AuthSession, RequiredAuthConfig } from './types'

const CLOCK_SKEW_SECONDS = 30

interface CsrfCookieValue {
  createdAt: number
  sessionBinding: string
  token: string
}

interface CsrfTokenResult {
  setCookie: string | null
  token: string
}

export async function csrfTokenForRequest(
  request: Request,
  config: RequiredAuthConfig,
  session: AuthSession,
): Promise<CsrfTokenResult> {
  const existing = await readCsrfCookie(request, config)
  const binding = sessionBinding(session)
  if (existing && isFresh(existing, config) && safeEqual(existing.sessionBinding, binding)) {
    return { setCookie: null, token: existing.token }
  }

  const value = {
    createdAt: unixTimestamp(),
    sessionBinding: binding,
    token: randomToken(32),
  } satisfies CsrfCookieValue
  return {
    setCookie: await csrfCookie(config).serialize(value),
    token: value.token,
  }
}

export async function validateCsrfToken(
  request: Request,
  config: RequiredAuthConfig,
  session: AuthSession,
): Promise<boolean> {
  const cookie = await readCsrfCookie(request, config)
  if (
    !cookie ||
    !isFresh(cookie, config) ||
    !safeEqual(cookie.sessionBinding, sessionBinding(session))
  ) {
    return false
  }

  let formData: FormData
  try {
    formData = await request.clone().formData()
  } catch {
    return false
  }

  const submitted = formData.get('csrf')
  return typeof submitted === 'string' && safeEqual(submitted, cookie.token)
}

export async function clearCsrfToken(config: RequiredAuthConfig): Promise<string> {
  return csrfCookie(config).serialize('', { maxAge: 0 })
}

function csrfCookie(config: RequiredAuthConfig) {
  return createCookie(`${config.cookieName}_csrf`, {
    httpOnly: true,
    maxAge: config.sessionMaxAgeSeconds,
    path: '/',
    sameSite: 'lax',
    secrets: [config.sessionSecret],
    secure: authCookieSecure(config),
  })
}

async function readCsrfCookie(
  request: Request,
  config: RequiredAuthConfig,
): Promise<CsrfCookieValue | null> {
  const value = await csrfCookie(config).parse(request.headers.get('cookie'))
  return isCsrfCookieValue(value) ? value : null
}

function isCsrfCookieValue(value: unknown): value is CsrfCookieValue {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<CsrfCookieValue>
  return (
    typeof candidate.createdAt === 'number' &&
    Number.isFinite(candidate.createdAt) &&
    typeof candidate.sessionBinding === 'string' &&
    candidate.sessionBinding.length > 0 &&
    typeof candidate.token === 'string' &&
    candidate.token.length > 0
  )
}

function isFresh(value: CsrfCookieValue, config: RequiredAuthConfig): boolean {
  const now = unixTimestamp()
  return (
    value.createdAt <= now + CLOCK_SKEW_SECONDS &&
    now - value.createdAt <= config.sessionMaxAgeSeconds
  )
}

function sessionBinding(session: AuthSession): string {
  return createHash('sha256').update(session.accessToken).digest('hex')
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left)
  const rightBuffer = Buffer.from(right)
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer)
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}

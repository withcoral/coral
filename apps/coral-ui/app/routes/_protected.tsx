import { Outlet, redirect } from 'react-router'

import type { Route } from './+types/_protected'

import { coralUIAuthConfig } from '@/auth/config.server'
import { csrfTokenForRequest } from '@/auth/csrf.server'
import { AUTH_STREAM_REQUEST_HEADER, EXPIRED_SESSION_LOGIN_HEADER } from '@/auth/response'
import {
  EXPIRED_SESSION_RESPONSE_HEADER,
  loginLocationForRequest,
  markAuthResponsePrivate,
} from '@/auth/response.server'
import { requestAuthContext } from '@/auth/server-context'
import { clearCoralUISession, readCoralUISession } from '@/auth/session.server'
import type { RequiredAuthConfig } from '@/auth/types'
import { routePath } from '@/routing/routemap'

export const middleware: Route.MiddlewareFunction[] = [
  async ({ context, request }, next) => {
    const config = coralUIAuthConfig()

    if (config.mode === 'disabled') {
      context.set(requestAuthContext, { accessToken: null, mode: 'disabled' })
      return next()
    }

    const session = await readCoralUISession(request, config)
    if (!session) {
      throw authStreamExpiredResponse(request, await loginRedirect(request, config))
    }
    const csrf = await csrfTokenForRequest(request, config, session)
    context.set(requestAuthContext, {
      accessToken: session.accessToken,
      csrfToken: csrf.token,
      mode: 'required',
      session,
    })

    try {
      const response = await next()
      return await finalizeProtectedResponse(request, response, config, csrf.setCookie)
    } catch (error) {
      if (error instanceof Response) {
        throw await finalizeProtectedResponse(request, error, config, csrf.setCookie)
      }
      throw error
    }
  },
]

export default function Protected() {
  return <Outlet />
}

async function loginRedirect(request: Request, config: RequiredAuthConfig): Promise<Response> {
  const headers = new Headers()
  if (hasSessionCookie(request, config.cookieName)) {
    headers.append('Set-Cookie', await clearCoralUISession(config))
  }

  const response = redirect(loginLocationForRequest(request), { headers })
  return markAuthResponsePrivate(response)
}

function hasSessionCookie(request: Request, name: string): boolean {
  const cookie = request.headers.get('cookie')
  return cookie?.split(';').some((part) => part.trim().startsWith(`${name}=`)) ?? false
}

function appendSetCookie(response: Response, value: string | null): void {
  if (value) response.headers.append('Set-Cookie', value)
}

async function finalizeProtectedResponse(
  request: Request,
  response: Response,
  config: RequiredAuthConfig,
  csrfSetCookie: string | null,
): Promise<Response> {
  if (response.headers.get(EXPIRED_SESSION_RESPONSE_HEADER) === '1') {
    response.headers.delete(EXPIRED_SESSION_RESPONSE_HEADER)
    const expired = authStreamExpiredResponse(request, response)
    appendSetCookie(expired, await clearCoralUISession(config))
    return markAuthResponsePrivate(expired)
  }

  appendSetCookie(response, csrfSetCookie)
  return markAuthResponsePrivate(response)
}

function authStreamExpiredResponse(request: Request, redirectResponse: Response): Response {
  if (request.headers.get(AUTH_STREAM_REQUEST_HEADER) !== '1') return redirectResponse

  const headers = new Headers(redirectResponse.headers)
  const loginLocation = headers.get('Location') ?? routePath('login')
  headers.delete('Location')
  headers.set(EXPIRED_SESSION_LOGIN_HEADER, loginLocation)
  return new Response(null, { headers, status: 401 })
}

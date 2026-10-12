import { Outlet, redirect } from 'react-router'

import type { Route } from './+types/_protected'

import { clearCsrfToken, csrfTokenForRequest } from '@/auth/csrf.server'
import { loginLocationForUrl, markAuthResponsePrivate } from '@/auth/response.server'
import { requestAuthContext } from '@/auth/server-context'
import { clearCoralWebSession, readCoralWebSession } from '@/auth/session.server'
import type { RequiredAuthConfig } from '@/auth/types'
import { runtimeConfigContext } from '@/lib/runtime-config.server'

export const middleware: Route.MiddlewareFunction[] = [
  async ({ context, request, url }, next) => {
    const config = context.get(runtimeConfigContext).auth

    if (config.mode === 'disabled') {
      context.set(requestAuthContext, { accessToken: null, mode: 'disabled' })
      try {
        return finalizeProtectedResponse(await next(), null)
      } catch (error) {
        if (error instanceof Response) throw finalizeProtectedResponse(error, null)
        throw error
      }
    }

    const session = await readCoralWebSession(request, config)
    if (!session) {
      throw await loginRedirect(request, url, config)
    }
    const csrf = await csrfTokenForRequest(request, config, session)
    context.set(requestAuthContext, {
      accessToken: session.accessToken,
      csrfToken: csrf.token,
      mode: 'required',
      session,
    })

    let response: Response
    try {
      response = await next()
    } catch (error) {
      if (!(error instanceof Response)) throw error
      if (error.status === 401) throw await loginRedirect(request, url, config, true)
      throw finalizeProtectedResponse(error, csrf.setCookie)
    }
    // A valid cookie can outlive Coral's token (revocation or a database reset).
    // Handle returned action failures as well as rendered loader errors here.
    if (response.status === 401) throw await loginRedirect(request, url, config, true)
    return finalizeProtectedResponse(response, csrf.setCookie)
  },
]

export default function Protected() {
  return <Outlet />
}

async function loginRedirect(
  request: Request,
  url: URL,
  config: RequiredAuthConfig,
  sessionExpired = false,
): Promise<Response> {
  const headers = new Headers()
  if (hasSessionCookie(request, config.cookieName)) {
    headers.append('Set-Cookie', await clearCoralWebSession(config))
  }

  if (sessionExpired) headers.append('Set-Cookie', await clearCsrfToken(config))

  // Pause before restarting OAuth: a persistently rejected account must not loop.
  // The raw request may target .data; OAuth must resume at the document URL.
  const location = `${loginLocationForUrl(url)}${sessionExpired ? '&sessionExpired=1' : ''}`
  const response = redirect(location, { status: request.method === 'POST' ? 303 : 302, headers })
  return markAuthResponsePrivate(response)
}

function hasSessionCookie(request: Request, name: string): boolean {
  const cookie = request.headers.get('cookie')
  return cookie?.split(';').some((part) => part.trim().startsWith(`${name}=`)) ?? false
}

function finalizeProtectedResponse(response: Response, csrfSetCookie: string | null): Response {
  if (csrfSetCookie) response.headers.append('Set-Cookie', csrfSetCookie)
  return markAuthResponsePrivate(response)
}

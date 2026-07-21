import { Outlet, redirect } from 'react-router'

import type { Route } from './+types/_protected'

import { reefAuthConfig } from '@/auth/config.server'
import { csrfTokenForRequest } from '@/auth/csrf.server'
import { markAuthResponsePrivate } from '@/auth/response.server'
import { requestAuthContext } from '@/auth/server-context'
import { clearReefSession, readReefSession } from '@/auth/session.server'
import type { RequiredAuthConfig } from '@/auth/types'

export const middleware: Route.MiddlewareFunction[] = [
  async ({ context, request }, next) => {
    const config = reefAuthConfig()

    if (config.mode === 'disabled') {
      context.set(requestAuthContext, { accessToken: null, mode: 'disabled' })
      return next()
    }

    const session = await readReefSession(request, config)
    if (!session) throw await loginRedirect(request, config)
    const csrf = await csrfTokenForRequest(request, config, session)
    context.set(requestAuthContext, {
      accessToken: session.accessToken,
      csrfToken: csrf.token,
      mode: 'required',
      session,
    })

    try {
      const response = await next()
      appendSetCookie(response, csrf.setCookie)
      return markAuthResponsePrivate(response)
    } catch (error) {
      if (error instanceof Response) {
        appendSetCookie(error, csrf.setCookie)
        markAuthResponsePrivate(error)
      }
      throw error
    }
  },
]

export default function Protected() {
  return <Outlet />
}

async function loginRedirect(request: Request, config: RequiredAuthConfig): Promise<Response> {
  const url = new URL(request.url)
  const returnTo = `${url.pathname}${url.search}`
  const headers = new Headers()
  if (hasSessionCookie(request, config.cookieName)) {
    headers.append('Set-Cookie', await clearReefSession(config))
  }

  const response = redirect(`/login?returnTo=${encodeURIComponent(returnTo)}`, { headers })
  return markAuthResponsePrivate(response)
}

function hasSessionCookie(request: Request, name: string): boolean {
  const cookie = request.headers.get('cookie')
  return cookie?.split(';').some((part) => part.trim().startsWith(`${name}=`)) ?? false
}

function appendSetCookie(response: Response, value: string | null): void {
  if (value) response.headers.append('Set-Cookie', value)
}

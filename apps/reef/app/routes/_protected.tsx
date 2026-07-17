import { Outlet, redirect } from 'react-router'

import type { Route } from './+types/_protected'

import { reefAuthConfig } from '@/auth/config.server'
import { requestAuthContext } from '@/auth/server-context'
import { clearReefSession, readReefSession } from '@/auth/session.server'
import type { RequiredAuthConfig } from '@/auth/types'

const PRIVATE_RESPONSE_HEADERS = {
  'Cache-Control': 'private, no-store',
  Vary: 'Cookie',
} as const

export const middleware: Route.MiddlewareFunction[] = [
  async ({ context, request }, next) => {
    const config = reefAuthConfig()

    if (config.mode === 'disabled') {
      context.set(requestAuthContext, { accessToken: null, mode: 'disabled' })
      return next()
    }

    const session = await readReefSession(request, config)
    if (!session) throw await loginRedirect(request, config)
    context.set(requestAuthContext, {
      accessToken: session.accessToken,
      mode: 'required',
      session,
    })

    try {
      const response = await next()
      applyPrivateHeaders(response)
      return response
    } catch (error) {
      if (error instanceof Response) applyPrivateHeaders(error)
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
  applyPrivateHeaders(response)
  return response
}

function hasSessionCookie(request: Request, name: string): boolean {
  const cookie = request.headers.get('cookie')
  return cookie?.split(';').some((part) => part.trim().startsWith(`${name}=`)) ?? false
}

function applyPrivateHeaders(response: Response): void {
  response.headers.set('Cache-Control', PRIVATE_RESPONSE_HEADERS['Cache-Control'])
  response.headers.append('Vary', PRIVATE_RESPONSE_HEADERS.Vary)
}

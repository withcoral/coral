import { redirect } from 'react-router'

import type { Route } from './+types/logout'

import { clearCsrfToken, validateCsrfToken } from '@/auth/csrf.server'
import { markAuthResponsePrivate } from '@/auth/response.server'
import {
  clearOAuthTransaction,
  coralWebSessionHandle,
  signOutCoralWebSession,
} from '@/auth/session.server'
import { runtimeConfigContext } from '@/lib/runtime-config.server'
import { routePath } from '@/routing/routemap'

export async function loader() {
  return markAuthResponsePrivate(redirect('/'))
}

export async function action({ context, request }: Route.ActionArgs) {
  if (request.method !== 'POST') {
    throw markAuthResponsePrivate(new Response('Method Not Allowed', { status: 405 }))
  }

  const config = context.get(runtimeConfigContext).auth
  if (config.mode === 'disabled') return markAuthResponsePrivate(redirect('/'))
  // CSRF evidence binds to the session handle, so sign-out needs no answer
  // from Coral to check it.
  const handle = await coralWebSessionHandle(request, config)
  if (!handle || !(await validateCsrfToken(request, config, { handle }))) {
    throw markAuthResponsePrivate(new Response('Invalid CSRF token', { status: 403 }))
  }

  const headers = new Headers()
  headers.append('Set-Cookie', await clearCsrfToken(config))
  headers.append('Set-Cookie', await clearOAuthTransaction(config))
  // Ending the session at Coral ends its grant, so it cannot refresh again.
  // When Coral does not answer, the browser still loses the session, and a
  // pending sign-out cookie lets the next visit finish ending it.
  const signOut = await signOutCoralWebSession(request, config)
  for (const cookie of signOut.setCookies) headers.append('Set-Cookie', cookie)

  // Land on the signed-out screen so sign-in does not start again by itself.
  return markAuthResponsePrivate(redirect(`${routePath('login')}?signedOut=1`, { headers }))
}

export default function Logout() {
  return null
}

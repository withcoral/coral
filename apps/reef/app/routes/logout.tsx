import { redirect } from 'react-router'

import type { Route } from './+types/logout'

import { reefAuthConfig } from '@/auth/config.server'
import { clearCsrfToken, validateCsrfToken } from '@/auth/csrf.server'
import { markAuthResponsePrivate } from '@/auth/response.server'
import { clearOAuthTransaction, clearReefSession } from '@/auth/session.server'

export async function loader() {
  return markAuthResponsePrivate(redirect('/'))
}

export async function action({ request }: Route.ActionArgs) {
  if (request.method !== 'POST') {
    throw markAuthResponsePrivate(new Response('Method Not Allowed', { status: 405 }))
  }

  const config = reefAuthConfig()
  if (config.mode === 'disabled') return markAuthResponsePrivate(redirect('/'))
  if (!(await validateCsrfToken(request, config))) {
    throw markAuthResponsePrivate(new Response('Invalid CSRF token', { status: 403 }))
  }

  const headers = new Headers()
  headers.append('Set-Cookie', await clearCsrfToken(config))
  headers.append('Set-Cookie', await clearOAuthTransaction(config))
  headers.append('Set-Cookie', await clearReefSession(config))

  // Coral Cloud does not currently advertise a provider-neutral browser logout
  // or revocation endpoint. Keep logout local and stop automatic SSO bounce by
  // landing on the signed-out login screen.
  return markAuthResponsePrivate(redirect('/login?signedOut=1', { headers }))
}

export default function Logout() {
  return null
}

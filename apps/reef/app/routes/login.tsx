import { Form, redirect } from 'react-router'

import type { Route } from './+types/login'

import { reefAuthConfig } from '@/auth/config.server'
import { startCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { authPrivateHeaders, markAuthResponsePrivate } from '@/auth/response.server'
import { readReefSession } from '@/auth/session.server'
import { getMeta } from '@/meta'
import * as Button from '@/wax/components/button'
import { Typography } from '@/wax/components/typography'

import * as styles from './login.css'

export function meta(_args: Route.MetaArgs) {
  return getMeta('Sign in')
}

export function headers() {
  return authPrivateHeaders()
}

export async function loader({ request }: Route.LoaderArgs) {
  const config = reefAuthConfig()
  if (config.mode === 'disabled') return redirect('/')

  const session = await readReefSession(request, config)
  if (session) return markAuthResponsePrivate(redirect('/'))

  const url = new URL(request.url)
  if (url.searchParams.has('signedOut')) return null

  return markAuthResponsePrivate(await startCoralOAuthLogin(request, config))
}

export default function Login() {
  return (
    <main className={styles.page}>
      <section className={styles.content} aria-labelledby="login-title">
        <Typography.HeadingLarge as="h1" id="login-title">
          Coral 🪸
        </Typography.HeadingLarge>
        <Typography.Body>You have been signed out.</Typography.Body>
        <Form action="/login" className={styles.actions} method="get">
          <Button.TextButton type="submit">Sign in with Coral Cloud</Button.TextButton>
        </Form>
      </section>
    </main>
  )
}

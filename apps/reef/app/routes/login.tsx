import { Form, redirect } from 'react-router'

import type { Route } from './+types/login'

import { reefAuthConfig } from '@/auth/config.server'
import { startCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { authPrivateHeaders, markAuthResponsePrivate } from '@/auth/response.server'
import { safeInternalPath } from '@/auth/safe-path.server'
import { readReefSession } from '@/auth/session.server'
import { getMeta } from '@/meta'
import { routePath } from '@/routing/routemap'
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
  // The interstitial is the one branch that renders instead of starting a login,
  // so it is also the one that has to carry the destination forward itself. It
  // is sanitized here rather than on submit because it is about to be written
  // into the page and handed back as a query parameter.
  if (url.searchParams.has('signedOut')) {
    return { returnTo: safeInternalPath(url.searchParams.get('returnTo')) }
  }

  return markAuthResponsePrivate(await startCoralOAuthLogin(request, config))
}

export default function Login({ loaderData }: Route.ComponentProps) {
  const returnTo = loaderData?.returnTo
  return (
    <main className={styles.page}>
      <section className={styles.content} aria-labelledby="login-title">
        <Typography.HeadingLarge as="h1" id="login-title">
          Coral 🪸
        </Typography.HeadingLarge>
        <Typography.Body>You have been signed out.</Typography.Body>
        {/* GET, so submitting replaces the query wholesale — `signedOut` drops
            away and the loader starts a real login on the next pass. */}
        <Form action={routePath('login')} className={styles.actions} method="get">
          {returnTo && returnTo !== '/' ? (
            <input type="hidden" name="returnTo" value={returnTo} />
          ) : null}
          <Button.TextButton type="submit">Sign in with Coral Cloud</Button.TextButton>
        </Form>
      </section>
    </main>
  )
}

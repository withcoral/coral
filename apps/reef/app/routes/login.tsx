import { Form, redirect } from 'react-router'

import type { Route } from './+types/login'

import { startCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { authPrivateHeaders, markAuthResponsePrivate } from '@/auth/response.server'
import { safeInternalPath } from '@/auth/safe-path.server'
import { readCoralWebSession } from '@/auth/session.server'
import { runtimeConfigContext } from '@/lib/runtime-config.server'
import { routePath } from '@/routing/routemap'
import { Brand } from '@/wax/components/brand'
import * as Button from '@/wax/components/button'
import * as Card from '@/wax/components/card'
import { PageTitle } from '@/wax/components/page-title'
import { Typography } from '@/wax/components/typography'

import * as styles from './login.css'

export function headers() {
  return authPrivateHeaders()
}

export async function loader({ context, request }: Route.LoaderArgs) {
  const config = context.get(runtimeConfigContext).auth
  if (config.mode === 'disabled') return redirect('/')

  const session = await readCoralWebSession(request, config)
  if (session) return markAuthResponsePrivate(redirect('/'))

  const url = new URL(request.url)
  // The interstitial is the one branch that renders instead of starting a login,
  // so it is also the one that has to carry the destination forward itself. It
  // is sanitized here rather than on submit because it is about to be written
  // into the page and handed back as a query parameter.
  const sessionExpired = url.searchParams.has('sessionExpired')
  if (url.searchParams.has('signedOut') || sessionExpired) {
    return { returnTo: safeInternalPath(url.searchParams.get('returnTo')), sessionExpired }
  }

  return markAuthResponsePrivate(await startCoralOAuthLogin(request, config))
}

export default function Login({ loaderData }: Route.ComponentProps) {
  const returnTo = loaderData?.returnTo
  return (
    <main className={styles.page}>
      <PageTitle>Sign in</PageTitle>
      <section className={styles.content} aria-labelledby="login-title">
        <Brand />
        <Card.Container className={styles.panel}>
          <Typography.HeadingMedium as="h1" id="login-title">
            {loaderData?.sessionExpired ? 'Your session expired' : 'You are signed out'}
          </Typography.HeadingMedium>
          {/* GET, so submitting replaces the query wholesale — the interstitial flag drops
              away and the loader starts a real login on the next pass. */}
          <Form action={routePath('login')} className={styles.actions} method="get">
            {returnTo && returnTo !== '/' ? (
              <input type="hidden" name="returnTo" value={returnTo} />
            ) : null}
            <Button.TextButton type="submit" size="36" fullWidth>
              Sign in
            </Button.TextButton>
          </Form>
        </Card.Container>
      </section>
    </main>
  )
}

import { data, Form, redirect } from 'react-router'

import type { Route } from './+types/login'

import { startCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { authPrivateHeaders, markAuthResponsePrivate } from '@/auth/response.server'
import { safeInternalPath } from '@/auth/safe-path.server'
import { finishPendingSignOuts, resolveCoralWebSession } from '@/auth/session.server'
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

  // A sign-out Coral did not answer is finished here, before anything else.
  const cookies = new Headers()
  const pending = await finishPendingSignOuts(request, config)
  if (pending) cookies.append('Set-Cookie', pending.setCookie)
  const signOutPending = pending?.unfinished ?? false

  let session
  try {
    session = await resolveCoralWebSession(request, config)
  } catch (error) {
    if (error instanceof Response) throw markAuthResponsePrivate(error)
    throw error
  }
  if (session) return markAuthResponsePrivate(redirect('/', { headers: cookies }))

  const url = new URL(request.url)
  // The interstitial is the one branch that renders instead of starting a login,
  // so it is also the one that has to carry the destination forward itself. It
  // is sanitized here rather than on submit because it is about to be written
  // into the page and handed back as a query parameter.
  const sessionExpired = url.searchParams.has('sessionExpired')
  if (url.searchParams.has('signedOut') || sessionExpired || signOutPending) {
    return data(
      {
        returnTo: safeInternalPath(url.searchParams.get('returnTo')),
        sessionExpired,
        signOutPending,
      },
      { headers: cookies },
    )
  }

  const login = await startCoralOAuthLogin(request, config)
  for (const cookie of cookies.getSetCookie()) login.headers.append('Set-Cookie', cookie)
  return markAuthResponsePrivate(login)
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
          {loaderData?.signOutPending ? (
            <Typography.Body role="status">
              Sign-out is not complete. Reload this page to finish it.
            </Typography.Body>
          ) : null}
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

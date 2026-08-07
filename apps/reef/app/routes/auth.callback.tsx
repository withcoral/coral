import type { Route } from './+types/auth.callback'

import { reefAuthConfig } from '@/auth/config.server'
import { completeCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { markAuthResponsePrivate } from '@/auth/response.server'
import { routePath } from '@/routing/routemap'
import * as Button from '@/wax/components/button'
import { Typography } from '@/wax/components/typography'

import * as styles from './login.css'

export async function loader({ request }: Route.LoaderArgs) {
  const config = reefAuthConfig()
  if (config.mode === 'disabled') throw new Response('Not Found', { status: 404 })

  try {
    return markAuthResponsePrivate(await completeCoralOAuthLogin(request, config))
  } catch (error) {
    if (error instanceof Response) markAuthResponsePrivate(error)
    throw error
  }
}

export default function AuthCallback() {
  return null
}

export function ErrorBoundary() {
  return (
    <main className={styles.page}>
      <section className={styles.content} aria-labelledby="auth-error-title">
        <Typography.HeadingLarge as="h1" id="auth-error-title">
          Sign-in failed
        </Typography.HeadingLarge>
        <Typography.Body>
          Try signing in again. If the problem continues, contact your administrator.
        </Typography.Body>
        <div className={styles.actions}>
          <Button.InternalLink to={routePath('login')}>Try again</Button.InternalLink>
        </div>
      </section>
    </main>
  )
}

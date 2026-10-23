import type { Route } from './+types/auth.callback'

import { completeCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { markAuthResponsePrivate } from '@/auth/response.server'
import { runtimeConfigContext } from '@/lib/runtime-config.server'
import { routePath } from '@/routing/routemap'
import * as Button from '@/wax/components/button'
import { PageTitle } from '@/wax/components/page-title'
import { Typography } from '@/wax/components/typography'

import * as styles from './login.css'

export async function loader({ context, request }: Route.LoaderArgs) {
  const config = context.get(runtimeConfigContext).auth
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
  const title = 'Sign-in failed'
  return (
    <main className={styles.page}>
      <PageTitle>{title}</PageTitle>
      <section className={styles.content} aria-labelledby="auth-error-title">
        <Typography.HeadingLarge as="h1" id="auth-error-title">
          {title}
        </Typography.HeadingLarge>
        <div className={styles.actions}>
          <Button.InternalLink to={routePath('login')}>Try again</Button.InternalLink>
        </div>
      </section>
    </main>
  )
}

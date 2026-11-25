import { isRouteErrorResponse, useRouteError } from 'react-router'

import type { Route } from './+types/auth.callback'

import { completeCoralOAuthLogin, type FailedSignIn } from '@/auth/coral-oauth.server'
import { markAuthResponsePrivate } from '@/auth/response.server'
import { runtimeConfigContext } from '@/lib/runtime-config.server'
import { routePath } from '@/routing/routemap'
import * as Button from '@/wax/components/button'
import * as DescriptionList from '@/wax/components/description-list'
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
  const error = useRouteError()
  // The loader puts a code and a time in the error only when they had their one form.
  const failed = isRouteErrorResponse(error) && isFailedSignIn(error.data) ? error.data : null
  return (
    <main className={styles.page}>
      <PageTitle>{title}</PageTitle>
      <section className={styles.content} aria-labelledby="auth-error-title">
        <Typography.HeadingLarge as="h1" id="auth-error-title">
          {title}
        </Typography.HeadingLarge>
        {failed ? (
          <>
            <Typography.Body>Give this code and time to your administrator.</Typography.Body>
            <DescriptionList.List>
              <DescriptionList.Row label="Code">{failed.code}</DescriptionList.Row>
              <DescriptionList.Row label="Time">{failed.time}</DescriptionList.Row>
            </DescriptionList.List>
          </>
        ) : null}
        <div className={styles.actions}>
          <Button.InternalLink to={routePath('login')}>Try again</Button.InternalLink>
        </div>
      </section>
    </main>
  )
}

function isFailedSignIn(value: unknown): value is FailedSignIn {
  const candidate = value as Partial<FailedSignIn> | null
  return typeof candidate?.code === 'string' && typeof candidate.time === 'string'
}

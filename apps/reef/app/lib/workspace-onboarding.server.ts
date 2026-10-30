import { createCookie, type RouterContextProvider } from 'react-router'
import { authCookieSecure, hostBoundCookieName } from '@/auth/config.server'
import { runtimeConfigContext } from './runtime-config.server'

const ONBOARDING_COOKIE = 'coral_web_workspace_onboarding'
const ONBOARDING_MAX_AGE_SECONDS = 10 * 60

/**
 * The one-time note, for the browser that created a Workspace, that its overview
 * opens onboarding. It names the Workspace, so no other overview uses it up. It
 * grants nothing: the overview loads the Workspace with the caller's own authority.
 */
function onboardingCookie(context: Readonly<RouterContextProvider>) {
  const auth = context.get(runtimeConfigContext).auth
  const secure = auth.mode === 'required' && authCookieSecure(auth)
  return createCookie(hostBoundCookieName(ONBOARDING_COOKIE, secure), {
    httpOnly: true,
    sameSite: 'lax',
    path: '/',
    maxAge: ONBOARDING_MAX_AGE_SECONDS,
    secure,
    secrets: auth.mode === 'required' ? [auth.sessionSecret] : undefined,
  })
}

/** The `Set-Cookie` value that opens onboarding on `workspace`'s next overview. */
export async function commitWorkspaceOnboarding(
  workspace: string,
  context: Readonly<RouterContextProvider>,
) {
  return onboardingCookie(context).serialize(workspace)
}

/**
 * Whether `workspace`'s overview opens onboarding, and the `Set-Cookie` value that
 * stops it opening again. Another Workspace's overview leaves the note in place.
 */
export async function consumeWorkspaceOnboarding(
  request: Request,
  workspace: string,
  context: Readonly<RouterContextProvider>,
): Promise<{ onboarding: boolean; setCookie: string | null }> {
  const cookie = onboardingCookie(context)
  const value: unknown = await cookie.parse(request.headers.get('cookie'))
  if (value !== workspace) return { onboarding: false, setCookie: null }
  return { onboarding: true, setCookie: await cookie.serialize('', { maxAge: 0 }) }
}

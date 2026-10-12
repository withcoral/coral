import { createHash, randomBytes } from 'node:crypto'
import type { BrowserContext, Page } from 'playwright'
import { inject, type TestContext } from 'vitest'

import { withBrowserSession } from './browser-session.ts'
import type { Target } from './config.ts'
import type { SyntheticUser } from './oidc-fixture.ts'

export interface Actor extends SyntheticUser {
  /** Coral's stable User identity, distinct from the upstream OIDC subject. */
  userId: string
  // Test-only credentials from real login. Never put these in run metadata,
  // logs, the viewer, a browser view model, or a committed storage-state file.
  accessToken: string
  storageState: Awaited<ReturnType<BrowserContext['storageState']>>
}

export function authenticatedTarget(): Target & {
  authentication: NonNullable<Target['authentication']>
} {
  const { target } = inject('reel')
  if (!target.authentication) throw new Error('This scenario requires an authenticated target')
  return { ...target, authentication: target.authentication }
}

export async function newUser(
  test: TestContext,
  label: string,
  emailVerified = true,
  email?: string,
): Promise<SyntheticUser> {
  const { authentication } = authenticatedTarget()
  const response = await fetch(`${authentication.oidcURL}/fixture/accounts`, {
    method: 'POST',
    signal: test.signal,
    headers: { 'Content-Type': 'application/json', Origin: authentication.oidcURL },
    body: JSON.stringify({ label, emailVerified, email }),
  })
  if (!response.ok) throw new Error(`Could not create synthetic identity: HTTP ${response.status}`)
  const user: SyntheticUser = await response.json()
  test.onTestFinished(async ({ task }) => {
    try {
      const removed = await fetch(
        `${authentication.oidcURL}/fixture/accounts/${encodeURIComponent(user.subject)}`,
        {
          method: 'DELETE',
          signal: AbortSignal.timeout(5000),
          headers: { Origin: authentication.oidcURL },
        },
      )
      if (!removed.ok) throw new Error('Could not remove synthetic identity')
    } catch (error) {
      if (task.result?.state === 'fail')
        console.warn('Synthetic identity cleanup failed after the test failed')
      else throw error
    }
  })
  return user
}

// No cookie signing, token minting, or auth-module imports: drive the normal
// browser redirects. Reuse only genuine storage state after this flow succeeds.
export async function signIn(page: Page, user: SyntheticUser, target = authenticatedTarget()) {
  await page.goto(`${target.baseURL}/`)
  await page.waitForURL(
    (url) =>
      url.origin === target.authentication.oidcURL && url.pathname.startsWith('/interaction/'),
  )
  await page.getByRole('heading', { name: 'Synthetic identity provider', exact: true }).waitFor()
  // Subject is the stable identity; labels may repeat across scenarios.
  await page.locator(`button[value="${user.subject}"]`).click()
  // Home opens the first Workspace's Activity, or stays home when there is none.
  await page.waitForURL(
    (url) =>
      url.origin === target.baseURL &&
      (url.pathname === '/' || /^\/workspaces\/[^/]+\/activity$/.test(url.pathname)),
  )
  await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
}

async function oauthToken(
  page: Page,
  resource: string,
  audience: 'reef' | undefined,
  target = authenticatedTarget(),
) {
  const { issuerURL, oidcURL } = target.authentication
  const verifier = randomBytes(32).toString('base64url')
  const state = randomBytes(32).toString('base64url')
  const clientId = `${oidcURL}/.well-known/oauth-client`
  const redirectUri = `${oidcURL}/fixture/callback`
  const authorization = new URL('/oauth/authorize', issuerURL)
  authorization.search = new URLSearchParams({
    response_type: 'code',
    client_id: clientId,
    redirect_uri: redirectUri,
    resource,
    state,
    code_challenge: createHash('sha256').update(verifier).digest('base64url'),
    code_challenge_method: 'S256',
  }).toString()
  // The context's real IdP session selects this actor again. This is a second,
  // genuine OAuth client, configured as trusted only to omit Coral's consent
  // ceremony. APIRequestContext follows the actual redirects without taking
  // the recorded browser away from Reef. PKCE and one-use redemption still run.
  const authorized = await page.request.get(authorization.toString())
  const callback = new URL(authorized.url())
  const code = callback.searchParams.get('code')
  if (
    callback.origin !== oidcURL ||
    callback.pathname !== '/fixture/callback' ||
    callback.searchParams.get('state') !== state ||
    !code ||
    callback.searchParams.has('error')
  ) {
    // Name where the redirects ended, without the code or state they carry.
    const reason = callback.searchParams.get('error')
    throw new Error(
      `Fixture OAuth callback failed or has invalid state: HTTP ${authorized.status()} at ` +
        `${callback.origin}${callback.pathname}${reason ? `, error ${reason}` : ''}`,
    )
  }
  const response = await page.request.post(`${issuerURL}/oauth/token`, {
    form: {
      grant_type: 'authorization_code',
      code,
      client_id: clientId,
      redirect_uri: redirectUri,
      resource,
      code_verifier: verifier,
      ...(audience ? { coral_audience: audience } : {}),
    },
  })
  if (!response.ok()) throw new Error(`Fixture OAuth exchange failed: HTTP ${response.status()}`)
  const body = await response.json()
  if (body.token_type !== 'Bearer' || typeof body.access_token !== 'string') {
    throw new Error('Fixture OAuth exchange returned no bearer credential')
  }
  return body.access_token as string
}

/** A genuine User OAuth grant for the exact Workspace MCP resource, not a Reef token. */
export async function mcpToken(page: Page, workspace: string) {
  const target = authenticatedTarget()
  // Public HTTP validates the exact resource audience. coral_audience=mcp
  // instead selects the private first-party MCP audience, not this endpoint.
  return oauthToken(page, `${target.mcpURL}/workspaces/${workspace}/mcp`, undefined, target)
}

export async function loginActor(
  test: TestContext,
  user: SyntheticUser,
  target = authenticatedTarget(),
): Promise<Actor> {
  let actor: Actor | undefined
  await withBrowserSession(
    test,
    async ({ page, step }) => {
      await step(`Sign in as ${user.label}`, async () => {
        await signIn(page, user, target)
      })
      const storageState = await page.context().storageState()
      const accessToken = await oauthToken(page, target.baseURL, 'reef', target)
      const claims = JSON.parse(Buffer.from(accessToken.split('.')[1], 'base64url').toString())
      if (typeof claims.sub !== 'string' || !claims.sub)
        throw new Error('Coral issued no User identity')
      actor = { ...user, userId: claims.sub, storageState, accessToken }
    },
    { target, actor: user.label, label: 'Login', purpose: 'setup' },
  )
  return actor!
}

/** Ordinary product setup: a real Owner sign-in, recorded separately from the journey. */
export async function withOwnerSession(
  test: TestContext,
  body: (
    session: Parameters<Parameters<typeof withBrowserSession>[1]>[0] & { actor: Actor },
  ) => Promise<void>,
) {
  const actor = await loginActor(test, authenticatedTarget().authentication.owner)
  await withBrowserSession(test, (session) => body({ ...session, actor }), {
    actor: actor.label,
    storageState: actor.storageState,
  })
}

export async function createActors(test: TestContext) {
  const { authentication } = authenticatedTarget()
  return {
    owner: await loginActor(test, authentication.owner),
    alice: await loginActor(test, await newUser(test, 'Alice')),
    bob: await loginActor(test, await newUser(test, 'Bob')),
  }
}

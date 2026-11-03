import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { request } from 'playwright'
import { expect, inject, test } from 'vitest'
import { loginActor } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { startStack } from '../../../support/stack.ts'

// Coral access tokens last five seconds, and its verifier allows a minute of
// clock skew; Reef sessions last a hundred seconds from sign-in.
const ACCESS_TOKEN_TTL_SECONDS = 5
const PAST_ACCESS_TOKEN_EXPIRY_MS = (ACCESS_TOKEN_TTL_SECONDS + 65) * 1000
const WEB_SESSION_MAX_AGE_SECONDS = 100

test('A Reef session outlives its access tokens, ends at its own deadline, and stays ended after sign-out', async (context) => {
  const config = inject('reel')
  const bootstrap = inject('reelBootstrap')
  const runDir = join(config.runDir, config.target.name, 'session-renewal')
  await mkdir(runDir, { recursive: true })
  // Shortened lifetimes would cut every other scenario's credentials short, so
  // this journey owns its stack and its SQLite state. Coral's own tests run
  // the session store on both databases.
  const stack = await startStack({
    ...bootstrap,
    postgresURL: undefined,
    runDir,
    authenticated: true,
    sessionLifetimes: {
      accessTokenTtlSeconds: ACCESS_TOKEN_TTL_SECONDS,
      webSessionMaxAgeSeconds: WEB_SESSION_MAX_AGE_SECONDS,
    },
  })
  context.onTestFinished(() => stack.stop())
  const target = { ...stack.target, authentication: stack.target.authentication! }
  const owner = await loginActor(context, target.authentication.owner, target)
  const signedInAt = Date.now()

  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const accountMenu = page.getByRole('button', { name: /^Open account menu for / })
      const dialog = page.getByRole('dialog')
      let workspacePath = ''

      await step(
        'Open the create workspace form while the first access token is fresh',
        async () => {
          await page.goto('/')
          await accountMenu.waitFor()
          await page
            .getByRole('navigation', { name: 'Coral', exact: true })
            .getByRole('button', { name: 'Create workspace', exact: true })
            .click()
          await page.getByLabel('Workspace name', { exact: true }).fill('Renewed')
        },
      )

      await step('Submit the open form after every token minted so far has expired', async () => {
        await page.waitForTimeout(PAST_ACCESS_TOKEN_EXPIRY_MS)
        await dialog.getByRole('button', { name: 'Create workspace', exact: true }).click()
        await page.waitForURL((url) => /^\/workspaces\/[0-9a-f-]{36}$/.test(url.pathname))
        workspacePath = new URL(page.url()).pathname
        // One submission, one Workspace: renewal neither drops nor repeats it.
        await expect
          .poll(() =>
            page
              .getByRole('navigation', { name: 'Coral', exact: true })
              .getByRole('button', { name: 'Renewed', exact: true })
              .count(),
          )
          .toBe(1)
      })

      await step('Answer concurrent protected requests from one session', async () => {
        const responses = await Promise.all(
          Array.from({ length: 6 }, () => page.request.get(workspacePath, { maxRedirects: 0 })),
        )
        expect(responses.map((response) => response.status())).toEqual(Array(6).fill(200))
      })

      await step('Offer sign-in once the session reaches its absolute end', async () => {
        const remaining = signedInAt + (WEB_SESSION_MAX_AGE_SECONDS + 5) * 1000 - Date.now()
        await page.waitForTimeout(Math.max(0, remaining))
        await page.reload()
        await page.getByRole('heading', { name: 'Your session expired', exact: true }).waitFor()
        await page.getByRole('button', { name: 'Sign in', exact: true }).click()
        await accountMenu.waitFor()
      })

      await step('Sign out, and keep a copied session cookie from returning', async () => {
        const copied = await page.context().storageState()
        await page.goto(workspacePath)
        await accountMenu.click()
        await page.getByRole('menuitem', { name: 'Sign out', exact: true }).click()
        await page.getByRole('heading', { name: 'You are signed out', exact: true }).waitFor()

        const replay = await request.newContext({ baseURL: target.baseURL, storageState: copied })
        try {
          const response = await replay.get(workspacePath, { maxRedirects: 0 })
          expect(response.status()).toBe(302)
          expect(response.headers().location).toMatch(/^\/login\?/)
        } finally {
          await replay.dispose()
        }
      })
    },
    { target, actor: owner.label, storageState: owner.storageState },
  )
}, 300_000)

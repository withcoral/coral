import { type Browser, chromium } from 'playwright'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { csrfTokenForRequest, validateCsrfToken } from './csrf.server'
import {
  commitCoralWebSession,
  commitOAuthTransaction,
  readCoralWebSession,
  readOAuthTransaction,
} from './session.server'
import { requiredAuthConfig } from './test-config'
import type { AuthSession } from './types'

// Reef signs every cookie it sets, and that proves nothing here: Mallory holds
// a Coral account of her own, so each cookie she tosses was minted by this same
// server for her own session. What keeps Alice's session hers is the browser
// refusing to store a `__Host-` cookie that carries `Domain` or a `Path` other
// than `/`. That is a browser rule, so a real Chromium holds the cookie jar.
// Both hosts are served from route handlers: the browser stores a fulfilled
// route's `Set-Cookie` headers and attaches the `Cookie` header to the next
// request exactly as it would for a network response.
const config = requiredAuthConfig
const REGISTRABLE_DOMAIN = 'example.test'
const reef = new URL(config.publicUrl)
const sibling = new URL(`https://mallory.${REGISTRABLE_DOMAIN}/`)
// The browser lists cookies with a longer `Path` first, so a tossed cookie
// scoped to this path would precede Alice's on a request under it.
const DEEP_PATH = '/workspaces/analytics'
// A cookie without the prefix, tossed the same way, shows the attack was
// delivered. Were the browser not storing the sibling's cookies at all, the
// assertions below would pass for the wrong reason.
const CANARY = 'tossed_canary'

const alice: AuthSession = {
  accessToken: 'alice-coral-access-token',
  expiresAt: unixTimestamp() + 1800,
  tokenType: 'Bearer',
}
const mallory: AuthSession = { ...alice, accessToken: 'mallory-coral-access-token' }

interface Visit {
  setCookie: string[]
  url: URL
}

let browser: Browser

beforeAll(async () => {
  if (!reef.hostname.endsWith(`.${REGISTRABLE_DOMAIN}`)) {
    throw new Error(`${reef.hostname} must be a subdomain of ${REGISTRABLE_DOMAIN}`)
  }
  browser = await chromium.launch()
}, 60_000)

afterAll(async () => {
  await browser?.close()
})

describe('hosted Reef cookies', () => {
  it('stay bound to Reef’s host when a sibling host tosses its own signed cookies at the registrable domain', async () => {
    const aliceCsrf = await csrfTokenForRequest(new Request(reef.href), config, alice)
    const malloryCsrf = await csrfTokenForRequest(new Request(reef.href), config, mallory)
    const malloryCookies = [
      await commitCoralWebSession(mallory, config),
      malloryCsrf.setCookie!,
      await commitOAuthTransaction(
        { codeVerifier: 'mallory', returnTo: '/', state: 'mallory' },
        config,
      ),
    ]

    const cookieHeaders = await browse([
      // Alice signs in: Reef's own response sets her session and CSRF cookies.
      {
        setCookie: [await commitCoralWebSession(alice, config), aliceCsrf.setCookie!],
        url: new URL('/', reef),
      },
      // Mallory's host re-serves her cookies scoped to the registrable domain,
      // at Reef's path and at a longer one.
      {
        setCookie: [
          ...malloryCookies.flatMap((cookie) => [
            `${cookie}; Domain=${REGISTRABLE_DOMAIN}`,
            `${cookie.replace('Path=/', `Path=${DEEP_PATH}`)}; Domain=${REGISTRABLE_DOMAIN}`,
          ]),
          `${CANARY}=1; Domain=${REGISTRABLE_DOMAIN}; Path=/; Secure`,
        ],
        url: sibling,
      },
      // Alice returns to Reef under the longer path.
      { setCookie: [], url: new URL(DEEP_PATH, reef) },
    ])
    const cookieHeader = cookieHeaders[2]

    expect(cookieHeader).toContain(`${CANARY}=1`)
    const request = new Request(new URL(DEEP_PATH, reef), { headers: { cookie: cookieHeader } })
    await expect(readCoralWebSession(request, config)).resolves.toEqual(alice)
    await expect(readOAuthTransaction(request, config)).resolves.toBeNull()
    await expect(
      validateCsrfToken(postRequest(cookieHeader, aliceCsrf.token), config, alice),
    ).resolves.toBe(true)
    await expect(
      validateCsrfToken(postRequest(cookieHeader, malloryCsrf.token), config, alice),
    ).resolves.toBe(false)
  }, 30_000)
})

/**
 * Navigates one browser context through `visits` in order, serving each from
 * its own `Set-Cookie` headers, and returns the `Cookie` header the browser
 * attached to each request.
 */
async function browse(visits: Visit[]): Promise<string[]> {
  const context = await browser.newContext()
  try {
    const cookieHeaders: string[] = []
    await context.route('**/*', async (route) => {
      const index = visits.findIndex((visit) => visit.url.href === route.request().url())
      if (index === -1) {
        await route.abort()
        return
      }
      cookieHeaders[index] = route.request().headers().cookie ?? ''
      const { setCookie } = visits[index]
      await route.fulfill({
        body: '<!doctype html><title>Coral</title>',
        contentType: 'text/html',
        headers: setCookie.length > 0 ? { 'set-cookie': setCookie.join('\n') } : {},
        status: 200,
      })
    })
    const page = await context.newPage()
    for (const visit of visits) await page.goto(visit.url.href)

    return cookieHeaders
  } finally {
    await context.close()
  }
}

function postRequest(cookieHeader: string, csrf: string): Request {
  return new Request(new URL('/logout', reef), {
    body: new URLSearchParams({ csrf }),
    headers: { cookie: cookieHeader },
    method: 'POST',
  })
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}

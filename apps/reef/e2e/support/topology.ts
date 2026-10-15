import type { TestContext } from 'vitest'
import { inject } from 'vitest'

import { withOwnerSession } from './actors.ts'
import { withBrowserSession, type Session } from './browser-session.ts'

/** The Organization Owner's recorded session, in whichever deployment Reel selected. */
export interface OwnerSession extends Session {
  /** The Owner's management bearer, or undefined where the implicit local Owner needs none. */
  accessToken: string | undefined
  /**
   * Approves fixture Provider consent. Remotely it returns after the click, while
   * Reef's own callback brings the browser back. Locally, Coral's loopback listener
   * completes the sign-in, and it opens the page Reef named before it returns.
   */
  approveConsent(): Promise<void>
}

/**
 * Runs a journey as the Organization Owner in the selected deployment: a genuine
 * sign-in on the main suite, or the implicit local Owner without a ceremony.
 * Journeys that both suites run take every deployment difference from here.
 */
export async function withOwner(test: TestContext, body: (session: OwnerSession) => Promise<void>) {
  const remote = Boolean(inject('reel').target.authentication)
  const asOwner = (session: Session, accessToken: string | undefined) => {
    const { page } = session
    // Reef names the page that keeps the sign-in's progress on each consent redirect.
    let returnTo = ''
    page.on('response', (response) => {
      returnTo = response.headers()['x-coral-authorization-return-to'] ?? returnTo
    })
    return body({
      ...session,
      accessToken,
      async approveConsent() {
        const approve = page.getByRole('link', { name: 'Approve fixture access', exact: true })
        // The consent page follows the response that names the page, so read it once that page shows.
        await approve.waitFor()
        const progress = returnTo
        returnTo = ''
        await approve.click()
        if (remote) return
        if (!progress) throw new Error('Reef named no page to return to after consent')
        await page.getByText(/Return to Coral to check your connection\./).waitFor()
        await page.goto(progress)
      },
    })
  }
  if (remote) await withOwnerSession(test, (session) => asOwner(session, session.actor.accessToken))
  else await withBrowserSession(test, (session) => asOwner(session, undefined))
}

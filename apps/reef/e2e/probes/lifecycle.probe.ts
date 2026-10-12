import { expect, test } from 'vitest'
import { withBrowserSession } from '../support/browser-session.ts'

// Deliberately failing subprocess suite. Only e2e:self-check enables it.
for (const outcome of ['pass', 'assertion', 'timeout', 'after-session'] as const) {
  test(
    `Harness probe: ${outcome}`,
    async (context) => {
      await withBrowserSession(context, async ({ page, step }) => {
        await step('Open the real Coral shell', async () => {
          await page.goto('/')
          await page.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
        })
        if (outcome === 'assertion') {
          expect('actual', 'REEL_ORIGINAL_ASSERTION').toBe('expected')
        }
        if (outcome === 'timeout') {
          // Not a Playwright timeout: Vitest must close the browser even though
          // this arbitrary promise cannot be canceled and never settles.
          await new Promise(() => {})
        }
      })
      if (outcome === 'after-session') {
        expect('actual', 'REEL_LATE_ASSERTION').toBe('expected')
      }
    },
    outcome === 'timeout' ? 5000 : 30_000,
  )
}

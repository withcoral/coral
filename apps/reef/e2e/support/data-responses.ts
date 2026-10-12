import type { Page } from 'playwright'

/** Collects every loader and action payload the page receives, to prove a secret never travels. */
export async function collectDataResponses(page: Page) {
  const bodies: Promise<string>[] = []
  await page.route(
    (url) => url.pathname.endsWith('.data'),
    async (route) => {
      const capture = (async () => {
        const response = await route.fetch({ maxRedirects: 0, maxRetries: 0, timeout: 10_000 })
        try {
          const body = await response.body()
          // OAuth can navigate away after the first chunk. Capture the whole payload
          // before the browser receives it, so unloading cannot strand a body read.
          await route.fulfill({ response, body })
          return body.toString('utf8')
        } finally {
          await response.dispose()
        }
      })()
      bodies.push(capture)
      // Retain the rejection for the assertion, and unblock the browser on failure.
      await capture.catch(() => route.abort().catch(() => {}))
    },
  )
  return () => Promise.all(bodies)
}

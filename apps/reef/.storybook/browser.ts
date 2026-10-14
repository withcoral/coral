/**
 * Drives the test browser from a story's `play`, for behavior that depends on the
 * viewport or on user media preferences. Only the Vitest story runner has a test
 * browser; in Storybook's own UI each call resolves false and changes nothing.
 * `vitest.stories.config.ts` maps this import to `vitest/browser`, as addon-vitest does.
 */
const browser = () =>
  import('@vitest/browser/context')
    .then((context) =>
      (globalThis as { __vitest_browser__?: unknown }).__vitest_browser__ ? context : null,
    )
    .catch(() => null)

/** Resizes the story's viewport. addon-vitest restores its default before the next story. */
export async function resizeViewport(width: number, height: number) {
  const context = await browser()
  if (!context) return false
  await context.page.viewport(width, height)
  return true
}

/** Turns `prefers-reduced-motion: reduce` on or off. Turn it off before the play ends. */
export async function emulateReducedMotion(reduce: boolean) {
  const context = await browser()
  if (!context) return false
  await context.cdp().send('Emulation.setEmulatedMedia', {
    features: [{ name: 'prefers-reduced-motion', value: reduce ? 'reduce' : '' }],
  })
  return true
}

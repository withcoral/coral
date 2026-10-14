/**
 * Drives the test browser from a story's `play`, for behavior that depends on the
 * viewport or on user media preferences.
 *
 * Under the Vitest story runner each call applies its change, checks the page sees
 * it, and throws if it cannot, so a play never passes without the change. Storybook's
 * own UI has no test browser: there each call resolves false and changes nothing, and
 * the play skips the steps that need it.
 * `vitest.stories.config.ts` maps this import to `vitest/browser`, as addon-vitest does.
 */
async function browser() {
  if (!(globalThis as { __vitest_browser__?: unknown }).__vitest_browser__) return null
  return import('@vitest/browser/context')
}

/** Waits until `applied` holds, or throws after a second. */
async function confirm(change: string, applied: () => boolean) {
  for (let frame = 0; frame < 60; frame++) {
    if (applied()) return
    await new Promise((resolve) => requestAnimationFrame(resolve))
  }
  throw new Error(`The test browser did not apply: ${change}`)
}

/** Resizes the story's viewport. addon-vitest restores its default before the next story. */
export async function resizeViewport(width: number, height: number) {
  const context = await browser()
  if (!context) return false
  await context.page.viewport(width, height)
  await confirm(`a ${width}×${height} viewport`, () => window.innerWidth === width)
  return true
}

/** Turns `prefers-reduced-motion: reduce` on or off. Turn it off before the play ends. */
export async function emulateReducedMotion(reduce: boolean) {
  const context = await browser()
  if (!context) return false
  await context.cdp().send('Emulation.setEmulatedMedia', {
    features: [{ name: 'prefers-reduced-motion', value: reduce ? 'reduce' : '' }],
  })
  await confirm(
    `prefers-reduced-motion: ${reduce ? 'reduce' : 'no-preference'}`,
    () => matchMedia('(prefers-reduced-motion: reduce)').matches === reduce,
  )
  return true
}

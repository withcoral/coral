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

/** Waits until `applied` holds, or throws after two seconds. */
async function confirm(change: string, applied: () => boolean) {
  const deadline = performance.now() + 2000
  while (!applied()) {
    if (performance.now() > deadline) throw new Error(`The test browser did not apply: ${change}`)
    await new Promise((resolve) => setTimeout(resolve, 16))
  }
}

/** Resizes the story's viewport. addon-vitest restores its default before the next story. */
export async function resizeViewport(width: number, height: number) {
  const context = await browser()
  if (!context) return false
  await context.page.viewport(width, height)
  await confirm(
    `a ${width}×${height} viewport`,
    () => window.innerWidth === width && window.innerHeight === height,
  )
  return true
}

/** Turns `prefers-reduced-motion: reduce` on or off. Turn it off before the play ends. */
export async function emulateReducedMotion(reduce: boolean) {
  const context = await browser()
  if (!context) return false
  await context.cdp().send('Emulation.setEmulatedMedia', {
    features: [{ name: 'prefers-reduced-motion', value: reduce ? 'reduce' : 'no-preference' }],
  })
  await confirm(
    `prefers-reduced-motion: ${reduce ? 'reduce' : 'no-preference'}`,
    () => matchMedia('(prefers-reduced-motion: reduce)').matches === reduce,
  )
  return true
}

/**
 * Presses keys in the test browser itself, as a person would, so the browser moves
 * focus on Tab. Takes user-event's keyboard syntax, such as `{Shift>}{Tab}{/Shift}`.
 */
export async function press(keys: string) {
  const context = await browser()
  if (!context) return false
  await context.userEvent.keyboard(keys)
  return true
}

/** Whether a story runs in the test browser, where `press` reaches the page. */
export async function inTestBrowser() {
  return (await browser()) !== null
}

/** Clicks an element with the test browser's own mouse, so the browser moves focus. */
export async function click(element: Element) {
  const context = await browser()
  if (!context) return false
  await context.userEvent.click(element)
  return true
}

/**
 * One Web Animation the page started: its element, its transforms in order, its timing,
 * and what the recorder's `note` read from the element as it started.
 */
export interface StartedAnimation<Note = undefined> {
  target: Element
  transforms: string[]
  options: KeyframeAnimationOptions | number | undefined
  animation: Animation
  note: Note
}

/**
 * Records every Web Animation the page starts with `Element.animate` until `stop`.
 * Each still plays as it would without the recording. `note` reads the element as the
 * animation starts, for what the page may change or remove before the play looks.
 */
export function recordAnimations<Note = undefined>(note?: (target: Element) => Note) {
  const started: StartedAnimation<Note>[] = []
  const animate = Element.prototype.animate
  Element.prototype.animate = function (this: Element, keyframes, options) {
    const animation = animate.call(this, keyframes, options)
    const frames = Array.isArray(keyframes) ? keyframes : []
    started.push({
      target: this,
      transforms: frames.map((frame) => String(frame.transform ?? '')),
      options,
      animation,
      note: note?.(this) as Note,
    })
    return animation
  }
  return {
    started,
    /** Forgets what it recorded so far. */
    clear: () => void started.splice(0),
    stop: () => {
      Element.prototype.animate = animate
    },
  }
}

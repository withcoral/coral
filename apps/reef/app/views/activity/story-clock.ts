/** The moment a story with a live deadline renders at, so its label reads the same in every
 * run and snapshot, however long after loading the story runs. */
export const storyNow = Date.parse('2026-09-18T14:06:02.000Z')

/** `minutes` after `storyNow`, as Coral sends a deadline. */
export const minutesAfterStoryNow = (minutes: number) =>
  new Date(storyNow + minutes * 60_000).toISOString()

/** A story `beforeEach`: the page reads `storyNow` as the time, and the real clock returns after.
 * Not a Storybook spy: the test runner records each spy call with `Date.now`, which would recurse. */
export function freezeStoryClock() {
  const realNow = Date.now
  Date.now = () => storyNow
  return () => {
    Date.now = realNow
  }
}

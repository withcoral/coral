import type { Locator } from 'playwright'

/**
 * The exact logical arguments the first Arguments disclosure in `scope` shows. Reef
 * shows a JSON object one field per row, each value as its exact JSON text. It shows any
 * other JSON, or an object that rows would change, whole in one block.
 */
export async function shownArguments(scope: Locator): Promise<unknown> {
  // The summary's own disclosure, not an outer one that also holds it.
  const section = scope
    .locator('summary')
    .filter({ hasText: /^Arguments( \(\d+\))?$/ })
    .first()
    .locator('xpath=..')
  await section.waitFor()
  return section.evaluate((details) => {
    const rows = [...details.querySelectorAll(':scope dl > div')]
    if (rows.length)
      return Object.fromEntries(
        rows.map((row) => [
          row.querySelector('dt')!.textContent,
          JSON.parse(row.querySelector('dd')!.textContent!),
        ]),
      )
    const whole = details.querySelector('pre')
    return whole ? JSON.parse(whole.textContent!) : {}
  })
}

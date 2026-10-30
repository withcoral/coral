import { readFile } from 'node:fs/promises'

import type { Locator, Page } from 'playwright'

/**
 * The first retained payload in `scope` whose viewer shows `text`. Reef shows each log
 * message, emitted value, operation response and final value in one payload viewer.
 */
export function shownPayload(scope: Page | Locator, text: string): Locator {
  return scope
    .getByRole('group', { name: 'Payload content', exact: true })
    .filter({ hasText: text })
    .first()
}

/**
 * The payload named `label` in `scope`, exactly as retained: the file its Download
 * action saves. A long payload shows only its rows near the view, so its text on the
 * page can be only part of it.
 */
export async function retainedPayload(page: Page, scope: Locator, label: string) {
  const [download] = await Promise.all([
    page.waitForEvent('download'),
    scope.getByRole('button', { name: `Download ${label}`, exact: true }).click(),
  ])
  return readFile(await download.path(), 'utf8')
}

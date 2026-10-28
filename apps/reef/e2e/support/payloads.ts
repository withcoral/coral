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
 * The JSON payload named `label` in `scope`, exactly as retained: the Raw view of its
 * expanded viewer. Closes the viewer again.
 */
export async function retainedPayload(page: Page, scope: Locator, label: string) {
  await scope.getByRole('button', { name: `Expand ${label}`, exact: true }).click()
  const viewer = page.getByRole('dialog', { name: label, exact: true })
  await viewer.getByRole('radio', { name: 'Raw', exact: true }).click()
  const retained = await viewer.getByRole('group', { name: 'Raw', exact: true }).textContent()
  await viewer.getByRole('button', { name: 'Close', exact: true }).click()
  await viewer.waitFor({ state: 'detached' })
  return retained ?? ''
}

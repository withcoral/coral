import { expect, test } from 'vitest'
import type { Page } from 'playwright'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import { createWorkspace, publishWorkspaceProvider } from '../../../../support/providers.ts'

/** The bulk form's Apply, while it counts this many selected tools. */
const applyTo = (page: Page, count: number) =>
  page
    .getByRole('form', { name: 'Bulk policy', exact: true })
    .filter({ hasText: new RegExp(`(^|\\D)${count} selected`) })
    .getByRole('button', { name: 'Apply', exact: true })

const row = (page: Page, name: string) =>
  page.getByRole('row').filter({
    has: page.getByRole('checkbox', { name: `Select ${name}`, exact: true }),
  })

test('Drawer policy shares optimistic state and locks with the table', async (context) => {
  await withOwnerSession(context, async ({ page, actor }) => {
    await openPolicies(page, actor.accessToken)
    const held = await holdPolicies(page)
    try {
      await row(page, 'first').getByRole('checkbox').check()
      await row(page, 'first').getByRole('link', { name: 'first', exact: true }).click()
      const drawer = page.getByRole('dialog', { name: 'first', exact: true })
      const policyHeight = () =>
        drawer
          .locator('form[aria-busy]')
          .evaluate((element) => element.getBoundingClientRect().height)
      const before = await policyHeight()
      await drawer.getByRole('radio', { name: 'Always allow', exact: true }).click()
      await drawer
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      expect(await policyHeight()).toBe(before)
      expect(
        await drawer
          .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
          .isDisabled(),
      ).toBe(true)
      const opened = new URL(page.url()).pathname
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      // The route closes after the drawer's exit. A click on the same row
      // before then changes nothing, so the drawer would stay closed.
      await page.waitForURL((url) => url.pathname !== opened)
      await expect.poll(() => row(page, 'first').getAttribute('aria-busy')).toBe('true')
      await row(page, 'first')
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      // Selection survives the change; the bulk action is what waits.
      expect(await row(page, 'first').getByRole('checkbox').isChecked()).toBe(true)
      expect(await row(page, 'first').getByRole('checkbox').isDisabled()).toBe(false)
      expect(await applyTo(page, 1).isDisabled()).toBe(true)
      expect(
        await row(page, 'first')
          .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
          .isDisabled(),
      ).toBe(true)
      await row(page, 'first').getByRole('link', { name: 'first', exact: true }).click()
      await drawer
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      await expect.poll(() => held.submitted.length).toBe(1)
      held.release()
      await drawer
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      expect(await policyHeight()).toBe(before)
      await page.reload()
      await drawer
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      await row(page, 'first')
        .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
        .waitFor()
    } finally {
      held.release()
    }
  })
})

async function openPolicies(page: Page, accessToken: string) {
  const workspace = await createWorkspace(accessToken, `Pending policy ${crypto.randomUUID()}`)
  const { provider } = await publishWorkspaceProvider(
    accessToken,
    `workspaces/${workspace}`,
    openApiDocument('Pending', {
      first: { method: 'post' },
      second: { method: 'post' },
      third: { method: 'post' },
    }),
    'pending',
  )
  await page.goto(`/${provider}`)
  await row(page, 'first').waitFor()
  // Leave the Built-in Provider's tools out of every selection.
  await page.getByRole('searchbox', { name: 'Filter providers' }).fill('Pending')
}

// Hold the actual browser POST, then let the real action/backend respond. No
// synthetic success or failure payloads: rejection uses a nonexistent tool.
async function holdPolicies(page: Page, reject = false) {
  let release!: () => void
  const gate = new Promise<void>((resolve) => {
    release = resolve
  })
  const submitted: URLSearchParams[] = []
  await page.route('**/providers/**', async (route) => {
    const request = route.request()
    const form = new URLSearchParams(request.postData() ?? '')
    if (request.method() !== 'POST' || form.get('intent') !== 'policy') return route.continue()
    submitted.push(form)
    await gate
    if (reject) {
      // Point the last selected tool at one Coral does not know, so the
      // request reaches Coral and comes back with a per-tool failure.
      const selected = form.getAll('selectedTool')
      const last = JSON.parse(selected.pop()!) as { provider: string; tool: string }
      form.delete('selectedTool')
      for (const tool of selected) form.append('selectedTool', tool)
      form.append(
        'selectedTool',
        JSON.stringify({
          ...last,
          tool: last.tool.replace(/\/tools\/[^/]+$/, '/tools/missingTool'),
        }),
      )
      return route.continue({ postData: form.toString() })
    }
    return route.continue()
  })
  return { release, submitted }
}

test('Inline pending policy survives filtering, blocks overlaps, and allows unrelated writes', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    await openPolicies(page, actor.accessToken)
    const held = await holdPolicies(page)
    try {
      await step('Show the submitted policy as saving, not saved', async () => {
        await row(page, 'first').getByRole('radio', { name: 'Always allow', exact: true }).click()
        await expect.poll(() => row(page, 'first').getAttribute('aria-busy')).toBe('true')
        await row(page, 'first')
          .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
          .waitFor()
        expect(await row(page, 'first').getAttribute('aria-busy')).toBe('true')
        await page.emulateMedia({ reducedMotion: 'no-preference' })
        expect(
          await row(page, 'first').evaluate((element) => getComputedStyle(element).animationName),
        ).not.toBe('none')
        expect(
          await row(page, 'third').evaluate((element) => getComputedStyle(element).animationName),
        ).toBe('none')
        await page.emulateMedia({ reducedMotion: 'reduce' })
        expect(
          await row(page, 'first').evaluate((element) => getComputedStyle(element).animationName),
        ).toBe('none')
        await page.emulateMedia({ reducedMotion: 'no-preference' })
      })
      await step('Filter away and restore a pending row without losing ownership', async () => {
        await page.getByRole('searchbox', { name: 'Filter tools' }).fill('second')
        expect(await page.getByText('Saving…', { exact: true }).count()).toBe(0)
        await page.getByRole('searchbox', { name: 'Filter tools' }).fill('')
        await expect.poll(() => row(page, 'first').getAttribute('aria-busy')).toBe('true')
        expect(
          await row(page, 'first')
            .getByRole('radio', { name: 'Needs approval', exact: true })
            .isDisabled(),
        ).toBe(true)
        expect(await row(page, 'first').getByRole('checkbox').isDisabled()).toBe(false)
        // Group and header selection include the pending row; only Apply waits.
        await page.getByRole('checkbox', { name: 'Select all matching tools', exact: true }).check()
        expect(await row(page, 'first').getByRole('checkbox').isChecked()).toBe(true)
        expect(await applyTo(page, 3).isDisabled()).toBe(true)
        await page
          .getByRole('checkbox', { name: 'Select all matching tools', exact: true })
          .uncheck()
        await page.getByRole('checkbox', { name: 'Select Other tools', exact: true }).check()
        expect(await row(page, 'first').getByRole('checkbox').isChecked()).toBe(true)
        expect(await applyTo(page, 3).isDisabled()).toBe(true)
        await page.getByRole('checkbox', { name: 'Select Other tools', exact: true }).uncheck()
      })
      await step('Submit a disjoint inline mutation while the first is pending', async () => {
        await row(page, 'second').getByRole('radio', { name: 'Disabled', exact: true }).click()
        await expect.poll(() => row(page, 'second').getAttribute('aria-busy')).toBe('true')
        await row(page, 'second')
          .getByRole('radio', { name: 'Disabled', exact: true, checked: true })
          .waitFor()
        expect(await row(page, 'third').getAttribute('aria-busy')).toBe('false')
        expect(
          await row(page, 'third')
            .getByRole('radio', { name: 'Needs approval', exact: true, checked: true })
            .isEnabled(),
        ).toBe(true)
        await expect.poll(() => held.submitted.length).toBe(2)
        held.release()
        await expect.poll(() => row(page, 'first').getAttribute('aria-busy')).toBe('false')
        await expect.poll(() => row(page, 'second').getAttribute('aria-busy')).toBe('false')
        await page.reload()
        await row(page, 'first')
          .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
          .waitFor()
        await row(page, 'second')
          .getByRole('radio', { name: 'Disabled', exact: true, checked: true })
          .waitFor()
      })
    } finally {
      held.release()
    }
  })
})

test('Bulk pending policy survives toolbar unmount and rolls back a rejection outside filtered rows', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    await openPolicies(page, actor.accessToken)
    const held = await holdPolicies(page, true)
    try {
      await step('Optimistically update and lock only the submitted bulk rows', async () => {
        await page.getByRole('checkbox', { name: 'Select first', exact: true }).check()
        await page.getByRole('checkbox', { name: 'Select second', exact: true }).check()
        const bulk = page.getByRole('form', { name: 'Bulk policy', exact: true })
        await bulk.getByRole('button', { name: 'Needs approval', exact: true }).click()
        await page.getByRole('menuitemradio', { name: 'Always allow', exact: true }).click()
        await applyTo(page, 2).click()
        // The selection stays but holds still: a bulk save locks every
        // selection control until Coral answers.
        expect(await applyTo(page, 2).isDisabled()).toBe(true)
        for (const name of ['first', 'second']) {
          expect(await row(page, name).getByRole('checkbox').isChecked()).toBe(true)
          expect(await row(page, name).getByRole('checkbox').isDisabled()).toBe(true)
          await expect.poll(() => row(page, name).getAttribute('aria-busy')).toBe('true')
          await row(page, name)
            .getByRole('radio', { name: 'Always allow', exact: true, checked: true })
            .waitFor()
          expect(await row(page, name).getAttribute('aria-busy')).toBe('true')
          expect(
            await row(page, name)
              .getByRole('radio', { name: 'Needs approval', exact: true })
              .isDisabled(),
          ).toBe(true)
        }
        expect(await row(page, 'third').getAttribute('aria-busy')).toBe('false')
      })
      await step('Hold the selection still while the bulk change is pending', async () => {
        // "third" is free, but a second bulk change could target "first" and
        // "second" twice, so every selection control waits with Apply.
        expect(
          await page.getByRole('checkbox', { name: 'Select third', exact: true }).isDisabled(),
        ).toBe(true)
        expect(
          await page
            .getByRole('checkbox', { name: 'Select all matching tools', exact: true })
            .isDisabled(),
        ).toBe(true)
        await applyTo(page, 2).click({ force: true })
        expect(await held.submitted.length).toBe(1)
        expect(await row(page, 'third').getAttribute('aria-busy')).toBe('false')
      })
      await step('Unmount the bulk form, then roll back the rejected rows', async () => {
        await page.getByRole('searchbox', { name: 'Filter tools' }).fill('third')
        expect(await page.getByText('Saving…', { exact: true }).count()).toBe(0)
        await expect.poll(() => held.submitted.length).toBe(1)
        held.release()
        await page.getByRole('searchbox', { name: 'Filter tools' }).fill('')
        for (const name of ['first', 'second']) {
          await row(page, name)
            .getByRole('radio', { name: 'Needs approval', exact: true, checked: true })
            .waitFor()
          expect(await row(page, name).getAttribute('aria-busy')).toBe('false')
        }
        await page.reload()
        for (const name of ['first', 'second'])
          await row(page, name)
            .getByRole('radio', { name: 'Needs approval', exact: true, checked: true })
            .waitFor()
      })
    } finally {
      held.release()
    }
  })
})

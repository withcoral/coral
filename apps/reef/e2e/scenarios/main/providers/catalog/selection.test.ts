import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import { createWorkspace, publishWorkspaceProvider } from '../../../../support/providers.ts'

// Selection roll-up and filtered selection are workspace-providers-page.dom.test.tsx
// cases; this scenario keeps what needs a real layout.
test('A large catalog scrolls inside its table with aligned sticky rows', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const all = page.getByRole('checkbox', { name: 'Select all matching tools', exact: true })
    const checkbox = (name: string) =>
      page.getByRole('checkbox', { name: `Select ${name}`, exact: true })
    const filter = page.getByRole('searchbox', { name: 'Filter tools', exact: true })
    const table = page.getByRole('table', { name: 'Tools', exact: true })
    await step('Open a large catalog beside a small one', async () => {
      const workspace = await createWorkspace(actor.accessToken, `Selection ${crypto.randomUUID()}`)
      const entries = [
        ...Array.from(
          { length: 100 },
          (_, index) =>
            [
              `main${String(index).padStart(3, '0')}`,
              { tags: ['main'], summary: 'Matching set' },
            ] as const,
        ),
        ...Array.from(
          { length: 101 },
          (_, index) => [`large${String(index).padStart(3, '0')}`, { tags: ['large'] }] as const,
        ),
        ['extra', { tags: ['extra'] }] as const,
      ]
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Selection', Object.fromEntries(entries)),
        'selection',
      )
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Aside', { other: { tags: ['aside'] } }),
        'aside',
      )
      await page.goto(`/workspaces/${workspace}/providers`)
      await table.waitFor()
      expect(await all.isDisabled()).toBe(false)
      expect(await checkbox('large tools').isDisabled()).toBe(false)
    })
    await step('Keep scrolling inside the table with aligned sticky rows', async () => {
      await filter.fill('')
      const initialDocumentScroll = await page.evaluate(() => window.scrollY)
      const overflowingAncestors = await table.evaluate((element) => {
        const overflowing = []
        for (let ancestor = element.parentElement; ancestor; ancestor = ancestor.parentElement) {
          if (ancestor.scrollHeight > ancestor.clientHeight + 1) {
            overflowing.push({
              className: ancestor.className,
              height: ancestor.clientHeight,
              scrollHeight: ancestor.scrollHeight,
              overflow: getComputedStyle(ancestor).overflowY,
            })
          }
        }
        return overflowing
      })
      expect(overflowingAncestors).toEqual([])
      await table.evaluate((element) => {
        element.scrollTop = 600
      })
      const geometry = await table.evaluate((element) => {
        const header = element.querySelector('[role="row"]')!
        // The provider heading (level 1) sits under the header; a group heading
        // (level 2) sits under that. Both stay in view while their rows scroll.
        const visible = (row: Element) =>
          row.getBoundingClientRect().bottom > header.getBoundingClientRect().bottom
        const provider = [...element.querySelectorAll('[role="row"][aria-level="1"]')].find(
          visible,
        )!
        const group = [...element.querySelectorAll('[role="row"][aria-level="2"]')].find(visible)!
        const body = element.querySelector('[role="row"][aria-level="2"] + [role="row"]')!
        return {
          scroll: element.scrollTop,
          headerTop: header.getBoundingClientRect().top,
          tableTop: element.getBoundingClientRect().top,
          headerBottom: header.getBoundingClientRect().bottom,
          providerTop: provider.getBoundingClientRect().top,
          providerBottom: provider.getBoundingClientRect().bottom,
          groupTop: group.getBoundingClientRect().top,
          headerColumns: [...header.children].map((child) => child.getBoundingClientRect().left),
          bodyColumns: [...body.children].map((child) => child.getBoundingClientRect().left),
        }
      })
      expect(geometry.scroll).toBeGreaterThan(0)
      expect(Math.abs(geometry.headerTop - geometry.tableTop)).toBeLessThanOrEqual(2)
      expect(Math.abs(geometry.providerTop - geometry.headerBottom)).toBeLessThanOrEqual(2)
      expect(Math.abs(geometry.groupTop - geometry.providerBottom)).toBeLessThanOrEqual(2)
      geometry.headerColumns.forEach((left, index) =>
        expect(Math.abs(left - geometry.bodyColumns[index])).toBeLessThanOrEqual(1),
      )
      expect(await page.evaluate(() => window.scrollY)).toBe(initialDocumentScroll)
    })
    await step('Float the bulk form over the table without moving the table', async () => {
      const bulkForm = page.getByRole('form', { name: 'Bulk policy', exact: true })
      const before = (await table.boundingBox())!
      await all.check()
      await bulkForm.waitFor()
      const after = (await table.boundingBox())!
      expect(Math.abs(after.y - before.y)).toBeLessThanOrEqual(1)
      expect(Math.abs(after.height - before.height)).toBeLessThanOrEqual(1)
      const form = (await bulkForm.boundingBox())!
      expect(form.y).toBeGreaterThanOrEqual(after.y)
      expect(form.y + form.height).toBeLessThanOrEqual(after.y + after.height)
      await all.uncheck()
      await bulkForm.waitFor({ state: 'detached' })
    })
  })
})

/**
 * Opening the details drawer is a route change to a sibling route id, so a
 * remount of the tools tab would silently discard the operator's
 * selection. The bulk form and checked rows must survive opening, closing,
 * and history navigation.
 */
test('Opening a tool drawer preserves the table selection', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const table = page.getByRole('table', { name: 'Tools', exact: true })
    const checkbox = (name: string) =>
      page.getByRole('checkbox', { name: `Select ${name}`, exact: true })
    const details = (name: string) => page.getByRole('link', { name, exact: true })
    // The drawer is modal, so it hides the table from accessibility queries.
    // CSS locators still see it, unlike getByRole, and stay readable while open.
    // Only tool rows carry a details link, so this counts selected
    // tools without matching the header or group checkboxes.
    const checked = () =>
      page
        .locator('[role="table"] [role="row"]')
        .filter({ has: page.locator('input[type="checkbox"]:checked') })
        .filter({ has: page.locator('a[href*="/tools/"]') })
        .count()
    // The bulk form's Apply, while it counts this many selected tools.
    const bulkAction = (count: number) =>
      page
        .locator('form[aria-label="Bulk policy"]')
        .filter({ hasText: new RegExp(`(^|\\D)${count} selected`) })
        .locator('button', { hasText: /^Apply$/ })
    let provider = ''
    await step('Select tools in a provider catalog', async () => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Drawer selection ${crypto.randomUUID()}`,
      )
      const publication = await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Drawer selection', { first: {}, second: {}, third: {} }),
        'drawerselection',
      )
      // The drawer routes nest under the explicit provider path.
      provider = `/${publication.provider}`
      // The provider's tab leaves the Built-in Provider's tools out of
      // every selection.
      await page.goto(provider)
      await table.waitFor()
      await checkbox('first').check()
      await checkbox('second').check()
      expect(await checked()).toBe(2)
      await bulkAction(2).waitFor()
    })
    await step('Keep the selection while an unselected row opens its drawer', async () => {
      await details('third').click()
      await page.getByRole('dialog', { name: 'third', exact: true }).waitFor()
      await page.waitForURL((url) => url.pathname === `${provider}/tools/third`)
      expect(await checked()).toBe(2)
      expect(await bulkAction(2).count()).toBe(1)
      await page.keyboard.press('Escape')
      await page.waitForURL((url) => url.pathname === provider)
      await table.waitFor()
      expect(await checked()).toBe(2)
      expect(await checkbox('first').isChecked()).toBe(true)
      expect(await checkbox('second').isChecked()).toBe(true)
      await bulkAction(2).waitFor()
    })
    await step('Keep the selection when a selected row opens and closes its drawer', async () => {
      await details('first').click()
      const drawer = page.getByRole('dialog', { name: 'first', exact: true })
      await drawer.waitFor()
      expect(await checked()).toBe(2)
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      await page.waitForURL((url) => url.pathname === provider)
      await table.waitFor()
      expect(await checked()).toBe(2)
    })
    await step('Keep the selection when history navigation closes the drawer', async () => {
      await details('third').click()
      await page.getByRole('dialog', { name: 'third', exact: true }).waitFor()
      await page.goBack()
      await page.waitForURL((url) => url.pathname === provider)
      await table.waitFor()
      expect(await checked()).toBe(2)
      await page.goForward()
      await page.getByRole('dialog', { name: 'third', exact: true }).waitFor()
      expect(await checked()).toBe(2)
      await page.goBack()
      await table.waitFor()
    })
    await step('Keep a group selection while the drawer opens', async () => {
      await checkbox('first').uncheck()
      await checkbox('second').uncheck()
      await page.getByRole('checkbox', { name: 'Select all matching tools', exact: true }).check()
      expect(await checked()).toBe(3)
      await details('second').click()
      await page.getByRole('dialog', { name: 'second', exact: true }).waitFor()
      expect(await checked()).toBe(3)
      expect(await bulkAction(3).count()).toBe(1)
      await page.keyboard.press('Escape')
      await table.waitFor()
      expect(await checked()).toBe(3)
    })
  })
})

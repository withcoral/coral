import { sourceLifecycleHandlers } from './support/source-handlers'
import { expect, test } from './playwright.setup'

test('lists core sources by category, searches, and shows connected status', async ({
  network,
  page,
  review,
}) => {
  network.use(...sourceLifecycleHandlers())

  await review.chapter(
    'Open the sources page',
    'Render the bundled catalog with one installed source',
  )
  await page.goto('/#/sources')

  await expect(page.getByRole('heading', { name: 'Sources', level: 1 })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Observability' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Developer Tools' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Communication' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Project Management' })).toBeVisible()
  await expect(page.getByRole('button', { name: /Github/i })).toBeVisible()
  await expect(page.getByRole('button', { name: /Github/i }).getByText('Connected')).toBeVisible()
  await expect(page.getByRole('button', { name: /Linear/i })).toBeVisible()
  await expect(page.getByRole('button', { name: /Slack/i })).toBeVisible()
  await expect(page.getByRole('button', { name: /Sentry/i })).toBeVisible()
  await review.pause()

  await review.chapter('Filter the catalog', 'Type into the search box to filter to one card')
  await page.getByPlaceholder('Search sources…').fill('lin')

  await expect(page.getByRole('button', { name: /Linear/i })).toBeVisible()
  await expect(page.getByRole('button', { name: /Github/i })).toHaveCount(0)
  await expect(page.getByText('No sources match your search.')).toHaveCount(0)
  await review.pause()

  await review.chapter('Empty search state', 'Type a query that matches no source')
  await page.getByPlaceholder('Search sources…').fill('zzz-no-such-source')
  await expect(page.getByText('No sources match your search.')).toBeVisible()
  await review.pause()
})

test('installs a core source via paste, edits a binding, and removes it', async ({
  network,
  page,
  review,
}) => {
  network.use(...sourceLifecycleHandlers())

  await page.goto('/#/sources')
  await expect(page.getByRole('button', { name: /Linear/i })).toBeVisible()

  await review.chapter(
    'Open the install dialog',
    'Click the Linear card to open the paste install flow',
  )
  await page.getByRole('button', { name: /Linear/i }).click()

  const installDialog = page.getByRole('dialog', { name: /Linear/i })
  await expect(installDialog).toBeVisible()
  await expect(installDialog.getByRole('button', { name: 'Add source' })).toBeDisabled()
  await review.pause()

  await review.chapter('Paste a token and submit', 'Fill the secret input and click Add source')
  await installDialog.getByPlaceholder(/Linear api token/i).fill('lin_test_token')
  await expect(installDialog.getByRole('button', { name: 'Add source' })).toBeEnabled()
  await installDialog.getByRole('button', { name: 'Add source' }).click()

  await expect(page.getByText(`Installed linear`)).toBeVisible()
  await review.pause()

  await review.chapter(
    'Detail dialog opens with the form',
    'The install dialog hands off to the detail view for the just-installed source',
  )
  const detailDialog = page.getByRole('dialog', { name: /Linear/i })
  await expect(detailDialog.getByRole('heading', { name: 'Configuration' })).toBeVisible()
  await expect(detailDialog.getByText('LINEAR_API_TOKEN')).toBeVisible()
  await review.pause()

  await review.chapter(
    'Edit a stored secret in place',
    'Type a new value for the secret and save the change',
  )
  const secretInput = detailDialog.locator('input[type="password"]')
  await secretInput.fill('lin_test_token_v2')
  await detailDialog.getByRole('button', { name: 'Save changes' }).click()

  await expect(page.getByText('Updated linear')).toBeVisible()
  await expect(detailDialog.getByRole('button', { name: 'Close' })).toBeVisible()
  await review.pause()

  await review.chapter('Remove the source', 'Confirm the remove flow via the stacked confirm modal')
  await detailDialog.getByRole('button', { name: 'Remove' }).click()

  const confirmDialog = page.getByRole('dialog', { name: /Remove linear\?/ })
  await expect(confirmDialog).toBeVisible()
  await confirmDialog.getByRole('button', { name: 'Remove' }).click()

  await expect(page.getByText('Removed linear')).toBeVisible()
  await expect(page.getByRole('button', { name: /Linear/i })).toBeVisible()
  await review.pause()
})

test('cmd-F focuses the search input', async ({ network, page, review }) => {
  network.use(...sourceLifecycleHandlers())

  await page.goto('/#/sources')
  await expect(page.getByPlaceholder('Search sources…')).toBeVisible()

  await review.chapter('Press cmd/ctrl+F', 'Trigger the search-focus shortcut')
  await page.keyboard.press('ControlOrMeta+f')

  await expect(page.getByPlaceholder('Search sources…')).toBeFocused()
  await review.pause()
})

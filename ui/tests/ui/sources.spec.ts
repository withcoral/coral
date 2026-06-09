import { sourceLifecycleHandlers, sourceOAuthInstallHandlers } from './support/source-handlers'
import { expect, test } from './playwright.setup'

test('lists core sources by category, searches, and shows configured status', async ({
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
  await expect(page.getByRole('heading', { name: 'Configured' })).toBeVisible()
  const githubCard = page.getByRole('button', { name: /Github/i })
  await expect(githubCard).toBeVisible()
  await expect(githubCard.getByText('Configured')).toBeVisible()
  await expect(githubCard.getByText('Core')).toBeVisible()
  await expect(githubCard.getByText('v1.1.6')).toHaveCount(0)
  await expect(page.getByRole('heading', { name: 'Observability' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Communication' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Project Management' })).toBeVisible()
  await expect(page.getByRole('button', { name: /Linear/i })).toBeVisible()
  await expect(page.getByRole('button', { name: /Linear/i }).getByText('v2.2.0')).toHaveCount(0)
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

  await expect(page.getByText(`Configured linear`)).toBeVisible()
  await expect(page.getByText('Credentials were saved but not verified.')).toBeVisible()
  await expect(installDialog).toHaveCount(0)
  await review.pause()

  await review.chapter(
    'Open the configured source detail',
    'Click the now-configured Linear card to open the detail view',
  )
  await page.getByRole('button', { name: /Linear/i }).click()
  const detailDialog = page.getByRole('dialog', { name: /Linear/i })
  await expect(detailDialog.getByRole('heading', { name: 'Configuration' })).toBeVisible()
  await expect(detailDialog.getByText('LINEAR_API_TOKEN', { exact: true })).toBeVisible()
  await review.pause()

  await review.chapter(
    'Edit a stored secret in place',
    'Type a new value for the secret and save the change',
  )
  const secretInput = detailDialog.locator('input[type="password"]')
  await secretInput.fill('lin_test_token_v2')
  await detailDialog.getByRole('button', { name: 'Save changes' }).click()

  await expect(page.getByText('Updated linear')).toBeVisible()
  await expect(detailDialog).toHaveCount(0)
  await review.pause()

  await review.chapter('Remove the source', 'Confirm the remove flow inside the detail dialog')
  await page.getByRole('button', { name: /Linear/i }).click()
  const reopenedDetailDialog = page.getByRole('dialog', { name: /Linear/i })
  await expect(reopenedDetailDialog).toBeVisible()
  await reopenedDetailDialog.getByRole('button', { name: 'Remove' }).click()

  await expect(page.getByRole('dialog')).toHaveCount(1)
  await expect(reopenedDetailDialog.getByText('Remove linear?')).toBeVisible()
  await reopenedDetailDialog.getByRole('button', { name: 'Cancel' }).click()
  await expect(reopenedDetailDialog.getByText('Remove linear?')).toHaveCount(0)

  await reopenedDetailDialog.getByRole('button', { name: 'Remove' }).click()
  await expect(reopenedDetailDialog.getByText('Remove linear?')).toBeVisible()
  await reopenedDetailDialog.getByRole('button', { name: 'Remove' }).click()

  await expect(page.getByText('Removed linear')).toBeVisible()
  await expect(page.getByRole('button', { name: /Linear/i })).toBeVisible()
  await review.pause()
})

test('installed source detail uses manifest fields and masked secrets', async ({
  network,
  page,
  review,
}) => {
  network.use(...sourceLifecycleHandlers())

  await page.goto('/#/sources')

  await review.chapter(
    'Open an installed source',
    'CloudWatch Logs uses manifest defaults and configured secret inputs',
  )
  await page.getByRole('button', { name: /cloudwatch_logs/i }).click()

  const dialog = page.getByRole('dialog', { name: /cloudwatch_logs/i })
  await expect(dialog.getByRole('heading', { name: 'Configuration' })).toBeVisible()
  await expect(dialog.getByText('AWS_REGION', { exact: true })).toBeVisible()
  await expect(dialog.getByText('AWS_ACCESS_KEY_ID', { exact: true })).toBeVisible()
  await expect(
    dialog.getByText('AWS access key ID with CloudWatch Logs read permissions.'),
  ).toHaveCount(0)
  const variableInputs = dialog.locator('input[type="text"]')
  await expect(variableInputs.nth(0)).toHaveValue('us-east-1')
  await expect(variableInputs.nth(1)).toHaveValue('amazonaws.com')

  const secretInputs = dialog.locator('input[type="password"]')
  await expect(secretInputs).toHaveCount(2)
  await expect(secretInputs.first()).toHaveValue('••••••••')
  await review.pause()

  await review.chapter(
    'Replace a configured secret',
    'Focusing the masked secret clears the sentinel and allows a new value',
  )
  await secretInputs.first().focus()
  await expect(secretInputs.first()).toHaveValue('')
  await secretInputs.first().fill('AKIAUPDATED')
  await expect(dialog.getByRole('button', { name: 'Save changes' })).toBeVisible()
  await review.pause()
})

test('imported installed source detail uses effective source info', async ({
  network,
  page,
  review,
}) => {
  network.use(...sourceLifecycleHandlers())

  await page.goto('/#/sources')

  await review.chapter(
    'Open an imported installed source',
    'GitHub is configured as an imported source; detail loads its effective metadata',
  )
  await page.getByRole('button', { name: /Github/i }).click()

  const dialog = page.getByRole('dialog', { name: /Github/i })
  await expect(dialog.getByText('Imported', { exact: true })).toBeVisible()
  await expect(dialog.getByRole('heading', { name: 'Configuration' })).toBeVisible()
  await expect(dialog.getByText('GITHUB_API_BASE', { exact: true })).toBeVisible()
  await expect(dialog.getByText('GITHUB_TOKEN', { exact: true })).toBeVisible()
  const variableInputs = dialog.locator('input[type="text"]')
  await expect(variableInputs).toHaveCount(1)
  await expect(variableInputs.nth(0)).toHaveValue('https://api.github.com')
  const secretInputs = dialog.locator('input[type="password"]')
  await expect(secretInputs).toHaveCount(1)
  await expect(secretInputs.nth(0)).toHaveValue('••••••••')
  await expect(
    dialog.getByText("Imported sources can't be edited here yet", { exact: false }),
  ).toBeVisible()
  await expect(dialog.getByRole('button', { name: 'Save changes' })).toHaveCount(0)
  await review.pause()
})

test('installs GitHub through OAuth device code', async ({ network, page, review }) => {
  network.use(...sourceOAuthInstallHandlers())

  await page.goto('/#/sources')
  await page.getByRole('button', { name: /Github/i }).click()

  const dialog = page.getByRole('dialog', { name: /Github/i })
  await expect(dialog.getByRole('button', { name: 'Add source' })).toBeEnabled()

  await review.chapter(
    'Start device-code OAuth',
    'Click Add source and show the provider code while authorization is pending',
  )
  await dialog.getByRole('button', { name: 'Add source' }).click()

  await expect(dialog.getByText('ABCD-1234')).toBeVisible()
  const verificationLink = dialog.getByRole('link', { name: 'https://github.com/login/device' })
  await expect(verificationLink).toBeVisible()
  await expect(verificationLink).toHaveAttribute(
    'href',
    'https://github.com/login/device?user_code=ABCD-1234',
  )
  await review.pause()

  await expect(page.getByText('Configured github')).toBeVisible()
  await expect(page.getByText('Credentials were saved but not verified.')).toBeVisible()
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

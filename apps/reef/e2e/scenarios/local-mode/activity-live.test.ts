import { expect, test } from 'vitest'
import { withBrowserSession } from '../../support/browser-session.ts'
import { managementClients } from '../../support/management.ts'
import { openApiDocument } from '../../support/provider-fixtures.ts'
import { addProviderFromFile, programRunner, observeRun } from '../../support/providers.ts'
import { shownPayload } from '../../support/payloads.ts'

test('Local activity streams new records without a sign-in ceremony or a load button', async (context) => {
  const api = managementClients()
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Local live activity',
    requestId: crypto.randomUUID(),
  })
  await withBrowserSession(context, async ({ page, target, step }) => {
    const id = workspace.name.split('/')[1]
    const first = programRunner(page, target, id, { taskIntent: 'First local goal' })
    const accepted = await observeRun(first, await first.exec('return "local-live-value";'))
    expect((await first.endTask('success', 'Local completion 🪸')).isError).not.toBe(true)
    await step('Read exact retained output through local observation', async () => {
      const streams = Promise.all(
        ['count', 'observe'].map((name) =>
          page.waitForResponse(
            (response) => new URL(response.url()).pathname === `/activity/${name}`,
          ),
        ),
      )
      await page.goto(`/${workspace.name}/runs/${accepted.update.run_id}`)
      for (const response of await streams) {
        expect(response.status()).toBe(200)
        expect(response.headers()['content-type']).toContain('text/event-stream')
      }
      await shownPayload(page, 'local-live-value').waitFor()
      const second = programRunner(page, target, id, { taskIntent: 'Second local goal' })
      await observeRun(second, await second.exec('return 42;'))
      await page
        .getByRole('link', { name: 'Second local goal', exact: true, includeHidden: true })
        .waitFor({ timeout: 10_000 })
      expect(await page.getByRole('dialog').count()).toBe(1)
      expect(await page.getByText('Second local goal', { exact: true }).count()).toBe(1)
      await page.getByRole('button', { name: 'Close details', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/${workspace.name}/activity`)
      // The address changes first; the detail keeps its record while it exits.
      await page
        .getByRole('dialog', { name: 'Activity details', exact: true })
        .waitFor({ state: 'detached' })
      expect(await page.getByRole('button', { name: 'Load new', exact: true }).count()).toBe(0)
      await page.getByRole('link', { name: 'Second local goal', exact: true }).waitFor()
      expect(await page.content()).not.toContain('Local completion')
    })
    await step('Resolve local approvals and cancel a Run without a sign-in ceremony', async () => {
      await addProviderFromFile(
        page,
        id,
        openApiDocument('Local actions', { read: { summary: 'Read a value', method: 'post' } }, {}),
        'localActions',
      )
      const runner = programRunner(page, target, id, { waitMs: 1000 })
      for (const decision of ['deny', 'queue', 'cancel']) {
        const waiting = await runner.exec(
          'try { await coral.providers.localActions.read({}); } catch (error) { return error.code; }',
        )
        const run = `${workspace.name}/runs/${waiting.structuredContent.run_id}`
        await expect
          .poll(
            async () =>
              (await api.activity.inspectRun({ name: run, pageSize: 1 })).run?.actionPage?.actions
                .length,
          )
          .toBe(1)
        const observed = await runner.wait(waiting.structuredContent)
        const required = observed.structuredContent.required_actions[0]
        expect(new URL(required.run_status_url).pathname).toBe(`/${run}`)
        expect(Date.parse(required.deadline_at) - Date.now()).toBeGreaterThan(10 * 60_000)
        await page.goto(required.run_status_url)
        if (decision === 'queue') {
          // Decide from the queue row, without opening the review.
          await page.goto(`/${workspace.name}/activity`)
          await page
            .locator('[aria-label="Pending actions"]')
            .getByRole('button', { name: /^Deny: / })
            .click()
          await expect
            .poll(
              async () =>
                (await runner.wait(observed.structuredContent)).structuredContent.final_value,
            )
            .toEqual({ kind: 'value', value: 'approval_declined' })
        } else if (decision === 'deny') {
          await page.getByRole('link', { name: /^Review .*localActions\.read$/ }).click()
          await page.getByRole('button', { name: 'Deny this call', exact: true }).click()
          await page.getByText('Denied', { exact: true }).waitFor()
        } else {
          await page.getByRole('button', { name: 'Cancel run', exact: true }).click()
          await page
            .getByRole('dialog', { name: 'Cancel this run?' })
            .getByRole('button', { name: 'Cancel run', exact: true })
            .click()
          // Run owns a five-second shutdown grace before terminal publication.
          await page
            .getByRole('dialog', { name: 'Activity details' })
            .locator('header')
            .getByText('canceled', { exact: true })
            .waitFor({ timeout: 10_000 })
        }
        await expect
          .poll(
            async () =>
              (await api.activity.browseRequiredActions({ workspace: workspace.name })).actionCount,
          )
          .toBe(0n)
      }
    })
    await step('Read local completion feedback privately with its goal', async () => {
      await page.goto(
        `/${workspace.name}/activity?record=${encodeURIComponent(`${workspace.name}/tasks/${first.taskId()}`)}`,
      )
      await page.getByText('Local completion 🪸', { exact: true }).waitFor()
      // The header badge states the Task outcome above its feedback.
      await page
        .getByRole('dialog')
        .locator('header')
        .getByText('success', { exact: true })
        .waitFor()
      // The open detail leaves the list usable: another goal replaces it in place.
      await page.getByRole('link', { name: 'Second local goal', exact: true }).click()
      const detail = page.getByRole('dialog', { name: 'Activity details' })
      await detail.getByRole('heading', { name: 'Second local goal', exact: true }).waitFor()
      expect(await page.getByRole('dialog').count()).toBe(1)
      expect(await page.locator('dt:text-is("Feedback")').count()).toBe(0)
    })
  })
})

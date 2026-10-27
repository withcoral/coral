import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import {
  addProviderFromFile,
  breadcrumb,
  createWorkspace,
  observeRun,
  programRunner,
  setToolPolicy,
} from '../../../support/providers.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'

/**
 * Opens the recorded description's Details, which keep its source collapsed, and waits until
 * the recorded source that holds `marker` shows.
 */
async function openRecordedSource(page: Page, marker: string) {
  const details = page
    .getByRole('dialog')
    .locator('details')
    .filter({ has: page.locator('pre', { hasText: marker }) })
    .last()
  if ((await details.getAttribute('open')) === null)
    await details.locator('summary').first().click()
  await details.locator('pre').filter({ hasText: marker }).first().waitFor()
}

test('Activity keeps one hierarchy, exact retained evidence, and narrow-screen reading position', async (context) => {
  const actor = await loginActor(context, authenticatedTarget().authentication.owner)
  const api = managementClients(actor.accessToken)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      let first = ''
      const firstName = `Activity first ${crypto.randomUUID()}`
      let second = ''
      let task = ''
      let run = ''
      const intent = `Shared activity goal ${crypto.randomUUID()}`
      const original = 'export default function snapshot() { return "historical-source-249"; }'
      const replacement = 'export default function snapshot() { return "replacement-source-249"; }'
      await step('Land on Activity and keep workspace administration reachable', async () => {
        first = await createWorkspace(actor.accessToken, firstName)
        await page.goto('/')
        await breadcrumb(page, 'Activity').waitFor()
        // This Owner is shared, so home may open any of its Workspaces' Activity.
        const landing = new URL(page.url()).pathname
        expect(landing).toMatch(/^\/workspaces\/[^/]+\/activity$/)
        expect(
          await page
            .getByRole('navigation', { name: 'Coral', exact: true })
            .getByRole('link', {
              name: /^Activity(\. \d+ pending actions?)?$/,
            })
            .getAttribute('aria-current'),
        ).toBe('page')
        const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
        await nav.getByText('Workspaces', { exact: true }).waitFor()
        await nav.getByRole('button', { name: 'Create workspace', exact: true }).click()
        await page.getByRole('dialog', { name: 'Create workspace', exact: true }).waitFor()
        await page.keyboard.press('Escape')
        await page.getByRole('dialog').waitFor({ state: 'detached' })
        expect(new URL(page.url()).pathname).toBe(landing)
      })
      await step(
        'Record discovery and an accepted Run before changing the current function',
        async () => {
          const execute = programRunner(page, target, first, {
            accessToken: await mcpToken(page, first),
            taskIntent: intent,
          })
          expect(
            (await execute.saveFunction('history.snapshot', 'Retained history fixture', original))
              .isError,
          ).not.toBe(true)
          expect((await execute.describe('coral.functions.history.snapshot')).isError).not.toBe(
            true,
          )
          const observed = await observeRun(
            execute,
            await execute.exec(
              'for (let i = 0; i < 30; i++) coral.emit({i}); return coral.functions.history.snapshot();',
            ),
          )
          run = observed.update.run_id
          task = execute.taskId()
          expect(
            (
              await execute.saveFunction(
                'history.snapshot',
                'Changed current function',
                replacement,
              )
            ).isError,
          ).not.toBe(true)
          const rejected = await execute.exec('return { invalid syntax;')
          expect(rejected.isError).toBe(true)
          // Keep enough children to require real scrolling in the continuous history.
          for (let index = 0; index < 45; index++) {
            expect((await execute.describe('coral.functions.history.snapshot')).isError).not.toBe(
              true,
            )
          }
          expect((await execute.endTask('failure', 'No invoice export tool 🪸')).isError).not.toBe(
            true,
          )
          expect((await execute.endTask('success', 'Must not replace feedback')).isError).not.toBe(
            true,
          )
          second = await createWorkspace(
            actor.accessToken,
            `Activity second ${crypto.randomUUID()}`,
          )
          const other = programRunner(page, target, second, {
            accessToken: await mcpToken(page, second),
            taskIntent: intent,
          })
          await observeRun(other, await other.exec('return "second-workspace";'))
        },
      )
      await step(
        'Browse each workspace’s equal-intent goal separately without preloading its source',
        async () => {
          await page.goto(`/workspaces/${first}/activity`)
          await breadcrumb(page, 'Activity').waitFor()
          // Activity sits in its workspace, like the workspace's other pages.
          expect(
            await breadcrumb(page).getByRole('listitem', { includeHidden: true }).allInnerTexts(),
          ).toEqual([firstName, 'Activity'])
          expect(await page.getByRole('button', { name: 'Workspace scope' }).count()).toBe(0)
          const heading = await breadcrumb(page).boundingBox()
          const tableBounds = await page
            .getByRole('table', { name: 'Activity records', exact: true })
            .boundingBox()
          expect(tableBounds!.x).toBe(heading!.x)
          expect(tableBounds!.y).toBeGreaterThan(heading!.y + heading!.height)
          await page.getByRole('heading', { name: 'Your recent activity', exact: true }).waitFor()
          expect(
            await page.getByRole('navigation', { name: 'Activity scope', exact: true }).count(),
          ).toBe(0)
          expect(await page.getByRole('link', { name: /^Scope to / }).count()).toBe(0)
          expect(await page.getByRole('link', { name: 'all', exact: true }).count()).toBe(0)
          // The other workspace's goal has the same intent but belongs to its own Activity.
          expect(await page.getByRole('link', { name: intent, exact: true }).count()).toBe(1)
          expect(
            await page
              .getByRole('table', { name: 'Activity records' })
              .getByRole('columnheader')
              .allInnerTexts(),
          ).toEqual(['TIME · UTC', 'RECORD', 'DUR', 'CONTEXT'])
          expect(await page.content()).not.toContain('historical-source-249')
          expect(await page.content()).not.toContain('replacement-source-249')
          expect(await page.content()).not.toContain('No invoice export tool')
          await page.goto(`/workspaces/${second}/activity`)
          await breadcrumb(page, 'Activity').waitFor()
          expect(await page.getByRole('link', { name: intent, exact: true }).count()).toBe(1)
          expect(await page.content()).not.toContain('historical-source-249')
        },
      )
      await step('Read completion feedback only in exact goal detail', async () => {
        await page.goto(
          `/workspaces/${first}/activity?record=${encodeURIComponent(`workspaces/${first}/tasks/${task}`)}`,
        )
        await page.getByText('No invoice export tool 🪸', { exact: true }).waitFor()
        await page
          .getByRole('dialog')
          .locator('header')
          .getByText('failure', { exact: true })
          .waitFor()
        expect(await page.content()).not.toContain('Must not replace feedback')
        expect(
          await page
            .getByRole('table', { name: 'Activity records', includeHidden: true })
            .innerText(),
        ).not.toContain('No invoice export tool')
      })
      await step('Inspect the recorded description instead of today’s function', async () => {
        await page.goto(
          `/workspaces/${first}/activity?${new URLSearchParams({ expand: `workspaces/${first}/tasks/${task}` })}`,
        )
        await page.getByRole('link', { name: 'Inspect one Tool', exact: true }).first().click()
        await openRecordedSource(page, 'historical-source-249')
        expect(await page.content()).not.toContain('replacement-source-249')
        const rejectedRow = page
          .getByRole('row', { includeHidden: true })
          .and(page.getByTitle('rejected', { exact: true }))
        await rejectedRow.waitFor()
        expect(
          await page.getByRole('button', { name: 'Copy record link', exact: true }).count(),
        ).toBe(0)
      })
      await step(
        'Close detail and use browser navigation without losing the workspace or expansion',
        async () => {
          expect(new URL(page.url()).pathname).toBe(`/workspaces/${first}/activity`)
          expect(new URL(page.url()).searchParams.getAll('expand')).toContain(
            `workspaces/${first}/tasks/${task}`,
          )
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
          // The address changes first; the detail keeps its evidence while it exits.
          await page
            .getByRole('dialog', { name: 'Activity details', exact: true })
            .waitFor({ state: 'detached' })
          expect(await page.content()).not.toContain('historical-source-249')
          await page.goBack()
          await openRecordedSource(page, 'historical-source-249')
        },
      )
      await step(
        'Return from narrow detail to the same continuous history and scroll position',
        async () => {
          await page.setViewportSize({ width: 390, height: 844 })
          await page.goto(
            `/workspaces/${first}/activity?${new URLSearchParams({ expand: `workspaces/${first}/tasks/${task}` })}`,
          )
          const list = page.getByRole('table', {
            name: 'Activity records',
            exact: true,
            includeHidden: true,
          })
          expect(
            await page.getByRole('link', { name: /^(Next|Previous|First) records$/ }).count(),
          ).toBe(0)
          // The list starts at the top bar's gutter, where the drawer trigger opens it.
          const barStart = await page
            .getByRole('button', { name: 'Open navigation', exact: true })
            .boundingBox()
          expect((await list.boundingBox())!.x).toBe(barStart!.x)
          const overflowingAncestors = await list.evaluate((element) => {
            const overflowing = []
            for (
              let ancestor = element.parentElement;
              ancestor;
              ancestor = ancestor.parentElement
            ) {
              if (ancestor.scrollHeight > ancestor.clientHeight + 1) {
                overflowing.push(ancestor.tagName)
              }
            }
            return overflowing
          })
          expect(overflowingAncestors).toEqual([])
          await list.evaluate((element) => {
            element.scrollTop = element.scrollHeight
          })
          expect(await list.evaluate((element) => element.scrollTop)).toBeGreaterThan(0)
          const selectedLink = list
            .getByRole('link', { name: 'Inspect one Tool', exact: true })
            .last()
          const relativePosition = async () =>
            (await selectedLink.boundingBox())!.y - (await list.boundingBox())!.y
          const before = await relativePosition()
          await selectedLink.click()
          await openRecordedSource(page, 'replacement-source-249')
          await page.getByRole('dialog', { name: 'Activity details' }).waitFor()
          expect(await list.isVisible()).toBe(true)
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await list.waitFor({ state: 'visible' })
          await page.getByRole('dialog').waitFor({ state: 'hidden' })
          await expect.poll(relativePosition, { timeout: 10_000 }).toBeCloseTo(before, 0)
          expect(new URL(page.url()).searchParams.getAll('expand')).toContain(
            `workspaces/${first}/tasks/${task}`,
          )
          await page.setViewportSize({ width: 1440, height: 900 })
        },
      )
      await step('Reveal an exact emission beyond the first child page', async () => {
        const selected = `workspaces/${first}/runs/${run}/events/31`
        await page.goto(`/workspaces/${first}/activity?record=${encodeURIComponent(selected)}`)
        const table = page.getByRole('table', {
          name: 'Activity records',
          exact: true,
          includeHidden: true,
        })
        await table
          .getByRole('link', { name: 'emission #31', exact: true, includeHidden: true })
          .waitFor()
        await page.locator('pre').filter({ hasText: '29' }).first().waitFor()
        await page.goto(
          `/workspaces/${first}/activity?record=${encodeURIComponent(selected)}&tree=1`,
        )
        // Beside its Run's source, the emission leads back to history from its own
        // detail, and the drawer gives way to the list anchored at it.
        await page
          .getByRole('dialog', { name: 'Activity details' })
          .getByRole('region', { name: 'Selected record', exact: true })
          .getByRole('link', { name: 'View in history', exact: true })
          .click()
        await page.waitForURL(
          (url) => url.searchParams.get(`anchor.workspaces/${first}/runs/${run}`) === selected,
        )
        await page.getByRole('dialog').waitFor({ state: 'hidden' })
        // With no record selected, no row outside the page shows, so the link
        // below is the revealed page's own row.
        await page
          .getByText('Outside this workspace or page:', { exact: true })
          .waitFor({ state: 'detached' })
        await table.getByRole('link', { name: 'emission #31', exact: true }).waitFor()
        expect(new URL(page.url()).searchParams.has('scope')).toBe(false)
        expect(new URL(page.url()).searchParams.has('record')).toBe(false)
        expect(new URL(page.url()).searchParams.get('anchor')).toBe(
          `workspaces/${first}/tasks/${task}`,
        )
        expect(new URL(page.url()).searchParams.get(`anchor.workspaces/${first}/runs/${run}`)).toBe(
          selected,
        )
        await table
          .getByRole('rowgroup', { name: 'Call one Tool records', exact: true })
          .evaluate((element) => {
            const port = element.closest('[role=table]')!
            port.scrollTop +=
              element.getBoundingClientRect().top - port.getBoundingClientRect().top - 40
          })
        await table
          .getByRole('link', { name: 'emission #2', exact: true })
          .waitFor({ timeout: 10_000 })
        expect(
          await table.getByRole('link', { name: 'execution accepted', exact: true }).count(),
        ).toBe(0)
        expect(
          await table.getByRole('link', { name: 'execution ended', exact: true }).count(),
        ).toBe(0)
        await table.getByRole('img', { name: 'final value', exact: true }).waitFor()
        await table.getByRole('link', { name: 'final value', exact: true }).waitFor()
        const finalRow = table
          .getByRole('row')
          .filter({ has: page.getByRole('link', { name: 'final value', exact: true }) })
        await finalRow.getByLabel('23 B added', { exact: true }).waitFor()
        await finalRow.getByRole('link', { name: 'final value', exact: true }).click()
        await page.locator('pre').filter({ hasText: 'historical-source-249' }).first().waitFor()
        await page.getByRole('button', { name: 'Close details', exact: true }).click()
        await page.waitForURL((url) => !url.searchParams.has('record'))
      })
      await step(
        'Reveal a terminal Event without replacing its identity with the final value',
        async () => {
          const runName = `workspaces/${first}/runs/${run}`
          const finalPage = await api.activity.browseRunEntries({
            name: runName,
            anchor: `${runName}/final`,
            pageSize: 1,
          })
          const terminal = finalPage.entries[0]?.record
          if (terminal?.case !== 'event') throw new Error('Expected the terminal Event')
          const selected = `${runName}/events/${terminal.value.sequence}`
          const table = page.getByRole('table', {
            name: 'Activity records',
            exact: true,
            includeHidden: true,
          })
          for (const explicit of [false, true]) {
            await page.goto(
              `/workspaces/${first}/activity?record=${encodeURIComponent(selected)}${explicit ? '&tree=1' : ''}`,
            )
            if (explicit) {
              await page.getByText('Outside this workspace or page:', { exact: true }).waitFor()
              await page
                .getByRole('dialog', { name: 'Activity details' })
                .getByRole('region', { name: 'Selected record', exact: true })
                .getByRole('link', { name: 'View in history', exact: true })
                .click()
              await page.waitForURL(
                (url) =>
                  url.searchParams.get(`anchor.${runName}`) === selected &&
                  !url.searchParams.has('record'),
              )
              await page.getByRole('dialog').waitFor({ state: 'hidden' })
            }
            // Selected, the Event's own row; revealed, the one row the anchored page holds.
            const row = explicit
              ? table.getByRole('row').filter({
                  has: page.getByRole('link', { name: 'execution ended', exact: true }),
                })
              : table.locator('[role="row"][aria-selected="true"]')
            const eventLink = row.getByRole('link', {
              name: 'execution ended',
              exact: true,
              includeHidden: true,
            })
            await eventLink.waitFor()
            expect(
              new URL((await eventLink.getAttribute('href'))!, page.url()).searchParams.get(
                'record',
              ),
            ).toBe(selected)
            expect(await row.count()).toBe(1)
            expect(await row.getByLabel('23 B added', { exact: true }).count()).toBe(1)
            expect(
              await table
                .getByRole('link', { name: 'final value', exact: true, includeHidden: true })
                .count(),
            ).toBe(0)
            expect(
              await page.getByText('Outside this workspace or page:', { exact: true }).count(),
            ).toBe(0)
          }
          await page.waitForURL((url) => !url.searchParams.has('record'))
          await table.getByRole('link', { name: 'execution ended', exact: true }).waitFor()
        },
      )
      await step('Open a stable Run link in its workspace', async () => {
        await page.goto(`/workspaces/${first}/runs/${run}`)
        // A Run panel leads with its timing; it keeps no source or snapshots.
        const started = page.getByRole('dialog').getByText('Started', { exact: true })
        await started.waitFor()
        // A Run page sits in its workspace, like the workspace's other pages.
        expect(
          await page
            .getByRole('navigation', { name: 'breadcrumb', exact: true, includeHidden: true })
            .getByRole('listitem', { includeHidden: true })
            .allInnerTexts(),
        ).toEqual([firstName, 'Activity'])
        expect(await page.content()).not.toContain('replacement-source-249')
        await page.setViewportSize({ width: 390, height: 844 })
        await started.waitFor()
      })
      await step('Inspect a real Invocation without preloading its logical arguments', async () => {
        await page.setViewportSize({ width: 1440, height: 900 })
        const providerPath = await addProviderFromFile(
          page,
          first,
          openApiDocument(
            'History fixture',
            { lookup: { summary: 'Read one item' } },
            { server: 'https://history-fixture.invalid' },
          ),
          'historyFixture',
        )
        await page.goto(providerPath)
        await setToolPolicy(page, 'lookup', 'Disabled')
        const execute = programRunner(page, target, first, {
          accessToken: await mcpToken(page, first),
          taskIntent: `Inspect an invocation ${crypto.randomUUID()}`,
        })
        const observed = await observeRun(
          execute,
          await execute.exec(
            'try { await coral.providers.historyFixture.lookup({ marker: "exact-arguments-249" }); } catch (error) { return error.code; }',
          ),
        )
        expect(observed.update.final_value).toEqual({
          kind: 'value',
          value: 'tool_disabled',
        })
        const tree = new URLSearchParams()
        tree.append('expand', `workspaces/${first}/tasks/${execute.taskId()}`)
        tree.append('expand', `workspaces/${first}/runs/${observed.update.run_id}`)
        await page.goto(`/workspaces/${first}/activity?${tree}`)
        expect(await page.content()).not.toContain('exact-arguments-249')
        const toolLink = page
          .getByRole('table', { name: 'Activity records', exact: true })
          .getByRole('link', {
            name: 'coral.providers.historyFixture.lookup',
            exact: true,
          })
        const table = page.getByRole('table', { name: 'Activity records', exact: true })
        // The Run returns only the failure code, so its context is that code's JSON.
        const added = `${JSON.stringify('tool_disabled').length} B added`
        expect(await table.getByLabel(added, { exact: true }).count()).toBe(3)
        expect(await table.getByLabel('No net context change', { exact: true }).count()).toBe(1)
        await toolLink.click()
        // The one scalar argument shows under its own field, with its exact JSON spelling.
        // The program beside it spells the same token, so read the record's own detail.
        await page
          .getByRole('region', { name: 'Selected record', exact: true })
          .getByText('"exact-arguments-249"', { exact: true })
          .waitFor()
        await page.getByText('Coral failure: tool_disabled', { exact: true }).waitFor()
      })
      await step(
        'Keep Agent list context and retained history after removing its Registration',
        async () => {
          await page.setViewportSize({ width: 1440, height: 900 })
          const issued = await api.agents.createAgent({
            parent: `workspaces/${first}`,
            displayName: 'History worker',
            requestId: crypto.randomUUID(),
          })
          const name = issued.agent!.name
          const execute = programRunner(page, target, first, {
            accessToken: issued.credential,
            taskIntent: `Agent history ${crypto.randomUUID()}`,
          })
          const observed = await observeRun(
            execute,
            await execute.exec('return "retained-agent-output";'),
          )
          const runName = `workspaces/${first}/runs/${observed.update.run_id}`
          const taskName = `workspaces/${first}/tasks/${execute.taskId()}`
          await page.goto(`/${name}?expand=${encodeURIComponent(taskName)}`)
          await page.getByRole('link', { name: 'Call one Tool', exact: true }).click()
          await page.waitForURL((url) => url.searchParams.get('record') === runName)
          expect(new URL(page.url()).pathname).toBe(`/${name}`)
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
          await api.agents.deleteAgent({ name, etag: issued.agent!.etag })
          await page.reload()
          await page
            .getByText('This agent cannot start new work', {
              exact: true,
            })
            .waitFor()
          await page.getByRole('link', { name: 'Call one Tool', exact: true }).click()
          await page.locator('pre').filter({ hasText: 'retained-agent-output' }).first().waitFor()
          expect(
            await page
              .getByRole('navigation', { name: 'breadcrumb', exact: true, includeHidden: true })
              .getByRole('listitem', { includeHidden: true })
              .allInnerTexts(),
          ).toEqual([firstName, 'Agents', 'History worker'])
          expect(await page.content()).not.toContain(issued.credential)
          await page.goto(`/${runName}?tree=1`)
          await page.locator('pre').filter({ hasText: 'retained-agent-output' }).first().waitFor()
        },
      )
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})

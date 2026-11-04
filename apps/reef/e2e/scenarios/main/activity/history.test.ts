import type { Locator, Page } from 'playwright'
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
import { shownPayload } from '../../../support/payloads.ts'

/**
 * Waits until the selected discovery Run's return value, the description its Program read
 * with `coral.workspace.search` then, shows the recorded source that holds `marker`.
 */
async function openRecordedSource(page: Page, marker: string) {
  await shownPayload(
    page.getByRole('dialog').getByRole('region', { name: 'Return value', exact: true }),
    marker,
  ).waitFor()
}

/**
 * Selects the Event that `mark` names on its Run's row. The Events of a quick Run share one
 * point on the time axis, so the keyboard picks the named mark where a pointer could not.
 */
async function selectMark(mark: Locator) {
  await mark.press('Enter')
}

test('Activity keeps each workspace apart, opens exact retained evidence from a timeline, and keeps a removed Agent’s history', async (context) => {
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
      // An open rail can hide the page behind it from the accessibility tree.
      const list = page.getByRole('table', {
        name: 'Activity records',
        exact: true,
        includeHidden: true,
      })
      const timeline = page.getByRole('table', {
        name: 'Timeline records',
        exact: true,
        includeHidden: true,
      })
      await step('Land on Overview and keep workspace administration reachable', async () => {
        first = await createWorkspace(actor.accessToken, firstName)
        await page.goto('/')
        await breadcrumb(page, 'Overview').waitFor()
        // This Owner is shared, so home may open any of its Workspaces' Overview.
        const landing = new URL(page.url()).pathname
        expect(landing).toMatch(/^\/workspaces\/[^/]+$/)
        expect(
          await page
            .getByRole('navigation', { name: 'Coral', exact: true })
            .getByRole('link', { name: 'Overview', exact: true })
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
      const taskName = () => `workspaces/${first}/tasks/${task}`
      const runName = () => `workspaces/${first}/runs/${run}`
      await step(
        'Browse each workspace’s equal-intent goal separately without preloading its source',
        async () => {
          await page.goto(`/workspaces/${first}/activity`)
          await breadcrumb(page, 'Activity').waitFor()
          // Activity sits in its workspace, like the workspace's other pages.
          expect(
            await breadcrumb(page).getByRole('listitem', { includeHidden: true }).allInnerTexts(),
          ).toEqual([firstName, 'Activity'])
          // The other workspace's goal has the same intent but belongs to its own Activity.
          expect(await list.getByRole('link', { name: intent, exact: true }).count()).toBe(1)
          // With no root named, the newest opens beside the list, and the address names it.
          await page.waitForURL((url) => url.searchParams.get('root') === taskName())
          await timeline.getByRole('link', { name: 'Find tools', exact: true }).first().waitFor()
          expect(await page.content()).not.toContain('historical-source-249')
          expect(await page.content()).not.toContain('replacement-source-249')
          expect(await page.content()).not.toContain('No invoice export tool')
          await page.goto(`/workspaces/${second}/activity`)
          await breadcrumb(page, 'Activity').waitFor()
          expect(await list.getByRole('link', { name: intent, exact: true }).count()).toBe(1)
          expect(await page.content()).not.toContain('historical-source-249')
        },
      )
      await step('Read completion feedback only in exact goal detail', async () => {
        await page.goto(`/workspaces/${first}/activity?record=${encodeURIComponent(taskName())}`)
        await page.getByText('No invoice export tool 🪸', { exact: true }).waitFor()
        await page
          .getByRole('dialog')
          .locator('header')
          .getByText('failure', { exact: true })
          .waitFor()
        expect(await page.content()).not.toContain('Must not replace feedback')
        expect(await list.innerText()).not.toContain('No invoice export tool')
        expect(await timeline.innerText()).not.toContain('No invoice export tool')
      })
      await step(
        'Inspect the recorded description from the timeline instead of today’s function',
        async () => {
          await page.goto(
            `/workspaces/${first}/activity?${new URLSearchParams({ root: taskName() })}`,
          )
          await timeline.getByRole('link', { name: 'Find tools', exact: true }).first().click()
          await openRecordedSource(page, 'historical-source-249')
          expect(await page.content()).not.toContain('replacement-source-249')
          // The rejected submission keeps its own row, which names its outcome.
          await timeline
            .getByRole('row', { includeHidden: true })
            .and(page.getByTitle('rejected', { exact: true }))
            .waitFor()
        },
      )
      await step(
        'Close detail and use browser navigation without losing the workspace or its timeline',
        async () => {
          expect(new URL(page.url()).pathname).toBe(`/workspaces/${first}/activity`)
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
          expect(new URL(page.url()).searchParams.get('root')).toBe(taskName())
          // The address changes first; the detail keeps its evidence while it exits.
          await page
            .getByRole('dialog', { name: 'Activity details', exact: true })
            .waitFor({ state: 'detached' })
          expect(await page.content()).not.toContain('historical-source-249')
          await page.goBack()
          await openRecordedSource(page, 'historical-source-249')
        },
      )
      await step('Open an exact emission from its address, in its Task’s timeline', async () => {
        const selected = `${runName()}/events/31`
        await page.goto(`/workspaces/${first}/activity?record=${encodeURIComponent(selected)}`)
        // The Event's own Task opens around it, and the address names that Task.
        await page.waitForURL(
          (url) =>
            url.searchParams.get('root') === taskName() &&
            url.searchParams.get('record') === selected,
        )
        await timeline
          .getByRole('button', { name: /^emission #31 at /, includeHidden: true })
          .waitFor()
        await shownPayload(page, '29').waitFor()
        // The Run's return value is a mark on the same row. It shows the snapshot's own value.
        await selectMark(
          timeline.getByRole('button', { name: /^return value at /, includeHidden: true }),
        )
        await page.waitForURL((url) => url.searchParams.get('record') === `${runName()}/final`)
        await shownPayload(page, 'historical-source-249').waitFor()
        await page
          .getByRole('region', { name: 'Agent context', exact: true })
          .locator('div', { has: page.getByText('Return value sent', { exact: true }) })
          .getByText('23 B', { exact: true })
          .waitFor()
        await page.getByRole('button', { name: 'Close details', exact: true }).click()
        await page.waitForURL((url) => !url.searchParams.has('record'))
      })
      await step(
        'Open a terminal Event without replacing its identity with the final value',
        async () => {
          const finalPage = await api.activity.browseRunEntries({
            name: runName(),
            anchor: `${runName()}/final`,
            pageSize: 1,
          })
          const terminal = finalPage.entries[0]?.record
          if (terminal?.case !== 'event') throw new Error('Expected the terminal Event')
          const selected = `${runName()}/events/${terminal.value.sequence}`
          await page.goto(`/workspaces/${first}/activity?record=${encodeURIComponent(selected)}`)
          await page
            .getByRole('dialog')
            .getByRole('heading', { name: 'Program run ended: succeeded', exact: true })
            .waitFor()
          await page.waitForURL((url) => url.searchParams.get('root') === taskName())
          expect(new URL(page.url()).searchParams.get('record')).toBe(selected)
        },
      )
      await step('Open a stable Run link in its Task’s timeline', async () => {
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
        // The Run's Task opens beside it, with the Run's own row selected.
        await timeline
          .getByRole('row', { selected: true, includeHidden: true })
          .getByRole('link', { name: 'Call one Tool', exact: true, includeHidden: true })
          .waitFor()
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
        await page.goto(
          `/workspaces/${first}/activity?${new URLSearchParams({ root: `workspaces/${first}/tasks/${execute.taskId()}` })}`,
        )
        // The call shows under its Run, named by its provider and tool.
        const toolLink = timeline.getByRole('link', { name: 'History fixture lookup', exact: true })
        await toolLink.waitFor()
        expect(await page.content()).not.toContain('exact-arguments-249')
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
          const agentRun = `workspaces/${first}/runs/${observed.update.run_id}`
          const agentTask = `workspaces/${first}/tasks/${execute.taskId()}`
          await page.goto(`/${name}?${new URLSearchParams({ root: agentTask })}`)
          await timeline.getByRole('link', { name: 'Call one Tool', exact: true }).click()
          await page.waitForURL((url) => url.searchParams.get('record') === agentRun)
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
          await timeline.getByRole('link', { name: 'Call one Tool', exact: true }).click()
          await shownPayload(page, 'retained-agent-output').waitFor()
          expect(
            await page
              .getByRole('navigation', { name: 'breadcrumb', exact: true, includeHidden: true })
              .getByRole('listitem', { includeHidden: true })
              .allInnerTexts(),
          ).toEqual([firstName, 'Agents', 'History worker'])
          expect(await page.content()).not.toContain(issued.credential)
          await page.goto(`/${agentRun}`)
          await shownPayload(page, 'retained-agent-output').waitFor()
        },
      )
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})

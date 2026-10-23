import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { withOwnerSession, createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { shownArguments } from '../../../support/arguments.ts'
import { joinAsOwner, managementClients } from '../../../support/management.ts'
import {
  addProviderFromFile,
  breadcrumb,
  observeRun,
  programRunner,
} from '../../../support/providers.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import { OperationAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

test('Activity automatically streams new rows and continuously loads bounded child viewports without losing detail', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Continuous activity',
      requestId: crypto.randomUUID(),
    })
    const id = workspace.name.split('/')[1]
    const token = await mcpToken(page, id)
    let sequence = 0
    const tasks = new Map<string, ReturnType<typeof programRunner>>()
    async function arrival() {
      const runner = programRunner(page, target, id, {
        accessToken: token,
        taskIntent: `Live goal ${++sequence}`,
      })
      if (sequence === 1)
        expect(
          (
            await runner.saveFunction(
              'live.fixture',
              'Describe fixture',
              'export default () => 42;',
            )
          ).isError,
        ).not.toBe(true)
      expect((await runner.describe('coral.functions.live.fixture')).isError).not.toBe(true)
      tasks.set(`${workspace.name}/tasks/${runner.taskId()}`, runner)
      return runner
    }
    let latest!: ReturnType<typeof programRunner>
    for (let index = 0; index < 30; index++) latest = await arrival()
    const activityUrl = `/workspaces/${id}/activity`
    const table = page.getByRole('table', { name: 'Activity records', includeHidden: true })
    const records = () => table.locator('[data-record-key]')
    const names = () =>
      records().evaluateAll((elements) =>
        elements.map((element) => element.getAttribute('data-record-key')),
      )
    /** The first record on screen under the column heading, fully or, with `partly`, at all. */
    async function onScreen(partly = false) {
      const name = await table.evaluate((port, any) => {
        const top = port
          .querySelector('[role=rowgroup]:has([role=columnheader])')!
          .getBoundingClientRect().bottom
        const bottom = port.getBoundingClientRect().bottom
        return [...port.querySelectorAll('[data-record-key]')]
          .find((row) => {
            const box = row.getBoundingClientRect()
            return any
              ? box.bottom > top && box.top < bottom
              : box.top >= top && box.bottom <= bottom
          })
          ?.getAttribute('data-record-key')
      }, partly)
      return table.locator(`[data-record-key="${name}"]`)
    }
    // Away from the newest records, Coral keeps only the running records near the
    // viewport current, so the witness must be on screen.
    async function observedArrival(witness = records().first()) {
      const name = (await witness.getAttribute('data-record-key'))!
      const impact = witness.getByTitle(/Display values are rounded/)
      const before = await impact.getAttribute('title')
      latest = await arrival()
      // A retained row's changed context proves a post-arrival emission reached the reader.
      // Do not synchronize on a total count or an arbitrary polling delay.
      expect((await tasks.get(name)!.describe('coral.functions.live.fixture')).isError).not.toBe(
        true,
      )
      await expect.poll(() => impact.getAttribute('title'), { timeout: 10_000 }).not.toBe(before)
    }
    await step(
      'Show arrivals immediately at the global edge, with no Load new button',
      async () => {
        await page.setViewportSize({ width: 1440, height: 760 })
        await page.goto(activityUrl)
        await table.getByRole('link', { name: 'Live goal 30', exact: true }).waitFor()
        latest = await arrival()
        await table
          .getByRole('link', { name: 'Live goal 31', exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await table.evaluate((element) => element.scrollTop)).toBe(0)
        expect(await page.getByRole('button', { name: 'Load new', exact: true }).count()).toBe(0)
      },
    )
    await step(
      'Keep the loaded records while reading away from the newest ones, and load older ones as you scroll',
      async () => {
        const stopped = page.waitForRequest(
          (request) =>
            request.url().endsWith('/activity/observe') &&
            request.postDataJSON()?.window?.followRoot === false,
        )
        await table.hover()
        await page.mouse.wheel(0, 100)
        await stopped
        await expect.poll(() => table.evaluate((element) => element.scrollTop)).toBeGreaterThan(0)
        const original = await names()
        const before = (await records().last().boundingBox())!.y
        await observedArrival(await onScreen())
        expect(await names()).toEqual(original)
        expect(Math.abs((await records().last().boundingBox())!.y - before)).toBeLessThan(2)
        for (const name of ['Previous page', 'Next page'])
          expect(await page.getByRole('link', { name, exact: true }).count()).toBe(0)
        // Reaching the end loads the older records below, and the view stays where it is.
        await page.getByRole('button', { name: 'Load older records', exact: true }).waitFor()
        const last = table.locator(
          `[data-record-key="${await records().last().getAttribute('data-record-key')}"]`,
        )
        const reached = await last.evaluate((row) => {
          const port = row.closest('[role=table]')!
          port.scrollTop = port.scrollHeight
          return row.getBoundingClientRect().top
        })
        await expect.poll(() => records().count()).toBeGreaterThan(original.length)
        expect(
          Math.abs((await last.evaluate((row) => row.getBoundingClientRect().top)) - reached),
        ).toBeLessThan(2)
        const loaded = await names()
        expect(loaded.slice(0, original.length)).toEqual(original)
        expect(
          await page.getByRole('button', { name: 'Load older records', exact: true }).count(),
        ).toBe(0)
      },
    )
    await step(
      'Reopen a selected record around its anchor, then load newer records without moving it',
      async () => {
        const chosen = records().nth(-3)
        const name = (await chosen.getAttribute('data-record-key'))!
        await chosen.scrollIntoViewIfNeeded()
        // The URL anchors at the first record on screen, even one partly hidden.
        const anchor = (await (await onScreen(true)).getAttribute('data-record-key'))!
        await chosen.getByRole('link').first().click()
        await page.waitForURL((url) => url.searchParams.get('record') === name)
        expect(new URL(page.url()).searchParams.get('anchor')).toBe(anchor)
        await page.reload()
        const anchored = table.locator(`[data-record-key="${anchor}"]`)
        await anchored.waitFor()
        const offset = async () =>
          (await anchored.boundingBox())!.y - (await table.boundingBox())!.y
        const opening = await offset()
        await page.getByRole('button', { name: 'Close details', exact: true }).click()
        await page.waitForURL((url) => !url.searchParams.has('record'))
        // The newer records load above the anchor; the anchor stays the first record on screen.
        await expect.poll(async () => (await names()).indexOf(anchor)).toBeGreaterThan(0)
        expect(Math.abs((await offset()) - opening)).toBeLessThan(2)
        expect(await table.evaluate((element) => element.scrollTop)).toBeGreaterThan(0)
        // A link to a page from before continuous loading opens at that page's first record.
        await page.goto(`${activityUrl}?after=${encodeURIComponent(anchor)}`)
        await page.waitForURL(
          (url) => url.searchParams.has('anchor') && !url.searchParams.has('after'),
        )
        const opened = new URL(page.url()).searchParams.get('anchor')!
        await table.locator(`[data-record-key="${opened}"]`).waitFor()
        await expect
          .poll(async () => (await names()).indexOf(opened) - (await names()).indexOf(anchor))
          .toBe(1)
      },
    )
    let taskLabel = ''
    let task = ''
    let run = ''
    await step(
      'Expand an execution outside the first task window without fetching the whole history',
      async () => {
        for (let index = 0; index < 200; index++)
          await latest.describe('coral.functions.live.fixture')
        const observed = await observeRun(
          latest,
          await latest.exec('for (let i = 0; i < 300; i++) await coral.emit({i});'),
        )
        for (let index = 0; index < 150; index++)
          await latest.describe('coral.functions.live.fixture')
        taskLabel = `Live goal ${sequence}`
        task = `${workspace.name}/tasks/${latest.taskId()}`
        run = `${workspace.name}/runs/${observed.update.run_id}`
        const query = new URLSearchParams({ tree: '1' })
        query.append('expand', task)
        query.append('expand', run)
        await page.goto(`${activityUrl}?${query}`)
        await table.locator(`[data-record-key="${run}"]`).waitFor()
        expect(await records().count()).toBeLessThanOrEqual(226)
        expect(
          await page.getByRole('link', { name: /^(Next|Previous|First) records$/ }).count(),
        ).toBe(0)
        expect(await page.content()).not.toContain('"i":299')
      },
    )
    const taskSection = () =>
      table.getByRole('rowgroup', { name: `${taskLabel} records`, exact: true })
    const runSection = () =>
      table.getByRole('rowgroup', { name: 'Call one Operation records', exact: true })
    await step(
      'Scroll through later execution output and later task records without page controls',
      async () => {
        await table.locator(`[data-record-key="${run}"]`).scrollIntoViewIfNeeded()
        await runSection().evaluate((element) => {
          const port = element.closest('[role=table]')!
          port.scrollTop +=
            element.getBoundingClientRect().bottom -
            port.getBoundingClientRect().top -
            port.clientHeight +
            80
        })
        await table
          .getByRole('link', { name: 'emission #301', exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await records().count()).toBeLessThanOrEqual(226)
        await taskSection().evaluate((element) => {
          const port = element.closest('[role=table]')!
          port.scrollTop +=
            element.getBoundingClientRect().bottom -
            port.getBoundingClientRect().top -
            port.clientHeight
        })
        const newLabel = 'Prepare current work before offboarding'
        expect(
          (await latest.saveFunction('live.added', 'New child', 'export default () => 43;'))
            .isError,
        ).not.toBe(true)
        await taskSection()
          .getByRole('link', { name: newLabel, exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await table.locator(`[data-record-key="${run}"]`).count()).toBe(1)
        expect(await runSection().count()).toBe(1)
        expect(await records().count()).toBeLessThanOrEqual(226)
        // The expanded Run's row is a sticky heading, so scroll to the start of its output.
        await runSection().evaluate((element) => element.scrollIntoView({ block: 'start' }))
        await table
          .getByRole('link', { name: 'emission #2', exact: true })
          .waitFor({ timeout: 10_000 })
        await runSection().evaluate((element) => {
          const port = element.closest('[role=table]')!
          port.scrollTop +=
            element.getBoundingClientRect().bottom -
            port.getBoundingClientRect().top -
            port.clientHeight
        })
        await table
          .getByRole('link', { name: 'emission #301', exact: true })
          .waitFor({ timeout: 10_000 })
      },
    )
    await step(
      'Keep exact detail, mobile reading position and back/forward navigation during arrivals',
      async () => {
        await page.setViewportSize({ width: 390, height: 700 })
        const selected = `${run}/events/301`
        const link = table.getByRole('link', { name: 'emission #301', exact: true })
        await link.scrollIntoViewIfNeeded()
        const relativePosition = async () =>
          (await link.boundingBox())!.y - (await table.boundingBox())!.y
        const top = await relativePosition()
        expect(await table.evaluate((element) => element.scrollTop)).toBeGreaterThan(0)
        await link.click()
        await page.getByRole('dialog').waitFor()
        await observedArrival()
        expect(new URL(page.url()).searchParams.get('record')).toBe(selected)
        await page.locator('pre').filter({ hasText: '299' }).first().waitFor()
        await page.getByRole('button', { name: 'Close details', exact: true }).click()
        await page.waitForURL((url) => !url.searchParams.has('record'))
        // Back must reopen the detail, not find the one still exiting.
        await page
          .getByRole('dialog', { name: 'Activity details', exact: true })
          .waitFor({ state: 'detached' })
        await expect.poll(relativePosition, { timeout: 10_000 }).toBeCloseTo(top, 0)
        await page.goBack()
        await page.waitForURL((url) => url.searchParams.get('record') === selected)
        await page.locator('pre').filter({ hasText: '299' }).first().waitFor()
        await page.goForward()
        await page.waitForURL((url) => !url.searchParams.has('record'))
        await page.getByRole('dialog').waitFor({ state: 'hidden' })
        const before = await relativePosition()
        await page.context().setOffline(true)
        await page
          .getByText('Activity is unavailable. Reconnecting…', { exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await records().count()).toBe(0)
        await page.context().setOffline(false)
        await table
          .getByRole('link', { name: 'emission #301', exact: true })
          .waitFor({ timeout: 10_000 })
        await expect.poll(relativePosition, { timeout: 10_000 }).toBeCloseTo(before, 0)
        const anchor = new URL(page.url()).searchParams.get(`anchor.${run}`)
        expect(anchor).toBeTruthy()
        await page.reload()
        const anchored = table.locator(`[data-record-key="${anchor}"]`)
        await anchored.waitFor()
        await expect
          .poll(
            async () => {
              const rowBox = await anchored.boundingBox()
              const box = await table.boundingBox()
              return rowBox && box && rowBox.y >= box.y && rowBox.y < box.y + box.height
            },
            { timeout: 10_000 },
          )
          .toBe(true)
      },
    )
  })
})

test('Current Run and operation actions settle live while exact review stays selected for personal and Agent work', async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await api.workspaces.createWorkspace({
      displayName: 'Live actions',
      requestId: crypto.randomUUID(),
    })
    let calls = 0
    const upstream = await listen(
      context,
      createServer((_request, response) => {
        calls++
        response.writeHead(409, { 'content-type': 'application/json' }).end('{"native":"conflict"}')
      }),
    )
    const id = workspace.name.split('/')[1]
    const document = JSON.parse(
      openApiDocument(
        'Live gate',
        { lookup: { summary: 'Read an item' } },
        { server: upstream.origin },
      ).toString(),
    )
    document.paths['/lookup'].get.parameters = [
      { name: 'marker', in: 'query', required: true, schema: { type: 'string' } },
    ]
    const provider = (
      await addProviderFromFile(page, id, Buffer.from(JSON.stringify(document)), 'liveGate')
    ).slice(1)
    const activityUrl = `/workspaces/${id}/activity`
    /** This Workspace's Activity destination, named with its pending-action count. A modal
     * review hides the sidebar from the accessibility tree, so include hidden elements. */
    const activityLink = (count: bigint) =>
      page
        .getByRole('navigation', { name: 'Coral', exact: true, includeHidden: true })
        .getByRole('link', {
          name: count
            ? `Activity. ${count} pending ${count === 1n ? 'action' : 'actions'}`
            : 'Activity',
          exact: true,
          includeHidden: true,
        })
    const catalog = await api.providers.listOperations({ parent: provider })
    const operation = catalog.operations[0].name
    const issued = await api.agents.createAgent({
      parent: workspace.name,
      displayName: 'Live worker',
      requestId: crypto.randomUUID(),
    })
    const userToken = await mcpToken(page, id)
    for (const scope of ['personal', 'agent'] as const) {
      await step(`Observe ${scope} action state without dismissing exact review`, async () => {
        await api.policies.updateOperationPolicies({
          parent: provider,
          policies: [{ operation, accessState: OperationAccessState.APPROVAL_REQUIRED }],
        })
        const runner = programRunner(page, target, id, {
          accessToken: scope === 'personal' ? userToken : issued.credential,
          waitMs: 1,
          taskIntent: `Live ${scope} gate`,
        })
        const accepted = await runner.exec(
          'try { await coral.providers.liveGate.lookup({marker:"live-logical-arguments"}); } catch (error) { return error.code; }',
        )
        const run = `${workspace.name}/runs/${accepted.structuredContent.run_id}`
        await expect
          .poll(
            async () =>
              (await api.activity.inspectRun({ name: run, pageSize: 1 })).run?.requiresAction,
            { timeout: 10_000 },
          )
          .toBe(true)
        const entries = await api.activity.browseRunEntries({ name: run, pageSize: 25 })
        const invocation = entries.entries.find(
          (entry) => entry.record.case === 'invocation',
        )!.record
        if (invocation.case !== 'invocation') throw new Error('Expected a pending operation')
        const selected = invocation.value.name
        const query = new URLSearchParams({ tree: '1', record: selected })
        query.append('expand', `${workspace.name}/tasks/${runner.taskId()}`)
        query.append('expand', run)
        await page.goto(
          `${scope === 'personal' ? activityUrl : `/${issued.agent!.name.split('/').map(encodeURIComponent).join('/')}`}?${query}`,
        )
        const logical = { marker: 'live-logical-arguments' }
        expect(await shownArguments(page.getByRole('dialog'))).toEqual(logical)
        expect(await page.getByText('needs action', { exact: true }).count()).toBeGreaterThan(0)
        await page.locator('summary').filter({ hasText: 'Add a comment' }).click()
        await page.getByRole('textbox', { name: 'Comment (optional)' }).fill('Keep my review')
        await api.policies.updateOperationPolicies({
          parent: provider,
          policies: [{ operation, accessState: OperationAccessState.DISABLED }],
        })
        // The action leaves the queue as soon as the policy commits. The parked
        // call ends when its Run Owner's periodic precheck (every 15 s) finds the
        // Operation disabled.
        await expect
          .poll(() => page.getByText('needs action', { exact: true }).count(), { timeout: 10_000 })
          .toBe(0)
        await page
          .getByText('Coral failure: operation_disabled', { exact: true })
          .waitFor({ timeout: 25_000 })
        expect(new URL(page.url()).searchParams.get('record')).toBe(selected)
        expect(await page.getByRole('dialog').count()).toBe(1)
        expect(await shownArguments(page.getByRole('dialog'))).toEqual(logical)
        expect(
          await page.getByRole('button', { name: 'Approve this call', exact: true }).count(),
        ).toBe(0)
      })
      await step(`Find and review concurrent ${scope} actions in Activity`, async () => {
        await api.policies.updateOperationPolicies({
          parent: provider,
          policies: [{ operation, accessState: OperationAccessState.APPROVAL_REQUIRED }],
        })
        const runner = programRunner(page, target, id, {
          accessToken: scope === 'personal' ? userToken : issued.credential,
          waitMs: 1,
          taskIntent: `Concurrent ${scope} actions`,
        })
        const accepted = await runner.exec(
          // Long evidence exercises sticky review without relying on expanded diagnostics.
          `return await Promise.allSettled([coral.providers.liveGate.lookup({marker:"${'first '.repeat(240)}"}), coral.providers.liveGate.lookup({marker:"${'second '.repeat(240)}"})]);`,
        )
        const run = `${workspace.name}/runs/${accepted.structuredContent.run_id}`
        await expect
          .poll(
            async () =>
              (await api.activity.inspectRun({ name: run, pageSize: 1 })).run?.actionPage?.actions
                .length,
          )
          .toBe(2)
        // The badge counts this Workspace's actions on its own Activity destination.
        await page.goto(`/workspaces/${id}`)
        const workspaceCount = (
          await api.activity.countRequiredActions({ workspace: workspace.name })
        ).actionCount
        await activityLink(workspaceCount).waitFor()
        await activityLink(workspaceCount).click()
        // The Overview embeds the activity reader, so wait for Activity's own breadcrumb.
        await breadcrumb(page, 'Activity').waitFor()
        expect(new URL(page.url()).pathname).toBe(activityUrl)
        if (scope === 'personal') {
          const query = new URLSearchParams({ tree: '1' })
          query.append('expand', `${workspace.name}/tasks/${runner.taskId()}`)
          query.append('expand', run)
          await page.goto(`${activityUrl}?${query}`)
          const table = page.getByRole('table', { name: 'Activity records', exact: true })
          await table
            .getByRole('link', { name: 'coral.providers.liveGate.lookup', exact: true })
            .first()
            .click()
          await page.getByRole('dialog', { name: 'Review action', exact: true }).waitFor()
          await page.waitForURL((url) => Boolean(url.searchParams.get('request')))
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
          await table.getByRole('link', { name: 'Call one Operation', exact: true }).click()
          await page.getByRole('dialog', { name: 'Activity details', exact: true }).waitFor()
          expect(new URL(page.url()).searchParams.get('record')).toBe(run)
          expect(
            await page.getByRole('button', { name: 'Approve this call', exact: true }).count(),
          ).toBe(0)
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
          await page.getByRole('link', { name: 'Review', exact: true }).first().click()
          await page.getByRole('button', { name: 'Approve this call', exact: true }).waitFor()
          await page.waitForURL((url) =>
            Boolean(url.searchParams.get('record')?.includes('/invocations/')),
          )
        } else {
          await page.goto(activityUrl)
          await page.getByRole('link', { name: 'Review', exact: true }).first().click()
        }
        await page.getByRole('dialog', { name: 'Review action', exact: true }).waitFor()
        const history = page.getByRole('table', {
          name: 'Activity records',
          exact: true,
          includeHidden: true,
        })
        await history
          .getByRole('rowgroup', {
            name: `Concurrent ${scope} actions records`,
            exact: true,
            includeHidden: true,
          })
          .getByRole('link', { name: 'Call one Operation', exact: true, includeHidden: true })
          .waitFor()
        await history
          .getByRole('rowgroup', {
            name: 'Call one Operation records',
            exact: true,
            includeHidden: true,
          })
          .getByRole('link', {
            name: 'coral.providers.liveGate.lookup',
            exact: true,
            includeHidden: true,
          })
          .first()
          .waitFor()
        expect(
          await page.getByText('Outside this workspace or page:', { exact: true }).count(),
        ).toBe(0)
        await page.waitForURL((url) =>
          Boolean(url.searchParams.get('record')?.includes('/invocations/')),
        )
        const selectedOperation = new URL(page.url()).searchParams.get('record')!
        const selectedRow = history.locator('[role="row"][aria-selected="true"]')
        expect(await selectedRow.getAttribute('data-record-key')).toBe(selectedOperation)
        expect(await selectedRow.getByText('needs action', { exact: true }).count()).toBe(1)
        expect(
          await history
            .locator(`[data-record-key="${run}"]`)
            .getByText('needs action', { exact: true })
            .count(),
        ).toBe(0)
        await page.locator('summary').filter({ hasText: 'Add a comment' }).click()
        const comment = page.getByRole('textbox', { name: 'Comment (optional)' })
        const selectedRequest = new URL(page.url()).searchParams.get('request')
        await comment.fill('Reviewed once')
        // Observe a real subscription emission before deciding. It must not reset the form.
        const arrival = await runner.exec(
          'return await coral.providers.liveGate.lookup({marker:"arrival"});',
        )
        await activityLink(workspaceCount + 1n).waitFor()
        await api.activity.cancelActivityRun({
          name: `${workspace.name}/runs/${arrival.structuredContent.run_id}`,
        })
        await activityLink(workspaceCount).waitFor()
        expect(await comment.inputValue()).toBe('Reviewed once')
        // The two approvals the Run waits on, read from Coral before you decide.
        const waiting = (
          await api.activity.inspectRun({ name: run, pageSize: 1 })
        ).run!.actionPage!.actions.map(({ request }) => {
          if (request?.request.case !== 'approval') throw new Error('Expected an approval')
          return request.request.value
        })
        expect(waiting).toHaveLength(2)
        expect(waiting).toContain(selectedRequest)
        const [decided, remaining] = await Promise.all(
          [selectedRequest!, waiting.find((name) => name !== selectedRequest)!].map((name) =>
            api.activity.inspectApproval({ name }),
          ),
        )
        expect(decided.invocation).toBe(selectedOperation)
        expect(remaining.invocation).not.toBe(selectedOperation)
        const before = calls
        await page.getByRole('button', { name: 'Approve this call', exact: true }).click()
        // Your decision wins, so the review advances to the approval the Run waits on now.
        await page.waitForURL((url) => url.searchParams.get('request') === remaining.name)
        expect(new URL(page.url()).searchParams.get('record')).toBe(remaining.invocation)
        await expect
          .poll(() => selectedRow.getAttribute('data-record-key'))
          .toBe(remaining.invocation)
        await page.getByRole('dialog', { name: 'Review action', exact: true }).waitFor()
        await page.getByRole('button', { name: 'Approve this call', exact: true }).waitFor()
        // The decision keeps the exact comment as its Comment fact.
        const approved = await api.activity.inspectApproval({ name: decided.name })
        expect(approved.state).toBe('approved')
        expect(approved.comment).toBe('Reviewed once')
        await page.setViewportSize({ width: 1440, height: 900 })
        await page.getByRole('button', { name: 'Deny this call', exact: true }).click()
        await expect
          .poll(async () => (await api.activity.inspectApproval({ name: remaining.name })).state)
          .toBe('declined')
        await expect.poll(() => calls).toBe(before + 1)
        await expect
          .poll(
            async () =>
              (await api.activity.browseRequiredActions({ workspace: workspace.name })).actionCount,
          )
          .toBe(0n)
        // The live page settles: nothing waits on you, and no review offers a decision.
        await expect.poll(() => page.locator('[aria-label="Pending actions"]').count()).toBe(0)
        await expect
          .poll(() => page.getByRole('button', { name: 'Approve this call', exact: true }).count())
          .toBe(0)
        // Each settled review, opened on its own, shows its decision. Without a decision of
        // yours there, the review stays where you opened it. Only the approval carries the
        // exact comment, once, as its Comment fact.
        for (const [approval, label, comments] of [
          [decided, 'Approved', 1],
          [remaining, 'Denied', 0],
        ] as const) {
          const review = new URLSearchParams({
            tree: '1',
            record: approval.invocation,
            request: approval.name,
          })
          review.append('expand', `${workspace.name}/tasks/${runner.taskId()}`)
          review.append('expand', run)
          await page.goto(`${activityUrl}?${review}`)
          const dialog = page.getByRole('dialog')
          await dialog.getByText(label, { exact: true }).waitFor()
          expect(new URL(page.url()).searchParams.get('request')).toBe(approval.name)
          expect(new URL(page.url()).searchParams.get('record')).toBe(approval.invocation)
          expect(await selectedRow.getAttribute('data-record-key')).toBe(approval.invocation)
          expect(
            await page.getByRole('button', { name: 'Approve this call', exact: true }).count(),
          ).toBe(0)
          const commentFact = dialog
            .locator('dl > div')
            .filter({ has: page.locator('dt', { hasText: /^Comment$/ }) })
            .locator('dd', { hasText: /^Reviewed once$/ })
          await expect.poll(() => commentFact.count()).toBe(comments)
          expect(await dialog.getByText('Reviewed once', { exact: true }).count()).toBe(comments)
        }
        await expect.poll(() => page.locator('[aria-label="Pending actions"]').count()).toBe(0)
        expect(
          await page.getByRole('table', { name: 'Activity records', includeHidden: true }).count(),
        ).toBe(1)
        expect(await page.locator('[aria-label="Pending actions"]').count()).toBe(0)
        expect(
          await page.getByRole('button', { name: 'Approve this call', exact: true }).count(),
        ).toBe(0)
        const ended = await observeRun(runner, accepted)
        expect(ended.update.state).toBe('succeeded')
        await page.setViewportSize({ width: 390, height: 844 })
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
          true,
        )
        await page.setViewportSize({ width: 1440, height: 900 })
      })
    }
  })
})

test('Navigation never restores revoked workspace summaries while observation is stalled', async (context) => {
  const { owner, alice } = await createActors(context)
  const ownerApi = managementClients(owner.accessToken)
  const api = managementClients(alice.accessToken)
  const privateWorkspace = await api.workspaces.createWorkspace({
    displayName: 'Revoked activity',
    requestId: crypto.randomUUID(),
  })
  const availableWorkspace = await api.workspaces.createWorkspace({
    displayName: 'Available activity',
    requestId: crypto.randomUUID(),
  })
  await joinAsOwner(ownerApi, privateWorkspace.name, owner.userId)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      const privateRunner = programRunner(page, target, privateWorkspace.name.split('/')[1], {
        taskIntent: 'Revoked private goal',
      })
      const availableRunner = programRunner(page, target, availableWorkspace.name.split('/')[1], {
        taskIntent: 'Available personal goal',
      })
      await observeRun(privateRunner, await privateRunner.exec('return "private";'))
      await observeRun(availableRunner, await availableRunner.exec('return "available";'))
      let resume!: () => void
      const stalled = new Promise<void>((resolve) => {
        resume = resolve
      })
      // Delay transport only. Both the initial and navigation loaders use real current authority.
      await page.route('**/activity/observe', async (route) => {
        await stalled
        await route.continue().catch(() => {
          /* Navigation can cancel a held observation. */
        })
      })
      try {
        await step('Read the workspace before the first observation response', async () => {
          await page.goto(`/${privateWorkspace.name}/activity`)
          await page.getByRole('link', { name: 'Revoked private goal', exact: true }).waitFor()
          expect(
            await page.getByRole('link', { name: 'Available personal goal', exact: true }).count(),
          ).toBe(0)
        })
        await step('Reauthorize navigation after losing the workspace', async () => {
          const membership = await ownerApi.memberships.getWorkspaceMembership({
            name: `${privateWorkspace.name}/memberships/${alice.userId}`,
          })
          await ownerApi.memberships.deleteWorkspaceMembership({
            name: membership.name,
            etag: membership.etag,
          })
          // The stalled observation still shows the old row; navigating must not restore it.
          await page.getByRole('link', { name: 'Revoked private goal', exact: true }).click()
          await page.waitForURL((url) => url.searchParams.has('record'))
          await expect
            .poll(() =>
              page
                .getByRole('link', {
                  name: 'Revoked private goal',
                  exact: true,
                  includeHidden: true,
                })
                .count(),
            )
            .toBe(0)
          expect(
            await page
              .getByRole('navigation', { name: 'Coral', exact: true })
              .getByRole('button', { name: 'Revoked activity', exact: true })
              .count(),
          ).toBe(0)
        })
        await step('Keep the other workspace available', async () => {
          await page.goto(`/${availableWorkspace.name}/activity`)
          await page.getByRole('link', { name: 'Available personal goal', exact: true }).waitFor()
          expect(
            await page
              .getByRole('link', { name: 'Revoked private goal', exact: true, includeHidden: true })
              .count(),
          ).toBe(0)
          expect(
            await page
              .getByRole('table', { name: 'Activity records', includeHidden: true })
              .locator('[data-record-key]')
              .count(),
          ).toBe(1)
        })
      } finally {
        resume()
        await page.unrouteAll({ behavior: 'wait' })
      }
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})

test('Live authority loss conceals Agent history and exact snapshots without granting Agent browsing', async (context) => {
  const { owner, alice } = await createActors(context)
  const ownerApi = managementClients(owner.accessToken)
  const api = managementClients(alice.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Live authority',
    requestId: crypto.randomUUID(),
  })
  await joinAsOwner(ownerApi, workspace.name, owner.userId)
  const issued = await api.agents.createAgent({
    parent: workspace.name,
    displayName: 'Private worker',
    requestId: crypto.randomUUID(),
  })
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      const runner = programRunner(page, target, workspace.name.split('/')[1], {
        accessToken: issued.credential,
        taskIntent: 'Agent-only goal',
      })
      const observed = await observeRun(runner, await runner.exec('return "live-private-source";'))
      const run = `${workspace.name}/runs/${observed.update.run_id}`
      await step('Observe Agent work as its effective Workspace Owner', async () => {
        await page.goto(
          `/${issued.agent!.name.split('/').map(encodeURIComponent).join('/')}?record=${encodeURIComponent(run)}`,
        )
        await page.locator('pre').filter({ hasText: 'live-private-source' }).first().waitFor()
        await expect(
          managementClients(issued.credential).activity.browseActivityRoots({
            scope: { workspace: workspace.name, agent: issued.agent!.name },
          }),
        ).rejects.toBeDefined()
        await page.context().setOffline(true)
        await page
          .getByText('Activity is unavailable. Reconnecting…', { exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await page.locator('pre').filter({ hasText: 'live-private-source' }).count()).toBe(0)
        await page.context().setOffline(false)
        await page
          .locator('pre')
          .filter({ hasText: 'live-private-source' })
          .first()
          .waitFor({ timeout: 10_000 })
        expect(new URL(page.url()).searchParams.get('record')).toBe(run)
      })
      await step(
        'Demotion removes rows, exact source and surrounding Agent management without a reload',
        async () => {
          const membership = await ownerApi.memberships.getWorkspaceMembership({
            name: `${workspace.name}/memberships/${alice.userId}`,
          })
          await ownerApi.memberships.updateWorkspaceMembership({
            membership: {
              name: membership.name,
              etag: membership.etag,
              role: WorkspaceRole.MEMBER,
            },
            updateMask: { paths: ['role'] },
          })
          await page.getByText('You cannot inspect this agent', { exact: true }).waitFor()
          await expect
            .poll(() => page.locator('pre').filter({ hasText: 'live-private-source' }).count())
            .toBe(0)
          expect(await page.getByText('Agent-only goal', { exact: true }).count()).toBe(0)
          expect(
            await page.getByRole('heading', { name: 'Private worker', exact: true }).count(),
          ).toBe(0)
          expect(
            await page
              .getByRole('table', { name: 'Activity records', includeHidden: true })
              .locator('[data-record-key]')
              .count(),
          ).toBe(0)
          await page.getByText('This history is unavailable.', { exact: true }).waitFor()
        },
      )
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})

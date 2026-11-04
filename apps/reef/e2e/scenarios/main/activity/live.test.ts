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
import { shownPayload } from '../../../support/payloads.ts'
import { ToolAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

test('Activity streams new roots and the open timeline’s records, and keeps your place in the list', async (context) => {
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
    // An open rail can hide the page behind it from the accessibility tree.
    const timelineRegion = page.getByRole('region', {
      name: 'Timeline',
      exact: true,
      includeHidden: true,
    })
    const timeline = page.getByRole('table', { name: 'Timeline records', includeHidden: true })
    /** The open timeline's row of its one Run, named by the intent its submission gave it. */
    const runRow = () =>
      timeline.getByRole('row', { includeHidden: true }).filter({
        has: page.getByRole('link', { name: 'Call one Tool', exact: true, includeHidden: true }),
      })
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
      'Reopen an older timeline around its list anchor, then load newer records without moving it',
      async () => {
        const chosen = records().nth(-3)
        const name = (await chosen.getAttribute('data-record-key'))!
        await chosen.scrollIntoViewIfNeeded()
        // The URL anchors at the first record on screen, even one partly hidden.
        const anchor = (await (await onScreen(true)).getAttribute('data-record-key'))!
        // The row opens its timeline, not its detail.
        await chosen.getByRole('link').first().click()
        await page.waitForURL((url) => url.searchParams.get('root') === name)
        expect(new URL(page.url()).searchParams.get('anchor')).toBe(anchor)
        expect(new URL(page.url()).searchParams.has('record')).toBe(false)
        await page.reload()
        const anchored = table.locator(`[data-record-key="${anchor}"]`)
        await anchored.waitFor()
        await timeline.waitFor()
        const offset = async () =>
          (await anchored.boundingBox())!.y - (await table.boundingBox())!.y
        const opening = await offset()
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
    await step('Watch the open timeline gain records while its Task goes on', async () => {
      taskLabel = `Live goal ${sequence}`
      task = `${workspace.name}/tasks/${latest.taskId()}`
      await page.goto(`${activityUrl}?${new URLSearchParams({ root: task })}`)
      await timelineRegion.locator('header').getByText(taskLabel, { exact: true }).waitFor()
      await timeline.getByRole('link', { name: 'Find tools', exact: true }).first().waitFor()
      const saved = timeline.getByRole('link', {
        name: 'Prepare current work before offboarding',
        exact: true,
      })
      expect(await saved.count()).toBe(0)
      expect(
        (await latest.saveFunction('live.added', 'New child', 'export default () => 43;')).isError,
      ).not.toBe(true)
      await saved.waitFor({ timeout: 10_000 })
      const observed = await observeRun(
        latest,
        await latest.exec(
          'for (let i = 0; i < 5; i++) await coral.emit({ marker: `live-mark-${i}` });',
        ),
      )
      run = `${workspace.name}/runs/${observed.update.run_id}`
      // The Run arrives with its Events as marks on its own row.
      await runRow()
        .getByRole('button', { name: /^emission #6 at / })
        .waitFor({ timeout: 10_000 })
      expect(new URL(page.url()).searchParams.get('root')).toBe(task)
    })
    await step(
      'Keep the selected Event and its timeline through arrivals, back/forward and a lost connection',
      async () => {
        const selected = `${run}/events/6`
        await runRow()
          .getByRole('button', { name: /^emission #6 at / })
          .press('Enter')
        await page.waitForURL((url) => url.searchParams.get('record') === selected)
        await shownPayload(page, 'live-mark-4').waitFor()
        await observedArrival()
        expect(new URL(page.url()).searchParams.get('record')).toBe(selected)
        // A live update reads the open root again, never the newer one that arrived.
        expect(new URL(page.url()).searchParams.get('root')).toBe(task)
        await timelineRegion.locator('header').getByText(taskLabel, { exact: true }).waitFor()
        await shownPayload(page, 'live-mark-4').waitFor()
        await page.getByRole('button', { name: 'Close details', exact: true }).click()
        await page.waitForURL((url) => !url.searchParams.has('record'))
        // Back must reopen the detail, not find the one still exiting.
        await page
          .getByRole('dialog', { name: 'Activity details', exact: true })
          .waitFor({ state: 'detached' })
        await page.goBack()
        await page.waitForURL((url) => url.searchParams.get('record') === selected)
        await shownPayload(page, 'live-mark-4').waitFor()
        await page.goForward()
        await page.waitForURL((url) => !url.searchParams.has('record'))
        await page.getByRole('dialog').waitFor({ state: 'hidden' })
        await page.context().setOffline(true)
        await page
          .getByText('Activity is unavailable. Reconnecting…', { exact: true })
          .waitFor({ timeout: 10_000 })
        expect(await records().count()).toBe(0)
        await expect.poll(() => timeline.count()).toBe(0)
        await page.context().setOffline(false)
        await runRow()
          .getByRole('button', { name: /^emission #6 at / })
          .waitFor({ timeout: 10_000 })
        expect(new URL(page.url()).searchParams.get('root')).toBe(task)
      },
    )
  })
})

test('Current Run and tool actions settle live while exact review stays selected for personal and Agent work', async (context) => {
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
    /** This Workspace's Approvals destination, named with its pending-approval count. A modal
     * review hides the sidebar from the accessibility tree, so include hidden elements. */
    const approvalsLink = (count: bigint) =>
      page
        .getByRole('navigation', { name: 'Coral', exact: true, includeHidden: true })
        .getByRole('link', {
          name: count
            ? `Approvals. ${count} pending ${count === 1n ? 'approval' : 'approvals'}`
            : 'Approvals',
          exact: true,
          includeHidden: true,
        })
    const catalog = await api.providers.listTools({ parent: provider })
    const tool = catalog.tools[0].name
    const issued = await api.agents.createAgent({
      parent: workspace.name,
      displayName: 'Live worker',
      requestId: crypto.randomUUID(),
    })
    const userToken = await mcpToken(page, id)
    for (const scope of ['personal', 'agent'] as const) {
      await step(`Observe ${scope} action state without dismissing exact review`, async () => {
        await api.policies.updateToolPolicies({
          parent: provider,
          policies: [{ tool, accessState: ToolAccessState.APPROVAL_REQUIRED }],
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
        if (invocation.case !== 'invocation') throw new Error('Expected a pending tool')
        const selected = invocation.value.name
        const query = new URLSearchParams({ record: selected })
        await page.goto(
          `${scope === 'personal' ? activityUrl : `/${issued.agent!.name.split('/').map(encodeURIComponent).join('/')}`}?${query}`,
        )
        const logical = { marker: 'live-logical-arguments' }
        expect(await shownArguments(page.getByRole('dialog'))).toEqual(logical)
        await page.locator('summary').filter({ hasText: 'Add a comment' }).click()
        await page.getByRole('textbox', { name: 'Comment (optional)' }).fill('Keep my review')
        await api.policies.updateToolPolicies({
          parent: provider,
          policies: [{ tool, accessState: ToolAccessState.DISABLED }],
        })
        // The policy alone ends the parked call at its Run Owner's next periodic
        // precheck, up to 15 s later. An approval from elsewhere wakes the call now,
        // and the Policy Enforcement Point still refuses the disabled Tool.
        const { requests } = await api.activity.inspectInvocation({ name: selected })
        const approval = requests.find(({ request }) => request.case === 'approval')?.request
        if (approval?.case !== 'approval') throw new Error('Expected an approval')
        await api.activity.decideApproval({ name: approval.value, decision: 'approve' })
        await page
          .getByText('Coral failure: tool_disabled', { exact: true })
          .waitFor({ timeout: 10_000 })
        expect(new URL(page.url()).searchParams.get('record')).toBe(selected)
        expect(await page.getByRole('dialog').count()).toBe(1)
        expect(await shownArguments(page.getByRole('dialog'))).toEqual(logical)
        expect(await page.getByRole('button', { name: 'Approve', exact: true }).count()).toBe(0)
      })
      await step(`Find and review concurrent ${scope} actions in Activity`, async () => {
        await api.policies.updateToolPolicies({
          parent: provider,
          policies: [{ tool, accessState: ToolAccessState.APPROVAL_REQUIRED }],
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
        const task = `${workspace.name}/tasks/${runner.taskId()}`
        // An open review can hide the page behind it from the accessibility tree.
        const timeline = page.getByRole('table', {
          name: 'Timeline records',
          exact: true,
          includeHidden: true,
        })
        await expect
          .poll(
            async () =>
              (await api.activity.inspectRun({ name: run, pageSize: 1 })).run?.actionPage?.actions
                .length,
          )
          .toBe(2)
        // The badge counts this Workspace's approvals on its own Approvals destination.
        await page.goto(`/workspaces/${id}`)
        const workspaceCount = (
          await api.activity.countRequiredActions({ workspace: workspace.name })
        ).approvalCount
        await approvalsLink(workspaceCount).waitFor()
        await page
          .getByRole('navigation', { name: 'Coral', exact: true })
          .getByRole('link', { name: 'Activity', exact: true })
          .click()
        // The Overview embeds the activity reader, so wait for Activity's own breadcrumb.
        await breadcrumb(page, 'Activity').waitFor()
        expect(new URL(page.url()).pathname).toBe(activityUrl)
        if (scope === 'personal') {
          await page.goto(`${activityUrl}?${new URLSearchParams({ root: task })}`)
          await timeline
            .getByRole('link', { name: 'Live gate lookup', exact: true })
            .first()
            .click()
          await page.getByRole('dialog', { name: 'Review action', exact: true }).waitFor()
          await page.waitForURL((url) => Boolean(url.searchParams.get('request')))
          await page.getByRole('button', { name: 'Close details', exact: true }).click()
          await page.waitForURL((url) => !url.searchParams.has('record'))
          await timeline.getByRole('link', { name: 'Call one Tool', exact: true }).click()
          await page.getByRole('dialog', { name: 'Activity details', exact: true }).waitFor()
          expect(new URL(page.url()).searchParams.get('record')).toBe(run)
          expect(await page.getByRole('button', { name: 'Approve', exact: true }).count()).toBe(0)
          // The Run's own action list opens the review of a call that waits on you.
          await page
            .getByRole('navigation', { name: 'Actions in this run', exact: true })
            .getByRole('link', { name: /^Review / })
            .first()
            .click()
          await page.getByRole('button', { name: 'Approve', exact: true }).waitFor()
          await page.waitForURL((url) =>
            Boolean(url.searchParams.get('record')?.includes('/invocations/')),
          )
        } else {
          // An Owner finds the Agent's request on the Approvals page, then opens it in activity.
          await page.goto(`/workspaces/${id}/approvals`)
          await page
            .getByRole('table', { name: 'Pending approvals', exact: true })
            .getByRole('link', { name: /^Review: / })
            .first()
            .click()
          await page.getByRole('link', { name: 'Open in activity', exact: true }).click()
        }
        await page.getByRole('dialog', { name: 'Review action', exact: true }).waitFor()
        // The review opens in its Task's timeline, with the call selected under its Run.
        await page.waitForURL(
          (url) =>
            url.searchParams.get('root') === task &&
            Boolean(url.searchParams.get('record')?.includes('/invocations/')),
        )
        await timeline
          .getByRole('link', { name: 'Call one Tool', exact: true, includeHidden: true })
          .waitFor()
        const selectedTool = new URL(page.url()).searchParams.get('record')!
        const selectedRow = timeline.getByRole('row', { selected: true, includeHidden: true })
        expect(await selectedRow.getAttribute('data-record-key')).toBe(selectedTool)
        await selectedRow
          .getByRole('link', { name: 'Live gate lookup', exact: true, includeHidden: true })
          .waitFor()
        await page.locator('summary').filter({ hasText: 'Add a comment' }).click()
        const comment = page.getByRole('textbox', { name: 'Comment (optional)' })
        const selectedRequest = new URL(page.url()).searchParams.get('request')
        await comment.fill('Reviewed once')
        // Observe a real subscription emission before deciding. It must not reset the form.
        const arrival = await runner.exec(
          'return await coral.providers.liveGate.lookup({marker:"arrival"});',
        )
        await approvalsLink(workspaceCount + 1n).waitFor()
        await api.activity.cancelActivityRun({
          name: `${workspace.name}/runs/${arrival.structuredContent.run_id}`,
        })
        await approvalsLink(workspaceCount).waitFor()
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
        expect(decided.invocation).toBe(selectedTool)
        expect(remaining.invocation).not.toBe(selectedTool)
        const before = calls
        await page.getByRole('button', { name: 'Approve', exact: true }).click()
        // The review stays on the call you decided; it does not move to the next approval.
        await page
          .getByRole('dialog', { name: 'Review action', exact: true })
          .getByText('Approved', { exact: true })
          .waitFor()
        expect(new URL(page.url()).searchParams.get('record')).toBe(selectedTool)
        expect(new URL(page.url()).searchParams.get('request')).toBe(selectedRequest)
        // The decision keeps the exact comment as its Comment fact.
        const approved = await api.activity.inspectApproval({ name: decided.name })
        expect(approved.state).toBe('approved')
        expect(approved.comment).toBe('Reviewed once')
        // The other call still waits on its own row; its review opens from there.
        await timeline
          .locator(`[data-record-key="${remaining.invocation}"]`)
          .getByRole('link', { includeHidden: true })
          .click()
        await page.waitForURL(
          (url) =>
            url.searchParams.get('record') === remaining.invocation &&
            url.searchParams.get('request') === remaining.name,
        )
        await page.getByRole('dialog', { name: 'Review action', exact: true }).waitFor()
        await page.setViewportSize({ width: 1440, height: 900 })
        await page.getByRole('button', { name: 'Decline', exact: true }).click()
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
        await approvalsLink(0n).waitFor()
        await expect
          .poll(() => page.getByRole('button', { name: 'Approve', exact: true }).count())
          .toBe(0)
        // Each settled review, opened on its own, shows its decision. Without a decision of
        // yours there, the review stays where you opened it. Only the approval carries the
        // exact comment, once, as its Comment fact.
        for (const [approval, label, comments] of [
          [decided, 'Approved', 1],
          [remaining, 'Declined', 0],
        ] as const) {
          const review = new URLSearchParams({
            root: task,
            record: approval.invocation,
            request: approval.name,
          })
          await page.goto(`${activityUrl}?${review}`)
          const dialog = page.getByRole('dialog')
          await dialog.getByText(label, { exact: true }).waitFor()
          expect(new URL(page.url()).searchParams.get('request')).toBe(approval.name)
          expect(new URL(page.url()).searchParams.get('record')).toBe(approval.invocation)
          expect(await selectedRow.getAttribute('data-record-key')).toBe(approval.invocation)
          expect(await page.getByRole('button', { name: 'Approve', exact: true }).count()).toBe(0)
          const commentFact = dialog
            .locator('dl > div')
            .filter({ has: page.locator('dt', { hasText: /^Comment$/ }) })
            .locator('dd', { hasText: /^Reviewed once$/ })
          await expect.poll(() => commentFact.count()).toBe(comments)
          expect(await dialog.getByText('Reviewed once', { exact: true }).count()).toBe(comments)
        }
        expect(
          await page.getByRole('table', { name: 'Activity records', includeHidden: true }).count(),
        ).toBe(1)
        expect(await page.getByRole('button', { name: 'Approve', exact: true }).count()).toBe(0)
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
      const list = page.getByRole('table', { name: 'Activity records', includeHidden: true })
      try {
        await step('Read the workspace before the first observation response', async () => {
          await page.goto(`/${privateWorkspace.name}/activity`)
          await list.getByRole('link', { name: 'Revoked private goal', exact: true }).waitFor()
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
          // The stalled observation still shows the old row; navigating must not restore it,
          // in the list or in the timeline the row opens.
          await list.getByRole('link', { name: 'Revoked private goal', exact: true }).click()
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
          await list.getByRole('link', { name: 'Available personal goal', exact: true }).waitFor()
          expect(
            await page
              .getByRole('link', { name: 'Revoked private goal', exact: true, includeHidden: true })
              .count(),
          ).toBe(0)
          expect(await list.locator('[data-record-key]').count()).toBe(1)
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
          // The Agent's timeline goes with its list.
          expect(
            await page
              .getByRole('table', { name: 'Timeline records', includeHidden: true })
              .count(),
          ).toBe(0)
          await page.getByText('This history is unavailable.', { exact: true }).waitFor()
        },
      )
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})

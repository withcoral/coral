/** @vitest-environment happy-dom */
import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { use, useEffect, useState } from 'react'
import type { LoaderFunction, Location } from 'react-router'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import {
  activityPage,
  approvalRequest,
  invocationRow,
  runRow,
  taskRow,
  toolRow,
  workspace,
} from './fixtures'
import { leavesActivityReview } from './navigation'
import type { ActivityPage, ActivityRow, ActivityViewProps } from './types'

// The keys Activity owns: ↑/↓, Space, Enter and Escape on the table itself (or
// its record links) and on the rail itself, and ↑/↓ from the page background.
// Traversal order and its boundaries are navigation.test.ts's.

const invoices = taskRow('invoices', 'Find invoices')
const reminders = taskRow('reminders', 'Send reminders')
const search = toolRow(invoices.name, 'search', { intent: 'Find tools', position: 0 })
const reminder = runRow('reminder', invoices.name, {
  actor: 'Reminder run',
  intent: 'Reminder run',
  state: 'running',
  endedAt: null,
  requiresAction: true,
  position: 1,
})
// With `runcall=1`, the Run holds one call: a branch under the root that closes.
const sent = invocationRow(reminder.name, 'sent', {
  task: invoices.name,
  callPath: 'mail.send',
  position: 0,
})
// The rail can show a record the list does not reach, as a direct link opens it: a Run,
// whose own row heads its waterfall.
const elsewhere = runRow('elsewhere', '', { actor: 'Elsewhere', intent: 'Elsewhere' })
const rows: ActivityRow[] = [invoices, search, reminder, reminders, elsewhere]
const list = activityPage().listPath
/** The list as live updates leave it: its roots, and the first Task's children. */
interface Lists {
  roots: ActivityRow[]
  children: ActivityRow[]
}

const rail = () => screen.queryByRole('dialog', { name: 'Activity details' })
const confirmation = () => screen.findByRole('dialog', { name: 'Discard your comment?' })

/** The page a loader returns for the URL: its selection and expanded records. */
function pageFor({ search: query }: Location, { roots, children }: Lists): ActivityPage {
  const params = new URLSearchParams(query)
  const record = params.get('record')
  const runCall = params.has('runcall')
  const expanded = params.has('tree')
    ? params.getAll('expand')
    : [invoices.name, ...(runCall ? [reminder.name] : [])]
  const selectedRow = [...rows, ...children, sent].find((row) => row.name === record) ?? null
  const detail: ActivityPage['detail'] =
    selectedRow?.kind === 'task'
      ? { kind: 'task', task: selectedRow }
      : selectedRow?.kind === 'tool'
        ? {
            kind: 'tool',
            task: selectedRow.task,
            interaction: selectedRow,
            evidence: { kind: 'unknown' },
          }
        : selectedRow?.kind === 'run'
          ? {
              kind: 'run',
              run: selectedRow,
              request: approvalRequest,
              source: 'return null;',
              linkedSharedTools: [],
              linkedPrivateTools: [],
              terminal: { kind: 'unavailable' },
            }
          : null
  return activityPage({
    rows: roots,
    expanded,
    branches: {
      ...(expanded.includes(invoices.name) && {
        [invoices.name]: { rows: children, total: '2', next: '', unavailable: false },
      }),
      ...(runCall &&
        expanded.includes(reminder.name) && {
          [reminder.name]: { rows: [sent], total: '1', next: '', unavailable: false },
        }),
    },
    selected: selectedRow?.name ?? null,
    selectedRow,
    detail,
  })
}

interface RenderOptions {
  more?: ActivityViewProps['more']
  /** Runs for each navigation, so a test can hold a load. */
  loader?: LoaderFunction
  /** A pending promise holds the location's render, as a slow transition render does. */
  holdRender?: (location: Location) => Promise<void> | undefined
}

async function renderActivity(entry = list, { more, loader, holdRender }: RenderOptions = {}) {
  const user = userEvent.setup()
  let publish!: (next: Partial<Lists>) => void
  function Live({ location }: { location: Location }) {
    const [lists, setLists] = useState<Lists>({
      roots: [invoices, reminders],
      children: [search, reminder],
    })
    useEffect(() => {
      publish = (next) => setLists((value) => ({ ...value, ...next }))
    }, [])
    const held = holdRender?.(location)
    if (held) use(held)
    const page = pageFor(location, lists)
    const request = page.detail?.kind === 'run' ? page.detail.request : null
    return (
      <ActionReviewSession request={request} leaves={leavesActivityReview(request)}>
        <button type="button">Outside</button>
        <Activity page={page} more={more} />
      </ActionReviewSession>
    )
  }
  const rendered = renderRoute((location) => <Live location={location} />, { entry, loader })
  const table = await screen.findByRole('table', { name: 'Activity records' })
  // A rail that opens takes focus; let it, as a reader would. A Task opens none: its
  // header above its waterfall states it.
  const chosen = new URL(entry, 'http://reef').searchParams.get('record')
  if (chosen && !/\/tasks\/[^/]+$/.test(chosen))
    await waitFor(() => expect(rail()?.contains(document.activeElement)).toBe(true))
  const link = (name: string) => within(table).getByRole('link', { name })
  return {
    user,
    table,
    link,
    router: rendered.router,
    /** The record the keyboard cursor rests on, by its label. */
    cursor: () => table.querySelector('[data-active] a')?.textContent ?? null,
    record: () => new URLSearchParams(rendered.router.state.location.search).get('record'),
    /** Lists `next` as a live update would. */
    update: (next: Partial<Lists>) => act(() => publish(next)),
  }
}

// Catches: the first ↓ only focusing the table, or keys acting from a focused control.
it('↑ and ↓ from the page background enter the table on the same press', async () => {
  const { user, table, cursor } = await renderActivity()
  // The Task's header states the Task: the first record is the first under it.
  await user.keyboard('{ArrowDown}')
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Find tools')
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Reminder run')

  // A focused control keeps its keys, and the cursor stays out of sight.
  const outside = screen.getByRole('button', { name: 'Outside' })
  act(() => outside.focus())
  expect(cursor()).toBeNull()
  await user.keyboard('{ArrowDown}{ArrowUp}')
  expect(outside).toHaveFocus()
  act(() => table.focus())
  expect(cursor()).toBe('Reminder run')
})

it('↑ with no cursor starts at the last record', async () => {
  const { user, table, cursor } = await renderActivity()
  await user.keyboard('{ArrowUp}')
  expect(table).toHaveFocus()
  // The waterfall holds the open Task's records only.
  expect(cursor()).toBe('Reminder run')
})

const opened = (name: string) => `${list}?record=${encodeURIComponent(name)}`
/** The same address, with the Run holding its call. */
const withRunCall = (entry: string) => `${entry}${entry.includes('?') ? '&' : '?'}runcall=1`
/** The record heading the open detail shows, after the rail's own title. */
const detailHeading = (dialog: HTMLElement | null) =>
  dialog && within(dialog).getAllByRole('heading', { level: 2 })[1]?.textContent

// Catches: a hidden cursor apart from the detail, a step that adds history, or
// a step that takes focus out of the table or the rail.
it('with the rail open, ↑ and ↓ step the shown record and keep focus where they started', async () => {
  const { user, table, link, record, router, cursor } = await renderActivity(opened(search.name))
  const dialog = rail()!
  // A click on another row replaces the detail; focus rests on the table.
  await user.click(link('Reminder run'))
  expect(record()).toBe(reminder.name)
  // Focus rests on the table, whose cursor row shows the clicked record.
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Reminder run')
  // The next ↑ continues from the clicked record.
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(search.name)
  expect(router.state.historyAction).toBe('REPLACE')
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Find tools')
  expect(rail()).toBe(dialog)

  // From the page background, the open rail takes focus and steps.
  act(() => (document.activeElement as HTMLElement).blur())
  await user.keyboard('{ArrowDown}')
  expect(record()).toBe(reminder.name)
  expect(dialog).toHaveFocus()
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(search.name)
  expect(dialog).toHaveFocus()
  // Above the first record is the Task's header: the rail stays where it is.
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(search.name)
  expect(rail()).toBe(dialog)
  // A control in the rail keeps its keys.
  act(() => within(dialog).getByRole('button', { name: 'Close details' }).focus())
  await user.keyboard('{ArrowDown}')
  expect(record()).toBe(search.name)
})

// Catches: Space or ← → acting on the wrong record, held Space toggling again, Space
// closing the root, or a collapse leaving the cursor or the detail on a record it hides.
it('Space toggles the cursor Run or collapses the Run around a child; the root never closes; ← and → do nothing', async () => {
  const { user, cursor, router, table } = await renderActivity(withRunCall(list))
  const before = router.state.location.search
  const tools = () => within(table).queryByRole('link', { name: 'Find tools' })
  const call = () => table.querySelector(`[data-record-key="${sent.name}"]`)
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Find tools')
  // A record with no marks has nothing for ← and → to select.
  await user.keyboard('{ArrowLeft}{ArrowRight}')
  expect(tools()).toBeInTheDocument()
  // A child of the root: the root heads its waterfall and never closes.
  await user.keyboard('[Space]')
  expect(tools()).toBeInTheDocument()
  expect(cursor()).toBe('Find tools')
  // A child of a Run collapses its Run, and the cursor moves to the Run.
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(call()).toBeInTheDocument()
  await user.keyboard('[Space]')
  await waitFor(() => expect(cursor()).toBe('Reminder run'))
  expect(call()).toBeNull()
  // Held Space toggles once.
  await user.keyboard('[Space>3/]')
  expect(call()).toBeInTheDocument()
  // From the first record, ↑ stays: the Task above it is its header, which never closes.
  await user.keyboard('{ArrowUp}{ArrowUp}{ArrowUp}')
  expect(cursor()).toBe('Find tools')
  await user.keyboard('[Space]')
  expect(tools()).toBeInTheDocument()
  // A collapse changes what the waterfall shows, never the address.
  expect(router.state.location.search).toBe(before)
})

// Catches: a collapse that hides the open record leaving the detail on it, or
// one that does not hide it changing the detail.
it('a collapse that hides the shown record opens the record it collapses', async () => {
  const { user, table, record, router } = await renderActivity(withRunCall(opened(sent.name)))
  const collapse = () => within(table).getByRole('button', { name: 'Collapse record' })
  act(() => table.focus())
  // Space on the shown call, from the table: one replaced address, on its Run.
  await user.keyboard('[Space]')
  expect(record()).toBe(reminder.name)
  expect(router.state.historyAction).toBe('REPLACE')
  expect(table.querySelector(`[data-record-key="${sent.name}"]`)).toBeNull()
  // The rail stays open, on the Run.
  expect(rail()).not.toBeNull()

  // The expand control shows the call again and keeps the detail.
  await user.click(within(table).getByRole('button', { name: 'Expand record' }))
  expect(record()).toBe(reminder.name)
  // A collapse that hides nothing of the shown record keeps the detail.
  await user.click(within(table).getByRole('link', { name: 'Find tools' }))
  expect(record()).toBe(search.name)
  await user.click(collapse())
  expect(record()).toBe(search.name)
  expect(rail()).not.toBeNull()
  // A pointer collapse that hides it pushes, as other clicks do.
  await user.click(within(table).getByRole('button', { name: 'Expand record' }))
  await user.click(
    table.querySelector<HTMLElement>(`[data-record-key="${sent.name}"] [data-record-link]`)!,
  )
  expect(record()).toBe(sent.name)
  await user.click(collapse())
  expect(record()).toBe(reminder.name)
  expect(router.state.historyAction).toBe('PUSH')
})

// Catches: rail Space collapsing a parent or the root, or closing the detail.
it('Space in the rail toggles only the Run it shows, never the root', async () => {
  const { user, record, router, table } = await renderActivity(withRunCall(opened(search.name)))
  const dialog = rail()!
  act(() => dialog.focus())
  const before = router.state.location.search
  await user.keyboard('[Space]')
  expect(router.state.location.search).toBe(before)
  // Above the first record is the Task's header, which never closes: the rail stays.
  await user.keyboard('{ArrowUp}')
  expect(record()).toBe(search.name)
  await user.keyboard('[Space]')
  expect(screen.getByRole('link', { name: 'Find tools' })).toBeInTheDocument()
  // The Run the rail shows collapses in the waterfall, and stays in the rail.
  await user.keyboard('{ArrowDown}')
  expect(record()).toBe(reminder.name)
  await user.keyboard('[Space]')
  expect(table.querySelector(`[data-record-key="${sent.name}"]`)).toBeNull()
  expect(record()).toBe(reminder.name)
  expect(dialog).toHaveFocus()
  expect(rail()).toBe(dialog)
})

// Catches: Escape outside the table's ownership closing the rail, or the cursor
// returning to a record other than the last one shown.
it('Escape from the table closes the rail onto the last record it showed; Enter opens it again', async () => {
  const { user, table, record, cursor, router } = await renderActivity(opened(search.name))
  act(() => screen.getByRole('button', { name: 'Outside' }).focus())
  await user.keyboard('{Escape}')
  expect(record()).toBe(search.name)
  act(() => table.focus())
  await user.keyboard('{ArrowDown}')
  await user.keyboard('{Escape}')
  expect(record()).toBeNull()
  await waitFor(() => expect(rail()).toBeNull())
  await waitFor(() => expect(table.contains(document.activeElement)).toBe(true))
  expect(cursor()).toBe('Reminder run')
  act(() => table.focus())
  await user.keyboard('{Enter}')
  expect(record()).toBe(reminder.name)
  expect(router.state.historyAction).toBe('PUSH')
})

// Catches: a step the router has finished but React has not rendered reopening the
// rail that Escape closed, which also takes focus from the table.
it('Escape before a step renders closes the rail for good', async () => {
  let renderStep!: () => void
  const stepRender = new Promise<void>((resolve) => (renderStep = resolve))
  let landClose!: () => void
  const closeLoad = new Promise<void>((resolve) => (landClose = resolve))
  let closing = false
  const { user, table, record, cursor } = await renderActivity(opened(search.name), {
    loader: async ({ request }) => {
      if (closing && !new URL(request.url).searchParams.has('record')) await closeLoad
      return null
    },
    holdRender: (location) =>
      new URLSearchParams(location.search).get('record') === reminder.name ? stepRender : undefined,
  })
  act(() => table.focus())
  await user.keyboard('{ArrowDown}')
  // The address moves on; the rail still shows the record it showed.
  await waitFor(() => expect(record()).toBe(reminder.name))
  expect(detailHeading(rail())).toBe('Find tools')
  closing = true
  await user.keyboard('{Escape}')
  await waitFor(() => expect(rail()).toBeNull())
  // The step renders while the close is still loading.
  await act(async () => renderStep())
  expect(rail()).toBeNull()
  expect(table).toHaveFocus()
  await act(async () => landClose())
  await waitFor(() => expect(record()).toBeNull())
  expect(rail()).toBeNull()
  expect(table).toHaveFocus()
  expect(cursor()).toBe('Find tools')
})

// Catches: a pending navigation to a record outside the table, such as live mode's,
// missing from the close, so its late render reopens the rail.
it('Escape before a navigation outside the table renders closes the rail for good', async () => {
  let landLoad!: () => void
  const load = new Promise<void>((resolve) => (landLoad = resolve))
  let renderIt!: () => void
  const render = new Promise<void>((resolve) => (renderIt = resolve))
  let closing = false
  const { user, table, record, router } = await renderActivity(opened(search.name), {
    loader: async ({ request }) => {
      const requested = new URL(request.url).searchParams.get('record')
      if (requested === elsewhere.name) await load
      if (closing && requested === null) await new Promise(() => {})
      return null
    },
    holdRender: (location) =>
      new URLSearchParams(location.search).get('record') === elsewhere.name ? render : undefined,
  })
  act(() => table.focus())
  act(() => void router.navigate(opened(elsewhere.name)))
  await waitFor(() => expect(router.state.navigation.state).toBe('loading'))
  await act(async () => landLoad())
  await waitFor(() => expect(record()).toBe(elsewhere.name))
  closing = true
  await user.keyboard('{Escape}')
  await waitFor(() => expect(rail()).toBeNull())
  await act(async () => renderIt())
  expect(rail()).toBeNull()
  expect(table).toHaveFocus()
})

// Catches: picking, while the rail closes, the record a pending step was going to
// show reopening the record you closed until the pick loads.
it('a pick of the pending step’s record during a close leaves the rail closed until it loads', async () => {
  let landStep!: () => void
  const stepLoad = new Promise<void>((resolve) => (landStep = resolve))
  let closing = false
  const { user, table, link, record } = await renderActivity(opened(search.name), {
    loader: async ({ request }) => {
      const requested = new URL(request.url).searchParams.get('record')
      if (requested === reminder.name) await stepLoad
      // The close never lands, so only the pick can end it.
      if (closing && requested === null) await new Promise(() => {})
      return null
    },
  })
  act(() => table.focus())
  await user.keyboard('{ArrowDown}')
  closing = true
  await user.keyboard('{Escape}')
  await waitFor(() => expect(rail()).toBeNull())
  await user.click(link('Reminder run'))
  await act(async () => {})
  expect(rail()).toBeNull()
  await act(async () => landStep())
  await waitFor(() => expect(record()).toBe(reminder.name))
  // The run's pending approval opens the rail as its review.
  const dialog = await screen.findByRole('dialog')
  expect(within(dialog).getAllByText('Reminder run').length).toBeGreaterThan(0)
  expect(within(dialog).queryByText('Find tools')).toBeNull()
})

// Catches: a cursor kept by index, so arrivals retarget it, or one that outlives its record.
it('the cursor keeps its record across live arrivals, follows acceptance, and clears on removal', async () => {
  const { user, table, cursor, update } = await renderActivity()
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Find tools')
  update({ roots: [taskRow('newer', 'Newer task'), invoices, reminders] })
  expect(cursor()).toBe('Find tools')
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Reminder run')

  // A submission becomes the Run it was accepted as, in the same place.
  const submitted = { ...search, name: `${invoices.name}/interactions/exec`, intent: 'Run it' }
  update({ children: [submitted, reminder] })
  await user.keyboard('{ArrowUp}')
  expect(cursor()).toBe('Run it')
  const accepted = {
    ...reminder,
    name: `${workspace}/runs/accepted`,
    actor: 'Accepted run',
    position: 0,
  }
  update({ children: [{ ...accepted, submission: submitted }, reminder] })
  // Its label is still the submission's intent; the cursor rests on the Run itself.
  expect(table.querySelector('[data-active]')).toHaveAttribute('data-record-key', accepted.name)

  // A record that leaves the table takes the cursor with it.
  update({ children: [reminder] })
  expect(cursor()).toBeNull()
  // ↓ starts again at the first record under the Task's header.
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Reminder run')
})

// Catches: a key switching away from an unsent comment without asking, or the
// confirmation's own keys driving the table behind it.
it('a step or Escape with an unsent comment asks first', async () => {
  const { user, table, record } = await renderActivity(opened(reminder.name))
  const dialog = rail()!
  await user.type(within(dialog).getByRole('textbox', { name: 'Comment (optional)' }), 'Check it')
  const choose = async (name: 'Keep editing' | 'Discard comment') => {
    await user.click(within(await confirmation()).getByRole('button', { name }))
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Discard your comment?' })).toBeNull(),
    )
  }
  act(() => table.focus())
  await user.keyboard('{ArrowUp}')
  await confirmation()
  // The confirmation keeps its keys.
  await user.keyboard('{ArrowDown}{ArrowUp}')
  await choose('Keep editing')
  expect(record()).toBe(reminder.name)
  expect(within(dialog).getByRole('textbox', { name: 'Comment (optional)' })).toHaveValue(
    'Check it',
  )

  act(() => table.focus())
  await user.keyboard('{Escape}')
  await choose('Keep editing')
  expect(record()).toBe(reminder.name)
  expect(rail()).toBe(dialog)

  act(() => table.focus())
  await user.keyboard('{ArrowUp}')
  await choose('Discard comment')
  expect(record()).toBe(search.name)
})

it('Discard after Escape closes the rail onto the record it showed', async () => {
  const { user, table, record, cursor } = await renderActivity(opened(reminder.name))
  await user.type(within(rail()!).getByRole('textbox', { name: 'Comment (optional)' }), 'Check it')
  act(() => table.focus())
  await user.keyboard('{Escape}')
  await user.click(within(await confirmation()).getByRole('button', { name: 'Discard comment' }))
  await waitFor(() => expect(record()).toBeNull())
  await waitFor(() => expect(rail()).toBeNull())
  act(() => table.focus())
  expect(cursor()).toBe('Reminder run')
})

// Catches regressions of withcoral/lagoon#712: Enter or Space on a focused control
// acting on the cursor record instead of the control.
it("Enter and Space on a focused control stay the control's own", async () => {
  const { user, cursor, record, router } = await renderActivity(withRunCall(list))
  // The cursor rests on a record, so a key that leaks to the table would open it.
  await user.keyboard('{ArrowUp}')
  expect(cursor()).toBe('mail.send')
  act(() => screen.getByRole('button', { name: 'Outside' }).focus())
  await user.keyboard('{Enter}[Space]')
  expect(router.state.location.search).toBe('?runcall=1')
  // The expand control collapses its own Run once, and nothing else changes.
  act(() => screen.getByRole('button', { name: 'Collapse record' }).focus())
  await user.keyboard('[Space]')
  expect(document.querySelector(`[data-record-key="${sent.name}"]`)).toBeNull()
  expect(screen.getByRole('link', { name: 'Find tools' })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Expand record' })).toBeInTheDocument()
  expect(record()).toBeNull()
})

// Catches: a glimpse taking the page's keys, or losing its records' Tab stops.
it('a glimpse takes no keys and keeps a Tab stop on each record', async () => {
  const { user, table, cursor } = await renderActivity(list, {
    more: { to: list, label: 'View all activity', roots: 2 },
  })
  await user.keyboard('{ArrowDown}')
  expect(table).not.toHaveFocus()
  expect(cursor()).toBeNull()
  expect(table).not.toHaveAttribute('tabindex')
  expect(within(table).getByRole('link', { name: 'Find invoices' })).not.toHaveAttribute('tabindex')
})

// Catches: a pointer collapse leaving the closed cursor on a record it hides.
it('a collapse with the rail closed moves the cursor to the collapsed record', async () => {
  const { user, cursor, table } = await renderActivity(withRunCall(list))
  await user.keyboard('{ArrowDown}{ArrowDown}{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('mail.send')
  await user.click(screen.getByRole('button', { name: 'Collapse record' }))
  act(() => table.focus())
  await waitFor(() => expect(cursor()).toBe('Reminder run'))
})

// Catches: a click on the synthetic outside-page row leaving focus on its link,
// where the table's keys do nothing.
it('a click on a record outside the list rests focus on the table', async () => {
  const { user, table, record } = await renderActivity(opened(elsewhere.name))
  await user.click(within(table).getByRole('link', { name: 'Elsewhere' }))
  expect(table).toHaveFocus()
  await user.keyboard('{Escape}')
  expect(record()).toBeNull()
})
